//Package python将go执行过程连接到常驻的python agent worker
package python

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
)

const (
	ProtocolVersion = 1
	//MaxFrameBytes包含末尾换行符
	MaxFrameBytes  = 1 << 20
	DefaultTimeout = 30 * time.Second
)

var ErrClosed = errors.New("python runner已关闭")

type Options struct {
	Python string
	//SourceDir会加到此worker的PYTHONPATH前面，为空时使用已安装的包
	SourceDir string
	//Timeout包含等待其他调用的时间，零值使用DefaultTimeout
	Timeout time.Duration
	//Stderr默认为os.Stderr，自定义写入器必须及时返回
	Stderr io.Writer
}

//Runner串行调用同一个worker，响应无效、输入输出故障或调用中断时丢弃该worker，后续Run会启动新worker，
//中断的execution不会自动重试
type Runner struct {
	options Options
	gate    chan struct{}
	done    chan struct{}
	mu      sync.Mutex
	closed  bool
	worker  *worker
}

type worker struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	stdout   io.ReadCloser
	reader   *bufio.Reader
	stopOnce sync.Once
}

//Error返回runtime可识别的失败原因
type Error struct {
	kind    domain.ErrorKind
	message string
	cause   error
}

func (e *Error) Error() string                 { return e.message }
func (e *Error) Unwrap() error                 { return e.cause }
func (e *Error) FailureKind() domain.ErrorKind { return e.kind }

func failure(kind domain.ErrorKind, message string, cause error) error {
	return &Error{kind: kind, message: message, cause: cause}
}

func NewRunner(options Options) (*Runner, error) {
	if options.Python == "" {
		options.Python = "python3"
	}
	if options.Timeout < 0 {
		return nil, fmt.Errorf("python runner超时时间必须大于零")
	}
	if options.Timeout == 0 {
		options.Timeout = DefaultTimeout
	}
	if options.Stderr == nil {
		options.Stderr = os.Stderr
	}
	if options.SourceDir != "" {
		path, err := filepath.Abs(options.SourceDir)
		if err != nil {
			return nil, fmt.Errorf("解析python源码目录失败: %w", err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("检查python源码目录失败: %w", err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("python源码路径必须是目录")
		}
		options.SourceDir = path
	}
	return &Runner{options: options, gate: make(chan struct{}, 1), done: make(chan struct{})}, nil
}

func (r *Runner) Run(input core.ExecutionContext) (core.ExecutionResult, error) {
	return r.RunContext(context.Background(), input)
}

func (r *Runner) RunContext(ctx context.Context, input core.ExecutionContext) (core.ExecutionResult, error) {
	ctx, cancel := context.WithTimeout(ctx, r.options.Timeout)
	defer cancel()
	select {
	case <-r.done:
		return core.ExecutionResult{}, failure(domain.ErrorKindRuntime, ErrClosed.Error(), ErrClosed)
	case <-ctx.Done():
		return core.ExecutionResult{}, interrupted(ctx.Err())
	case r.gate <- struct{}{}:
	}
	defer func() { <-r.gate }()
	select {
	case <-r.done:
		return core.ExecutionResult{}, failure(domain.ErrorKindRuntime, ErrClosed.Error(), ErrClosed)
	default:
	}
	if err := ctx.Err(); err != nil {
		return core.ExecutionResult{}, interrupted(err)
	}
	if input.AttemptID == "" {
		return core.ExecutionResult{}, failure(domain.ErrorKindRuntime, "python请求的attempt_id不能为空", nil)
	}
	request := struct {
		Version int                   `json:"version"`
		ID      domain.ID             `json:"id"`
		Context core.ExecutionContext `json:"context"`
	}{ProtocolVersion, input.AttemptID, input}
	data, err := codec.Encode(request)
	if err != nil {
		return core.ExecutionResult{}, failure(domain.ErrorKindRuntime, "编码python请求失败: "+shortError(err), err)
	}
	if len(data)+1 > MaxFrameBytes {
		return core.ExecutionResult{}, failure(domain.ErrorKindRuntime, "python请求超过帧长度限制", nil)
	}
	data = append(data, '\n')
	process, err := r.getWorker()
	if err != nil {
		return core.ExecutionResult{}, err
	}
	//编码和启动进程可能已经耗尽剩余时间
	if err := ctx.Err(); err != nil {
		return core.ExecutionResult{}, interrupted(err)
	}
	type response struct {
		data []byte
		err  error
	}
	replies := make(chan response, 1)
	go func() {
		_, err := process.stdin.Write(data)
		if err != nil {
			replies <- response{err: fmt.Errorf("写入python请求失败: %w", err)}
			return
		}
		line, err := process.reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			err = errors.New("python响应超过帧长度限制")
		} else if err != nil {
			err = fmt.Errorf("读取python响应失败: %w", err)
		}
		replies <- response{data: line, err: err}
	}()
	var reply response
	select {
	case reply = <-replies:
	case <-ctx.Done():
		r.discard(process)
		<-replies
		return core.ExecutionResult{}, interrupted(ctx.Err())
	case <-r.done:
		r.discard(process)
		<-replies
		return core.ExecutionResult{}, failure(domain.ErrorKindInterrupted, "关闭runner时python调用中断", ErrClosed)
	}
	if err := ctx.Err(); err != nil {
		r.discard(process)
		return core.ExecutionResult{}, interrupted(err)
	}
	select {
	case <-r.done:
		r.discard(process)
		return core.ExecutionResult{}, failure(domain.ErrorKindInterrupted, "关闭runner时python调用中断", ErrClosed)
	default:
	}
	if reply.err != nil {
		r.discard(process)
		return core.ExecutionResult{}, failure(domain.ErrorKindRuntime, shortError(reply.err), reply.err)
	}
	result, err, valid := decodeResponse(reply.data, input)
	if cause := ctx.Err(); cause != nil {
		r.discard(process)
		return core.ExecutionResult{}, interrupted(cause)
	}
	select {
	case <-r.done:
		r.discard(process)
		return core.ExecutionResult{}, failure(domain.ErrorKindInterrupted, "关闭runner时python调用中断", ErrClosed)
	default:
	}
	if !valid {
		r.discard(process)
	}
	return result, err
}

func interrupted(cause error) error {
	return failure(domain.ErrorKindInterrupted, "python调用中断: "+cause.Error(), cause)
}

func (r *Runner) getWorker() (*worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, failure(domain.ErrorKindRuntime, ErrClosed.Error(), ErrClosed)
	}
	if r.worker != nil {
		return r.worker, nil
	}
	cmd := exec.Command(r.options.Python, "-u", "-m", "agent_runtime.worker")
	cmd.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")
	if r.options.SourceDir != "" {
		path := r.options.SourceDir
		if previous := os.Getenv("PYTHONPATH"); previous != "" {
			path += string(os.PathListSeparator) + previous
		}
		cmd.Env = append(cmd.Env, "PYTHONPATH="+path)
	}
	cmd.Stderr = r.options.Stderr
	cmd.WaitDelay = time.Second
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, failure(domain.ErrorKindRuntime, "创建python输入管道失败: "+shortError(err), err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return nil, failure(domain.ErrorKindRuntime, "创建python输出管道失败: "+shortError(err), err)
	}
	if err := cmd.Start(); err != nil {
		stdin.Close()
		stdout.Close()
		return nil, failure(domain.ErrorKindRuntime, "启动python worker失败: "+shortError(err), err)
	}
	r.worker = &worker{cmd: cmd, stdin: stdin, stdout: stdout, reader: bufio.NewReaderSize(stdout, MaxFrameBytes)}
	return r.worker, nil
}

func (w *worker) stop() {
	w.stopOnce.Do(func() {
		//关闭两个管道也能解除不读取stdin的worker阻塞
		w.cmd.Process.Kill()
		w.stdin.Close()
		w.stdout.Close()
		w.cmd.Wait()
	})
}

func (r *Runner) discard(w *worker) {
	w.stop()
	r.mu.Lock()
	if r.worker == w {
		r.worker = nil
	}
	r.mu.Unlock()
}

//Close中断当前调用并回收worker进程，随后永久关闭此runner，重复调用是安全的
func (r *Runner) Close() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.done)
	}
	w := r.worker
	r.mu.Unlock()
	if w != nil {
		r.discard(w)
	}
	return nil
}

func shortError(err error) string {
	message := err.Error()
	if len(message) > 512 {
		return strings.ToValidUTF8(message[:512], "") + "…"
	}
	return message
}
