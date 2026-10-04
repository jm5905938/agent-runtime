package model

import (
	"agent-runtime/core"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var _ core.ContextActionHandler = (*Handler)(nil)

func TestHandlerCallerCancellationCoversHeadersAndBody(t *testing.T) {
	for _, flushHeader := range []bool{false, true} {
		t.Run(map[bool]string{false: "headers", true: "body"}[flushHeader], func(t *testing.T) {
			started := make(chan struct{})
			serverCanceled := make(chan struct{})
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				io.Copy(io.Discard, request.Body)
				if flushHeader {
					writer.WriteHeader(http.StatusOK)
					io.WriteString(writer, `{"choices":[{"message":{"content":"partial`)
					writer.(http.Flusher).Flush()
				}
				close(started)
				select {
				case <-request.Context().Done():
					close(serverCanceled)
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			handler := testHandler(t, server.URL)
			go func() {
				result, err := handler.ExecuteContext(ctx, modelAction("private-input"))
				if result != nil {
					t.Error("canceled incomplete reply produced a result")
				}
				finished <- err
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("model request never arrived")
			}
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation sentinel was lost: %v", err)
				}
				for _, sensitive := range []string{testKey, server.URL, "private-input", "partial"} {
					if strings.Contains(err.Error(), sensitive) {
						t.Fatalf("cancellation error exposes request data: %v", err)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancellation did not stop the HTTP request promptly")
			}
			select {
			case <-serverCanceled:
			case <-time.After(2 * time.Second):
				t.Fatal("provider did not observe request cancellation")
			}
		})
	}
}

func TestHandlerPreCanceledContextSendsNoRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := testHandler(t, server.URL).ExecuteContext(ctx, modelAction("private-input"))
	if !errors.Is(err, context.Canceled) || result != nil || calls.Load() != 0 {
		t.Fatalf("pre-canceled model call ran: result=%v, err=%v, calls=%d", result, err, calls.Load())
	}
}

func TestHandlerCallerDeadlinePreservesSentinel(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		io.Copy(io.Discard, request.Body)
		select {
		case <-request.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result, err := testHandler(t, server.URL).ExecuteContext(ctx, modelAction("private-input"))
	if !errors.Is(err, context.DeadlineExceeded) || result != nil {
		t.Fatalf("deadline sentinel was lost: result=%v, err=%v", result, err)
	}
}
