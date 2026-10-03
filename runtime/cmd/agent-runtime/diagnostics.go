package main

import "sync"

type workerDiagnostics struct {
	mu        sync.Mutex
	data      []byte
	truncated bool
}

func (d *workerDiagnostics) Write(data []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := min(len(data), 64*1024-len(d.data))
	d.data = append(d.data, data[:count]...)
	d.truncated = d.truncated || count != len(data)
	return len(data), nil
}

func (d *workerDiagnostics) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.truncated {
		return string(d.data) + "\n[worker诊断已截断]"
	}
	return string(d.data)
}
