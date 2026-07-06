package main

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type mockConn struct {
	net.Conn
	writeChan chan struct{}
	closeChan chan struct{}
	mu        sync.Mutex
	deadline  time.Time
	timer     *time.Timer
}

func newMockConn() *mockConn {
	return &mockConn{
		writeChan: make(chan struct{}),
		closeChan: make(chan struct{}),
	}
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	select {
	case <-m.writeChan:
		return len(b), nil
	case <-m.closeChan:
		return 0, errors.New("closed")
	}
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadline = t
	if m.timer != nil {
		m.timer.Stop()
	}
	if !t.IsZero() {
		d := time.Until(t)
		m.timer = time.AfterFunc(d, func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			select {
			case <-m.closeChan:
			default:
				close(m.closeChan)
			}
		})
	}
	return nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.timer != nil {
			m.timer.Stop()
	}
	select {
	case <-m.closeChan:
	default:
		close(m.closeChan)
	}
	return nil
}

func TestWriteControlDeadline(t *testing.T) {
	mc := newMockConn()
	defer mc.Close()

	conn := newConn(mc, true, 1024, 1024)

	errChan := make(chan error, 1)
	go func() {
		errChan <- conn.WriteMessage(BinaryMessage, []byte("hello"))
	}()

	// Give the goroutine a moment to block on the write.
	time.Sleep(50 * time.Millisecond)

	// Call WriteControl with a short deadline.
	deadline := time.Now().Add(50 * time.Millisecond)
	err := conn.WriteControl(PingMessage, []byte("ping"), deadline)
	if err == nil {
		t.Error("expected error, got nil")
	}

	// The blocked WriteMessage should also return an error because the deadline was set.
	select {
	case err := <-errChan:
		if err == nil {
			t.Error("expected write to fail, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Error("WriteMessage did not unblock")
	}
}
