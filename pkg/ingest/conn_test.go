package ingest

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/oklog/pkg/fs"
)

func TestHandleConnectionsCleanup(t *testing.T) {
	// Bind a listener on some port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Set up a file log using our mock FS.
	// The mock FS counts file closures.
	fs := &mockFilesystem{}
	log, err := NewFileLog(fs, "/")
	if err != nil {
		t.Fatal(err)
	}

	// Listen for connections.
	// When shut down, send the error on the chan.
	errc := make(chan error, 1)
	var (
		connectionHandler = echo(t)
		segmentFlushAge   = time.Second
		segmentFlushSize  = 1024 // B
		connectedClients  = prometheus.NewGauge(prometheus.GaugeOpts{})
		bytes             = prometheus.NewCounter(prometheus.CounterOpts{})
		records           = prometheus.NewCounter(prometheus.CounterOpts{})
		syncs             = prometheus.NewCounter(prometheus.CounterOpts{})
		segmentAge        = prometheus.NewHistogram(prometheus.HistogramOpts{})
		segmentSize       = prometheus.NewHistogram(prometheus.HistogramOpts{})
	)
	go func() {
		errc <- HandleConnections(
			ln, connectionHandler, log, segmentFlushAge, segmentFlushSize,
			connectedClients, bytes, records, syncs, segmentAge, segmentSize,
		)
	}()

	// Connect to the handler.
	var conn net.Conn
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	if !within(time.Second, func() bool {
		conn, err = net.Dial("tcp", "127.0.0.1:"+port)
		return err == nil
	}) {
		t.Fatal("listener never came up")
	}

	// Write something to make sure the connection is good.
	message := "hello, world!\n"
	if n, err := fmt.Fprint(conn, message); err != nil {
		t.Fatal(err)
	} else if want, have := len(message), n; want != have {
		t.Fatalf("n: want %d, have %d", want, have)
	}
	if !within(time.Second, func() bool {
		return atomic.LoadUint64(&fs.wr) > 0
	}) {
		t.Fatal("timeout waiting for write")
	}

	// Make sure closing the listener triggers the segment to flush.
	pre := atomic.LoadUint64(&fs.cl)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		t.Logf("HandleConnections successfully torn down (%v)", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for shutdown")
	}
	var post uint64
	if !within(time.Second, func() bool {
		post = atomic.LoadUint64(&fs.cl)
		return (post - pre) > 0
	}) {
		t.Errorf("timeout waiting for Close: initial Closes=%d, current Closes=%d", pre, post)
	}

	// Make sure we can't write to the conn at some point.
	if !within(time.Second, func() bool {
		_, err := fmt.Fprintln(conn, "this should fail, eventually")
		return err != nil
	}) {
		t.Errorf("our connection never noticed the shutdown")
	}
}

func echo(t *testing.T) ConnectionHandler {
	return func(conn net.Conn, w *Writer, _ IDGenerator, _ prometheus.Gauge) error {
		s := bufio.NewScanner(conn)
		for s.Scan() {
			t.Logf("RECV> %s", s.Text())
			fmt.Fprintln(w, s.Text())
		}
		return s.Err()
	}
}

func within(d time.Duration, f func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return true
		}
		time.Sleep(d / 25)
	}
	return false
}

type mockFilesystem struct{ wr, cl uint64 }

func (fs *mockFilesystem) Create(path string) (fs.File, error)               { return &mockFile{&fs.wr, &fs.cl}, nil }
func (fs *mockFilesystem) Open(path string) (fs.File, error)                 { return &mockFile{&fs.wr, &fs.cl}, nil }
func (fs *mockFilesystem) Remove(path string) error                          { return nil }
func (fs *mockFilesystem) Rename(oldname, newname string) error              { return nil }
func (fs *mockFilesystem) Exists(path string) bool                           { return false }
func (fs *mockFilesystem) MkdirAll(path string) error                        { return nil }
func (fs *mockFilesystem) Chtimes(path string, atime, mtime time.Time) error { return nil }
func (fs *mockFilesystem) Walk(root string, walkFn filepath.WalkFunc) error  { return nil }
func (fs *mockFilesystem) Lock(string) (fs.Releaser, bool, error)            { return mockReleaser{}, false, nil }

type mockFile struct{ wr, cl *uint64 }

func (f *mockFile) Read(p []byte) (int, error)  { return len(p), nil }
func (f *mockFile) Write(p []byte) (int, error) { atomic.AddUint64(f.wr, 1); return len(p), nil }
func (f *mockFile) Close() error                { atomic.AddUint64(f.cl, 1); return nil }
func (f *mockFile) Name() string                { return "" }
func (f *mockFile) Size() int64                 { return 0 }
func (f *mockFile) Sync() error                 { return nil }

type mockReleaser struct{}

func (mockReleaser) Release() error { return nil }
