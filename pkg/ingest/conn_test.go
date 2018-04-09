package ingest

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/oklog/pkg/record"
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
			ln, connectionHandler, record.NewDynamicReader, log, segmentFlushAge, segmentFlushSize,
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
	message := "test_topic hello, world!\n"
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
	return func(read record.Reader, w *Writer, _ IDGenerator, _ prometheus.Gauge) error {
		for {
			r, err := read()
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			t.Logf("RECV> %s", r)
			fmt.Fprintln(w, r)
		}
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

func TestStreamClock(t *testing.T) {
	sc := newStreamClock()

	if _, err := sc.Read(make([]byte, 5)); err == nil {
		t.Fatalf("expected error on read with length other than 10")
	}

	b := make([]byte, 10)
	lastRand := make([]byte, 6)

	// Ensure clock is incremented properly.
	for i := uint32(0); i <= 100; i++ {
		copy(lastRand, b[:6])

		if n, err := sc.Read(b); err != nil {
			t.Fatalf("read failed: %s", err)
		} else if n != 10 {
			t.Fatalf("unexpected read length %d, want 10", n)
		}
		x := binary.BigEndian.Uint32(b[6:])
		if x != i {
			t.Fatalf("unexpected clock value %d, want %d", x, i)
		}
		// The first random 6 bytes should not change.
		if i > 0 && !bytes.Equal(b[:6], lastRand) {
			t.Fatalf("unexpected change in random component: got %x, want %x", b[:6], lastRand)
		}
	}

	// Manually update the clock component to right before overflowing.
	// The random component should be incremented on the next read and 4 high bytes
	// should be reset to 0.
	sc.clock |= math.MaxUint32
	prev := sc.clock

	if n, err := sc.Read(b); err != nil {
		t.Fatalf("read failed: %s", err)
	} else if n != 10 {
		t.Fatalf("unexpected read length %d, want 10", n)
	}
	if sc.clock != prev+1 {
		t.Fatalf("expected clock counter %d after increment, got %d", prev+1, sc.clock)
	} else if uint32(sc.clock) != 0 {
		t.Fatalf("unexpected non-zero value %d of 4 highest bytes", uint32(sc.clock))
	}
}

func BenchmarkStreamClockULID(b *testing.B) {
	for n, r := range map[string]io.Reader{
		"empty":       nil,
		"math-rand":   mathrand.New(mathrand.NewSource(0)),
		"crypto-rand": cryptorand.Reader,
		"streamclock": newStreamClock(),
	} {
		b.Run(fmt.Sprintf("entropy/%s", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ulid.MustNew(uint64(i), r)
			}
		})
	}
}
