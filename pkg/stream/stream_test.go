package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadOnce(t *testing.T) {
	t.Parallel()

	n := 3
	rcf := func(ctx context.Context, addr string) (io.ReadCloser, error) {
		return &ctxReader{ctx, []byte(addr), int32(10 * n)}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	sink := make(chan []byte)
	addr := "my.address.co"

	// Take n records from the sink, then cancel the context.
	go func() {
		defer cancel()
		var want, have []byte = []byte(addr), nil
		for i := 0; i < n; i++ {
			select {
			case have = <-sink:
			case <-time.After(100 * time.Millisecond):
				t.Fatal("timeout waiting for record")
			}
			if have = bytes.TrimSpace(have); !bytes.Equal(want, have) {
				t.Errorf("want %q, have %q", want, have)
			}
		}
	}()

	// Make sure the context cancelation terminates the function.
	if want, have := context.Canceled, readOnce(ctx, rcf, addr, sink); want != have {
		t.Errorf("want %v, have %v", want, have)
	}
}

func TestReadUntilCanceled(t *testing.T) {
	t.Parallel()

	rcf := func(ctx context.Context, addr string) (io.ReadCloser, error) {
		return &ctxReader{ctx, []byte(addr), 1}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	sink := make(chan []byte)
	addr := "some.addr.local"

	// Each ctxReader will die after 1 record.
	// So we want to take at least 3, to test it's reconnecting.
	// Once those 3 have been drained, cancel the context.
	go func() {
		defer cancel()
		var want, have []byte = []byte(addr), nil
		for i := 0; i < 3; i++ {
			select {
			case have = <-sink:
			case <-time.After(100 * time.Second):
				t.Fatal("timeout waiting for record")
			}
			if have = bytes.TrimSpace(have); !bytes.Equal(want, have) {
				t.Errorf("want %q, have %q", want, have)
			}
		}
	}()

	// Read until the context has been canceled.
	done := make(chan struct{})
	go func() {
		noSleep := func(time.Duration) { /* no delay pls */ }
		readUntilCanceled(ctx, rcf, "some.addr.local", sink, noSleep)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Errorf("timeout waiting for read loop to finish")
	}
}

func TestIssue59(t *testing.T) {
	// Start Execute, streaming from some infinite sources.
	var (
		ctx, cancel = context.WithCancel(context.Background())
		pf          = func() []string { return []string{"alpha", "bravo", "charlie"} }
		rcf         = func(_ context.Context, peer string) (io.ReadCloser, error) { return infiniteReader(peer), nil }
		sleep       = func(time.Duration) {} // no sleep
		ticker      = time.NewTicker
		sink        = make(chan []byte, 1024)
		done        = make(chan struct{})
	)

	go func() {
		Execute(ctx, pf, rcf, sleep, ticker, sink)
		close(sink)
		close(done)
	}()

	// Drain the records as quickly as possible.
	var (
		draindone = make(chan struct{})
		count     = 0
	)
	go func() {
		defer close(draindone)
		for range sink {
			count++
		}
	}()

	// Run it awhile, then cancel.
	begin := time.Now()
	time.Sleep(250 * time.Millisecond)
	cancel()
	<-draindone
	t.Logf("drained %d in %s", count, time.Since(begin))
	<-done
}

type ctxReader struct {
	ctx context.Context
	rec []byte
	cnt int32
}

func (r *ctxReader) Read(p []byte) (int, error) {
	if atomic.AddInt32(&r.cnt, -1) < 0 {
		return 0, errors.New("count exceeded")
	}
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return copy(p, append(r.rec, '\n')), nil
	}
}

func (*ctxReader) Close() error { return nil }

type infiniteReader string

func (r infiniteReader) Read(p []byte) (int, error) {
	return copy(p, []byte(string(r)+"\n")), nil
}

func (r infiniteReader) Close() error { return nil }
