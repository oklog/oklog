package store

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid"
)

func TestQueryRegistry(t *testing.T) {
	t.Parallel()

	// Set up an empty query registry.
	qr := newQueryRegistry()
	defer qr.Close()

	// Register a query for 'foo' records.
	fooctx, foocancel := context.WithCancel(context.Background())
	fooc := qr.Register(fooctx, recordFilterPlain([]byte("foo")))

	// Register a query for 'bar' records.
	barctx, barcancel := context.WithCancel(context.Background())
	barc := qr.Register(barctx, recordFilterPlain([]byte("bar")))

	// A helper function to generate segments.
	nopulid := ulid.MustNew(0, nil).String()
	makeSegment := func(records ...string) []byte {
		var buf bytes.Buffer
		for _, record := range records {
			fmt.Fprintf(&buf, "%s %s\n", nopulid, record)
		}
		return buf.Bytes()
	}

	// Push a segment through that should match foo.
	go qr.Match(makeSegment("no match", "abc foo def", "no match again"))
	select {
	case foo := <-fooc:
		if want, have := nopulid+" abc foo def\n", string(foo); want != have {
			t.Errorf("want %q, have %q", want, have)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout on first Match")
	}

	// Cancel the foo query, make sure the chan gets closed.
	foocancel()
	select {
	case foo, ok := <-fooc:
		if ok {
			t.Errorf("got unexpected record on foo chan: %s", foo)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout on foocancel")
	}

	// Push a segment through that matches foo and bar.
	// Since foo was canceled, it shouldn't try to push records there.
	// If it did, it would block forever, and we'd get a timeout.
	go qr.Match(makeSegment("match on foo, but no foo registered", "abc bar def", "again, no match"))
	select {
	case bar := <-barc:
		if want, have := nopulid+" abc bar def\n", string(bar); want != have {
			t.Errorf("want %q, have %q", want, have)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout on second Match")
	}

	// Cancel the bar query, make sure the chan gets closed.
	barcancel()
	select {
	case bar, ok := <-barc:
		if ok {
			t.Errorf("got unexpected record on bar chan: %s", bar)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout on barcancel")
	}
}

func TestQueryRegistryClose(t *testing.T) {
	t.Parallel()

	qr := newQueryRegistry()
	c := qr.Register(context.Background(), recordFilterPlain([]byte("")))
	qr.Close()
	select {
	case _, ok := <-c:
		if ok {
			t.Errorf("expected closed channel, got actual record")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestQueryRegistryRaces(t *testing.T) {
	t.Parallel()

	// We just gonna do a lot of stuff concurrently.
	// Try and trigger the race detector.
	qr := newQueryRegistry()
	n := 100

	// Manufacture a bunch of segments.
	segments := make([][]byte, n)
	for i := 0; i < n; i++ {
		var buf bytes.Buffer
		for j := 0; j < 50; j++ {
			fmt.Fprintf(&buf,
				"%s %s\n",
				ulid.MustNew(uint64(j), nil).String(),
				strings.Repeat(strconv.Itoa(j), 10),
			)
		}
		segments[i] = buf.Bytes()
	}

	// Register a bunch of queries, wait a bit, then cancel them.
	var (
		rd uint64
		wg sync.WaitGroup
	)
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		records := qr.Register(ctx, recordFilterPlain([]byte("")))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range records {
				atomic.AddUint64(&rd, 1)
			}
		}()
		go func() {
			time.Sleep(time.Second + time.Duration(rand.Intn(100))*time.Millisecond)
			cancel() // trigger the other goroutine to exit
		}()
	}

	// Match the bunch of segments.
	for i := 0; i < n; i++ {
		go func(segment []byte) { qr.Match(segment) }(segments[i])
	}

	// Wait for it all to finish.
	wg.Wait()
	if err := qr.Close(); err != nil {
		t.Error(err)
	}
	t.Logf("%d chan reads", atomic.LoadUint64(&rd))
}
