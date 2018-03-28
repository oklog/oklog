package stream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/oklog/ulid"
)

func TestCombined(t *testing.T) {
	// This test combines Stream and Deduplicate, like in the API.
	t.Parallel()

	// Our three nodes.
	type pipe struct {
		*io.PipeReader
		*io.PipeWriter
	}
	var (
		addrs = []string{"foo", "bar", "baz"}
		nodes = map[string]pipe{}
	)
	for _, addr := range addrs {
		r, w := io.Pipe()
		nodes[addr] = pipe{r, w}
	}

	// Factories for our nodes.
	peerFactory := func() []string {
		return addrs
	}
	readCloserFactory := func(ctx context.Context, addr string) (io.ReadCloser, error) {
		return newContextReadCloser(ctx, struct {
			io.Reader
			io.Closer
		}{
			Reader: nodes[addr].PipeReader,
			Closer: nodes[addr].PipeWriter,
		}), nil
	}

	// Our stream and deduplicate workers, just like in the API.
	var (
		ctx, cancel = context.WithCancel(context.Background())
		window      = 500 * time.Millisecond
	)
	raw := make(chan []byte, 1024)
	go func() {
		Execute(ctx, peerFactory, readCloserFactory, time.Sleep, time.NewTicker, raw)
		close(raw)
	}()
	deduplicated := make(chan []byte, 1024)
	go func() {
		Deduplicate(raw, window, time.NewTicker, deduplicated)
		close(deduplicated)
	}()

	// We gonna send some unique records.
	count := 255

	// But we gonna duplicate and send them out of order.
	go func() {
		writers := make([]io.Writer, 0, len(nodes))
		for _, p := range nodes {
			writers = append(writers, p.PipeWriter)
		}
		for i, value := range rand.Perm(count) {
			id := ulid.MustNew(uint64(value), nil)
			fmt.Fprintf(writers[(i+0)%len(writers)], "%s %d\n", id, value)
			fmt.Fprintf(writers[(i+1)%len(writers)], "%s %d\n", id, value)
			fmt.Fprintf(writers[(i+2)%len(writers)], "%s %d\n", id, value)
		}
	}()

	// Verify we got what we want.
	for i := 0; i < count; i++ {
		id := ulid.MustNew(uint64(i), nil)
		want := []byte(fmt.Sprintf("%s %d", id, i))
		have := <-deduplicated
		if bytes.Compare(want, have) != 0 {
			t.Fatalf("verifying: want %q, have %q", want, have)
		}
	}

	// All done. Make sure we shut down cleanly.
	cancel()
	select {
	case record, ok := <-deduplicated:
		if ok {
			t.Errorf("got unexpected record after context cancelation: %q", record)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Execute and Deduplicate shutdown after context cancelation")
	}
}

func newContextReadCloser(ctx context.Context, rc io.ReadCloser) io.ReadCloser {
	go func() {
		<-ctx.Done()
		rc.Close()
	}()
	return rc
}
