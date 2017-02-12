package stream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
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
		io.Reader
		io.Writer
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
	readCloserFactory := func(_ context.Context, addr string) (io.ReadCloser, error) {
		return ioutil.NopCloser(nodes[addr]), nil
	}

	// Our stream and deduplicate workers.
	var (
		ctx, cancel = context.WithCancel(context.Background())
		window      = 500 * time.Millisecond
	)
	raw := Execute(
		ctx,
		peerFactory,
		readCloserFactory,
		time.Sleep,
		time.NewTicker,
	)
	deduplicated := Deduplicate(
		raw,
		window,
		time.NewTicker,
	)

	// We gonna send some unique records.
	count := 255

	// But we gonna duplicate and send them out of order.
	go func() {
		writers := make([]io.Writer, 0, len(nodes))
		for _, p := range nodes {
			writers = append(writers, p.Writer)
		}
		for i, value := range rand.Perm(count) {
			id := ulid.MustNew(uint64(value), nil)
			fmt.Fprintf(writers[(i+0)%len(writers)], "%s %d\n", id, value)
			fmt.Fprintf(writers[(i+1)%len(writers)], "%s %d\n", id, value)
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
		t.Fatal("timeout waiting for shutdown after context cancelation")
	}
}
