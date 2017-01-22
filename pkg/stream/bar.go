package stream

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// readUntilCanceled is a kind of connection manager to the given addr.
// We connect to addr via the factory, read records, and put them on the sink.
// Any connection error causes us to wait a second and then reconnect.
// readUntilCanceled blocks until the context is canceled.
func readUntilCanceled(ctx context.Context, factory readerFactory, addr string, sink chan<- []byte) {
	for {
		switch readOnce(ctx, factory, addr, sink) {
		case context.Canceled:
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

func readOnce(ctx context.Context, factory readerFactory, addr string, sink chan<- []byte) error {
	r, err := factory(ctx, addr)
	if err != nil {
		return err
	}
	s := bufio.NewScanner(r)
	for s.Scan() {
		sink <- append(s.Bytes(), '\n')
	}
	return s.Err()
}

func httpReaderFactory(client *http.Client, addr2url func(string) string) readerFactory {
	return func(ctx context.Context, addr string) (io.Reader, error) {
		req, err := http.NewRequest("GET", addr2url(addr), nil)
		if err != nil {
			return nil, errors.Wrap(err, "NewRequest")
		}
		resp, err := client.Do(req.WithContext(ctx))
		if err != nil {
			return nil, errors.Wrap(err, "Do")
		}
		if resp.StatusCode != http.StatusOK {
			return nil, errors.Errorf("GET: %s", resp.Status)
		}
		return resp.Body, nil
	}
}
