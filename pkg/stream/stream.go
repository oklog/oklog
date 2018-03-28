package stream

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// PeerFactory should return the current set of peer addresses.
// Each address will be converted to an io.Reader via the ReadCloserFactory.
// The PeerFactory is periodically invoked to get the latest set of peers.
type PeerFactory func() []string

// ReadCloserFactory converts a peer address to an io.ReadCloser.
// ReadClosers must exit with context.Canceled when the context is canceled.
// Other errors will cause the managing goroutine to remanufacture.
type ReadCloserFactory func(context.Context, string) (io.ReadCloser, error)

type canceldone struct {
	cancel func()
	done   <-chan struct{}
}

// Execute creates and maintains streams of records to multiple peers.
// It muxes the streams of incoming records to the sink chan of records.
// It's designed to be invoked once per user stream request.
//
// The sleep func is used to backoff between retries of a single peer.
// The ticker func is used to regularly resolve peers.
func Execute(
	ctx context.Context,
	pf PeerFactory,
	rcf ReadCloserFactory,
	sleep func(time.Duration),
	ticker func(time.Duration) *time.Ticker,
	sink chan<- []byte,
) error {
	// Invoke the PeerFactory to get the initial addrs.
	// Initialize connection managers to each of them.
	active := updateActive(ctx, nil, pf(), rcf, sink, sleep)

	// Re-invoke the peerFactory every second.
	// This catches changes in topology.
	tk := ticker(time.Second)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			// Detect new peers, and create connection managers for them.
			// Terminate connection managers for peers that have gone away.
			active = updateActive(ctx, active, pf(), rcf, sink, sleep)

		case <-ctx.Done():
			// Context cancelation is transitive.
			// We just need to wait.
			for _, cd := range active {
				<-cd.done
			}
			return ctx.Err()
		}
	}
}

func updateActive(
	parent context.Context,
	prevgen map[string]canceldone,
	addrs []string,
	rcf ReadCloserFactory,
	sink chan<- []byte,
	sleep func(time.Duration),
) map[string]canceldone {
	// Create the "new" collection of peer managers.
	// Really, we just have to track the cancel func.
	nextgen := map[string]canceldone{}

	// The addrs represent all the connections we *should* have.
	for _, addr := range addrs {
		if cd, ok := prevgen[addr]; ok {
			// This addr already exists in our previous collection.
			// Just move its cancel func over to the new collection.
			nextgen[addr] = cd
			delete(prevgen, addr)
		} else {
			// This addr appears to be new!
			// Create a new connection manager for it.
			var (
				ctx, cancel = context.WithCancel(parent)
				done        = make(chan struct{})
			)
			go func(addr string) {
				readUntilCanceled(ctx, rcf, addr, sink, sleep)
				close(done)
			}(addr)
			nextgen[addr] = canceldone{cancel, done}
		}
	}

	// All the addrs left over in the previous collection are gone.
	// Their connection managers should be canceled.
	for _, cd := range prevgen {
		cd.cancel()
		<-cd.done
	}

	// Good to go.
	return nextgen
}

// readUntilCanceled is a kind of connection manager to the given addr.
// We connect to addr via the factory, read records, and put them on the sink.
// Any connection error causes us to wait a second and then reconnect.
// readUntilCanceled blocks until the context is canceled.
func readUntilCanceled(ctx context.Context, rcf ReadCloserFactory, addr string, sink chan<- []byte, sleep func(time.Duration)) {
	for {
		readOnce(ctx, rcf, addr, sink)
		select {
		case <-ctx.Done():
			return
		default:
			sleep(time.Second) // TODO(pb): better strategy?
		}
	}
}

// readOnce uses rcf to construct a ReadCloser to the given addr, and consumes
// and forwards records to the sink until
func readOnce(ctx context.Context, rcf ReadCloserFactory, addr string, sink chan<- []byte) error {
	rc, err := rcf(ctx, addr)
	if err != nil {
		return err
	}
	defer rc.Close()
	s := bufio.NewScanner(rc)
	for s.Scan() {
		select {
		case sink <- []byte(s.Text()):
			// We use s.Text to copy the record out of the Scanner.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Err()
}

// HTTPReadCloserFactory returns a ReadCloserFactory that converts the addr to a
// URL via the addr2url function, makes a GET request via the client, and
// returns the response body as the ReadCloser.
func HTTPReadCloserFactory(client Doer, addr2url func(string) string) ReadCloserFactory {
	return func(ctx context.Context, addr string) (io.ReadCloser, error) {
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

// Doer models http.Client.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}
