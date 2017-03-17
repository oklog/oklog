package store

import (
	"bufio"
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// queryRegistry holds active streaming queries.
type queryRegistry struct {
	mtx     sync.RWMutex
	reg     chanmap
	closing bool
}

type chanmap map[chan<- []byte]queryContext

type queryContext struct {
	pass   recordFilter
	done   <-chan struct{}
	cancel func()
}

func newQueryRegistry() *queryRegistry {
	return &queryRegistry{
		reg: chanmap{},
	}
}

// Register a new query. If successful, range over the returned chan for
// incoming records. If not successful, the returned chan will be nil.
// Records on the returned chan will NOT have trailing newlines.
func (qr *queryRegistry) Register(ctx context.Context, pass recordFilter) <-chan []byte {
	qr.mtx.Lock()
	defer qr.mtx.Unlock()

	// Don't accept new registrations if we're shutting down.
	if qr.closing {
		return nil
	}

	// Queries are typically deregistered when the parent context is canceled.
	// So we build our lifecycle management purely on context cancelation.
	// But what happens when we want to stop the queryRegistry itself?
	// We need a side-channel way to cancel each registered query.
	subctx, cancel := context.WithCancel(ctx)

	// Create the record chan, and register it.
	// TODO(pb): validate the buffer size
	c := make(chan []byte, 1024)
	qr.reg[c] = queryContext{pass, subctx.Done(), cancel}

	// Canceling the context should deregister the query and close the chan.
	// Spawn a cleanup goroutine to wait for the cancelation and do just that.
	// The cancel may come while we're doing a batch of Match sends.
	// That's fine; we detect it there, too, and stop sending records.
	// But we leave all cleanup duties for this little goroutine here.
	// Note: this is the ONLY place where we can close the records chan!
	go func() {
		<-subctx.Done()       // wait for cancel
		qr.mtx.Lock()         // take the write lock
		defer qr.mtx.Unlock() //
		delete(qr.reg, c)     // deregister the query
		close(c)              // signal we're done
	}()

	// The user should range over this chan for matching records.
	return c
}

func (qr *queryRegistry) Close() error {
	qr.mtx.Lock()
	defer qr.mtx.Unlock()

	// We're shutting down. No more registrations.
	qr.closing = true

	// Channels must have a single "owner" i.e. closer.
	// We've already given that responsibility to the cleanup goroutine.
	// So we need to trigger it, via our side-channel cancelation mechanism.
	for _, qc := range qr.reg {
		qc.cancel()
	}

	// Block until everything is done.
	// This is a little hacky. That's fine. I think.
	timeout := time.After(1 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		qr.mtx.Unlock()
		select {
		case <-ticker.C:
			qr.mtx.Lock()
			if len(qr.reg) <= 0 {
				return nil // empty, we're done
			}

		case <-timeout:
			qr.mtx.Lock()
			return errors.Errorf("timeout waiting for query registry shutdown; %d remain", len(qr.reg))
		}
	}
}

// Match a segment of records against the set of registered queries.
// The function may block if channel receivers are slow.
// Perhaps best to run in a goroutine?
func (qr *queryRegistry) Match(segment []byte) {
	qr.mtx.RLock()
	defer qr.mtx.RUnlock()

	// Don't bother if we don't have any registered queries.
	if len(qr.reg) <= 0 {
		return
	}

	// Don't bother if we're closing.
	if qr.closing {
		return
	}

	// Match each record in the segment against the registered queries.
	// Send any matches immediately.
	s := bufio.NewScanner(bytes.NewReader(segment))
	s.Split(scanLinesPreserveNewline)
	for s.Scan() {
		for c, qc := range qr.reg {
			if qc.pass(s.Bytes()) {
				select {
				case c <- []byte(s.Text()):
					// We use s.Text to copy the record out of the Scanner.

				case <-qc.done:
					// We're canceled! The cancelation was also detected by the
					// cleanup goroutine spawned by Register. That goroutine is
					// in charge of deregistering the query and closing the
					// chan. For our part, we should just stop sending records
					// to this chan.
					continue
				}
			}
		}
	}
}
