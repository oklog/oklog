package stream

import (
	"context"
	"io"
	"time"
)

// peerFactory returns the latest set of stream sources.
type peerFactory func() []string

// readerFactory converts a stream addr to an io.Reader.
type readerFactory func(context.Context, string) (io.Reader, error)

// runStream creates and maintains streams of records to multiple peers.
// Incoming records are muxed onto the provided sink chan.
// runStream blocks until the context is canceled.
func runStream(ctx context.Context, pf peerFactory, rf readerFactory, sink chan<- []byte) {
	// Invoke the peerFactory to get the initial addrs.
	// Initialize connection managers to each of them.
	active := updateActive(ctx, nil, pf(), rf, sink)

	// Re-invoke the peerFactory every second.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Detect new peers, and create connection managers for them.
			// Terminate connection managers for peers that have gone away.
			active = updateActive(ctx, active, pf(), rf, sink) // update

		case <-ctx.Done():
			// Context cancelation is transitive.
			// We just need to exit.
			return
		}
	}
}

func updateActive(
	parent context.Context,
	prevgen map[string]func(),
	addrs []string,
	factory readerFactory,
	sink chan<- []byte,
) map[string]func() {
	// Create the "new" collection of peer managers.
	// Really, we just have to track the cancel func.
	nextgen := map[string]func(){}

	for _, addr := range addrs {
		if cancel, ok := prevgen[addr]; ok {
			// This addr already exists in our previous collection.
			// Just move its cancel func over to the new collection.
			nextgen[addr] = cancel
			delete(prevgen, addr)
		} else {
			// This addr appears to be new!
			// Create a new connection manager for it.
			ctx, cancel := context.WithCancel(parent)
			go readUntilCanceled(ctx, factory, addr, sink)
			nextgen[addr] = cancel
		}
	}

	// All the addrs left over in the previous collection are gone.
	// Their connection managers should be canceled.
	for _, cancel := range prevgen {
		cancel()
	}

	// Good to go.
	return nextgen
}
