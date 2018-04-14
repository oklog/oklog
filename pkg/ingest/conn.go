package ingest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/oklog/pkg/record"
	"github.com/oklog/ulid"
)

// HandleConnections passes each connection from the listener to the connection handler.
// Terminate the function by closing the listener.
func HandleConnections(
	ln net.Listener,
	h ConnectionHandler,
	rfac record.ReaderFactory,
	log Log,
	segmentFlushAge time.Duration,
	segmentFlushSize int,
	connectedClients prometheus.Gauge,
	bytes, records, syncs prometheus.Counter,
	segmentAge, segmentSize prometheus.Histogram,
) error {
	// We shouldn't return until all connections are terminated.
	m := newConnectionManager()
	defer m.shutdown()

	for {
		// Accept a connection.
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		// Create a new writer for this connection.
		// It's important that it be closed.
		w, err := NewWriter(log, segmentFlushAge, segmentFlushSize, bytes, records, syncs, segmentAge, segmentSize)
		if err != nil {
			return err
		}

		// Create a new logical clock for the stream and ID generator for this connection.
		clock := newStreamClock()
		idGen := func() string { return ulid.MustNew(ulid.Now(), clock).String() }

		// Register the connection in the manager, and launch the handler.
		// The handler may exit from the client, or via manager shutdown.
		// In either case, the writer is closed.
		m.register(conn)
		go func() {
			defer conn.Close()
			h(rfac(conn), w, idGen, connectedClients)
			w.Stop() // make sure it's flushed
			m.remove(conn)
		}()
	}
}

// ConnectionHandler forwards records from the net.Conn to the IngestLog.
type ConnectionHandler func(r record.Reader, w *Writer, idGen IDGenerator, connectedClients prometheus.Gauge) error

// HandleFastWriter is a ConnectionHandler that writes records to the IngestLog.
func HandleFastWriter(r record.Reader, w *Writer, idGen IDGenerator, connectedClients prometheus.Gauge) (err error) {
	connectedClients.Inc()
	defer connectedClients.Dec()

	for {
		record, err := r()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// TODO(pb): short writes are possible
		if _, err := fmt.Fprintf(w, "%s %s", idGen(), record); err != nil {
			return err
		}
	}
}

// HandleDurableWriter is a ConnectionHandler that writes records to the
// IngestLog and syncs after each record.
func HandleDurableWriter(r record.Reader, w *Writer, idGen IDGenerator, connectedClients prometheus.Gauge) (err error) {
	connectedClients.Inc()
	defer connectedClients.Dec()

	for {
		record, err := r()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// TODO(pb): short writes are possible
		if _, err := fmt.Fprintf(w, "%s %s", idGen(), record); err != nil {
			return err
		}
		if err := w.Sync(); err != nil {
			return err
		}
	}
}

// HandleBulkWriter is a ConnectionHandler that writes an entire segment to the
// IngestLog at once.
func HandleBulkWriter(r record.Reader, w *Writer, idGen IDGenerator, connectedClients prometheus.Gauge) (err error) {
	return errors.New("TODO(pb): not implemented")
}

// IDGenerator should return unique record identifiers, i.e. ULIDs.
type IDGenerator func() string

func newConnectionManager() *connectionManager {
	return &connectionManager{
		active: map[string]net.Conn{},
	}
}

type connectionManager struct {
	mtx    sync.RWMutex
	active map[string]net.Conn
}

func (m *connectionManager) register(conn net.Conn) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.active[conn.RemoteAddr().String()] = conn
}

func (m *connectionManager) remove(conn net.Conn) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	delete(m.active, conn.RemoteAddr().String())
}

func (m *connectionManager) shutdown() {
	m.closeAllConnections()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if m.isEmpty() {
			return
		}
	}
}

func (m *connectionManager) closeAllConnections() {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	for _, conn := range m.active {
		conn.Close()
	}
}

func (m *connectionManager) isEmpty() bool {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	return len(m.active) <= 0
}

// streamClock is a logical clock that is initialized to start at a random offset
// that identifies a stream.
// It's guaranteed that the 4 high bytes are 0 initially. If they overflow, the random
// component is incremented by 1, effectively creating a new stream identifier.
// It is not safe to be used concurrently.
type streamClock struct {
	prefix [2]byte
	clock  uint64
}

func newStreamClock() *streamClock {
	var sc streamClock
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	binary.BigEndian.PutUint16(sc.prefix[:], uint16(r.Int()))
	// The first 4 bytes of the clock are initialized randomly. The first bit is always
	// zero so the random part just increments by 1 if the high 4 bytes overflow.
	sc.clock = uint64(r.Int31()) << 32

	return &sc
}

// Read populates b with the next clock tick and increments the clock.
// b must be of length 10.
func (sc *streamClock) Read(b []byte) (int, error) {
	if len(b) != 10 {
		return 0, fmt.Errorf("illegal read of length %d, expected 10", len(b))
	}
	copy(b, sc.prefix[:])
	binary.BigEndian.PutUint64(b[2:], sc.clock)

	sc.clock++

	return len(b), nil
}
