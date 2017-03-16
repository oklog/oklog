package ingest

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/ulid"
)

// HandleConnections passes each connection from the listener to the connection handler.
// Terminate the function by closing the listener.
func HandleConnections(
	ln net.Listener,
	h ConnectionHandler,
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

		// Create a new entropy source and ID generator for this connection.
		// rand.New is not goroutine safe!
		entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
		idGen := func() string { return ulid.MustNew(ulid.Now(), entropy).String() }

		// Register the connection in the manager, and launch the handler.
		// The handler may exit from the client, or via manager shutdown.
		// In either case, the writer is closed.
		m.register(conn)
		go func() {
			h(conn, w, idGen, connectedClients)
			w.Stop() // make sure it's flushed
			m.remove(conn)
		}()
	}
}

// ConnectionHandler forwards records from the net.Conn to the IngestLog.
type ConnectionHandler func(conn net.Conn, w *Writer, idGen IDGenerator, connectedClients prometheus.Gauge) error

// HandleFastWriter is a ConnectionHandler that writes records to the IngestLog.
func HandleFastWriter(conn net.Conn, w *Writer, idGen IDGenerator, connectedClients prometheus.Gauge) (err error) {
	connectedClients.Inc()
	defer connectedClients.Dec()
	defer conn.Close()
	s := bufio.NewScanner(conn)
	s.Split(scanLinesPreserveNewline)
	for s.Scan() {
		// TODO(pb): short writes are possible
		if _, err := fmt.Fprintf(w, "%s %s", idGen(), s.Text()); err != nil {
			return err
		}
	}
	return s.Err()
}

// HandleDurableWriter is a ConnectionHandler that writes records to the
// IngestLog and syncs after each record.
func HandleDurableWriter(conn net.Conn, w *Writer, idGen IDGenerator, connectedClients prometheus.Gauge) (err error) {
	connectedClients.Inc()
	defer connectedClients.Dec()
	defer conn.Close()
	s := bufio.NewScanner(conn)
	s.Split(scanLinesPreserveNewline)
	for s.Scan() {
		// TODO(pb): short writes are possible
		if _, err := fmt.Fprintf(w, "%s %s", idGen(), s.Text()); err != nil {
			return err
		}
		if err := w.Sync(); err != nil {
			return err
		}
	}
	return s.Err()
}

// HandleBulkWriter is a ConnectionHandler that writes an entire segment to the
// IngestLog at once.
func HandleBulkWriter(conn net.Conn, w *Writer, idGen IDGenerator, connectedClients prometheus.Gauge) (err error) {
	conn.Close()
	return errors.New("TODO(pb): not implemented")
}

// IDGenerator should return unique record identifiers, i.e. ULIDs.
type IDGenerator func() string

// Like bufio.ScanLines, but retain the \n.
func scanLinesPreserveNewline(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[0 : i+1], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

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
