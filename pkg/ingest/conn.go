package ingest

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/ulid"
)

// HandleConnections passes each connection from the listener to the connection handler.
// Terminate the function by closing the listener.
func HandleConnections(ln net.Listener, w *Writer, h ConnectionHandler, connectedClients prometheus.Gauge) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		// rand.New is not goroutine safe, need 1 per connection!
		entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
		idGen := func() string { return ulid.MustNew(ulid.Now(), entropy).String() } // TODO(pb): could improve efficiency
		go h(conn, w, idGen, connectedClients)
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
	for s.Scan() {
		// TODO(pb): short writes are possible
		if _, err := fmt.Fprintf(w, "%s %s\n", idGen(), s.Text()); err != nil {
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
	for s.Scan() {
		// TODO(pb): short writes are possible
		if _, err := fmt.Fprintf(w, "%s %s\n", idGen(), s.Text()); err != nil {
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

// IDGenerator should return unique record identifiers, e.g. ULIDs.
type IDGenerator func() string
