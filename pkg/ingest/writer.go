package ingest

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// NewWriter converts a Log to an io.Writer. Active segments are rotated
// once sz bytes are written, or every d if the segment is nonempty.
func NewWriter(log Log, sz int, d time.Duration, bytes, records, syncs prometheus.Counter, rotations *prometheus.CounterVec) (*Writer, error) {
	curr, err := log.Create()
	if err != nil {
		return nil, err
	}
	w := &Writer{
		log:       log,
		curr:      curr,
		cursz:     0,
		maxsz:     sz,
		action:    make(chan func()),
		bytes:     bytes,
		records:   records,
		syncs:     syncs,
		rotations: rotations,
		stop:      make(chan chan struct{}),
	}
	go w.loop(d)
	return w, nil
}

// Writer implements io.Writer on top of a Log.
type Writer struct {
	log       Log
	curr      WriteSegment
	cursz     int
	maxsz     int
	action    chan func()
	bytes     prometheus.Counter
	records   prometheus.Counter
	syncs     prometheus.Counter
	rotations *prometheus.CounterVec
	stop      chan chan struct{}
}

// Write implements io.Writer.
func (w *Writer) Write(p []byte) (int, error) {
	type res struct {
		n   int
		err error
	}
	c := make(chan res)
	w.action <- func() {
		n, err := w.curr.Write(p)
		if err != nil {
			c <- res{n, err}
			return
		}
		w.bytes.Add(float64(n))
		w.records.Inc()
		w.cursz += n
		if w.cursz >= w.maxsz {
			w.rotations.WithLabelValues("too_big").Inc()
			w.closeRotate()
		}
		c <- res{n, err}
	}
	r := <-c
	return r.n, r.err
}

// Sync the current segment to disk.
func (w *Writer) Sync() error {
	c := make(chan error)
	w.action <- func() {
		c <- w.curr.Sync()
		w.syncs.Inc()
	}
	return <-c
}

// Stop terminates the Writer. No further writes are allowed.
func (w *Writer) Stop() {
	c := make(chan struct{})
	w.stop <- c
	<-c
}

// loop serializes the events that hit the Writer. That includes user requests,
// like Write, Sync, and Stop; and the time.Ticker that controls time-based
// segment rotation.
//
// We need this single point of synchronization only because of the time-based
// segment rotation, which is asynchronous. Without that, we could control
// everything pretty elegantly from the Write method via a simple mutex.
func (w *Writer) loop(d time.Duration) {
	rotate := time.NewTicker(d)
	defer rotate.Stop()
	for {
		select {
		case f := <-w.action:
			f()

		case <-rotate.C:
			// Note we invoke closeRotate every d, even if it's been only a
			// short while since the last flush to disk. This could be optimized
			// by only starting the timer once bytes are written and resetting
			// it with every segment rotation, at the cost of some garbage
			// generation. Profiling data is necessary.
			w.rotations.WithLabelValues("too_old").Inc()
			w.closeRotate()

		case c := <-w.stop:
			w.closeOnly()
			w.stop = nil
			close(c)
			return
		}
	}
}

func (w *Writer) closeRotate() {
	if w.cursz <= 0 {
		// closeRotate is called, but the segment is empty!
		// We can just keep it open, instead of cycling it.
		return
	}
	if w.curr != nil {
		if err := w.curr.Close(); err != nil {
			panic(err)
		}
	}
	next, err := w.log.Create()
	if err != nil {
		panic(err)
	}
	w.curr, w.cursz = next, 0
}

func (w *Writer) closeOnly() {
	// This function exists because we need to rotate the active segment away
	// when the user requests a stop. That is, we shouldn't leave an active
	// segment lying around.
	if w.curr != nil {
		if w.cursz <= 0 {
			// closeOnly is called, but the segment is empty!
			// Delete the active segment instead of syncing it.
			w.curr.Delete()
		} else {
			if err := w.curr.Close(); err != nil {
				panic(err)
			}
		}
		w.curr, w.cursz = nil, 0
	}
}
