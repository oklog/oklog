package store

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Compacter is responsible for all post-flush segment mutation. That includes
// compacting highly-overlapping segments, compacting small and sequential
// segments, and enforcing the retention window.
type Compacter struct {
	log               Log
	segmentTargetSize int64
	retain            time.Duration
	purge             time.Duration
	stop              chan chan struct{}
	compactDuration   *prometheus.HistogramVec
	compactBytesWritten  prometheus.Counter
	trashSegments     *prometheus.CounterVec
	purgeSegments     *prometheus.CounterVec
	reporter          EventReporter
}

// NewCompacter creates a Compacter.
// Don't forget to Run it.
func NewCompacter(
	log Log,
	segmentTargetSize int64, retain time.Duration, purge time.Duration,
	compactDuration *prometheus.HistogramVec, compactBytesWritten prometheus.Counter, trashSegments, purgeSegments *prometheus.CounterVec,
	reporter EventReporter,
) *Compacter {
	return &Compacter{
		log:               log,
		segmentTargetSize: segmentTargetSize,
		retain:            retain,
		purge:             purge,
		stop:              make(chan chan struct{}),
		trashSegments:     trashSegments,
		purgeSegments:     purgeSegments,
		compactDuration:   compactDuration,
		compactBytesWritten: compactBytesWritten,
		reporter:          reporter,
	}
}

// Run performs compactions and cleanups.
// Run returns when Stop is invoked.
func (c *Compacter) Run() {
	ops := []func(){
		func() { c.compact("Overlapping", c.log.Overlapping) },
		func() { c.compact("Sequential", c.log.Sequential) },
		func() { c.moveToTrash() },
		func() { c.emptyTrash() },
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ops[0]()                      // execute
			ops = append(ops[1:], ops[0]) // shift

		case q := <-c.stop:
			close(q)
			return
		}
	}
}

// Stop the compacter from compacting.
func (c *Compacter) Stop() {
	defer func(begin time.Time) {
		c.reporter.ReportEvent(Event{
			Debug: true, Op: "Stop",
			Msg: fmt.Sprintf("shutdown took %s", time.Since(begin)),
		})
	}(time.Now())
	q := make(chan struct{})
	c.stop <- q
	<-q
}

func (c *Compacter) compact(kind string, getSegments func() ([]ReadSegment, error)) (compacted int, result string) {
	defer func(begin time.Time) {
		c.compactDuration.WithLabelValues(
			kind,
			strconv.Itoa(compacted),
			result,
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	// Fetch the segments that can be compacted.
	readSegments, err := getSegments()
	if err == ErrNoSegmentsAvailable {
		return 0, "NoSegmentsAvailable" // no problem
	}
	if err != nil {
		c.reporter.ReportEvent(Event{
			Op: "compact", Error: err,
			Msg: fmt.Sprintf("compact %s failed during getSegments", kind),
		})
		return 0, "Error"
	}
	defer func() {
		// Make sure ReadSegments are cleaned up.
		for _, readSegment := range readSegments {
			if err := readSegment.Reset(); err != nil {
				// We can't do anything but log the error.
				c.reporter.ReportEvent(Event{
					Op: "compact", Error: err,
					Msg: fmt.Sprintf("compact %s failed to Reset a read segment", kind),
				})
			}
		}
	}()

	// Merge and write all of the read segments into the log.
	// It may create multiple segments, if it's too much data.
	// That's why we use the specialized mergeRecordsToLog.
	readers := make([]io.Reader, len(readSegments))
	for i, readSegment := range readSegments {
		readers[i] = readSegment
	}
	nbytes, err := mergeRecordsToLog(c.log, c.segmentTargetSize, readers...)
	if err != nil {
		c.reporter.ReportEvent(Event{
			Op: "compact", Error: err,
			Msg: fmt.Sprintf("compact %s failed during mergeRecordsToLog", kind),
		})
		return 0, "Error"
	}

	c.compactBytesWritten.Add(float64(nbytes))
	// We've successfully written the merged segment(s).
	// Purge the read segments that were compacted.
	for _, readSegment := range readSegments {
		// If the purge fails, it's OK. We'll have extra duplicate records,
		// which is no big deal, and we might catch it in another pass of the
		// compacter.
		if err := readSegment.Purge(); err != nil {
			c.reporter.ReportEvent(Event{
				Op: "compact", Warning: err,
				Msg: fmt.Sprintf("compact %s failed to Purge a read segment, which is not critical", kind),
			})
		}
	}

	// Return.
	n := len(readSegments)
	readSegments = []ReadSegment{} // for the deferred cleanup
	return n, "OK"
}

func (c *Compacter) moveToTrash() {
	oldestRecord := time.Now().Add(-c.retain)
	readSegments, err := c.log.Trashable(oldestRecord)
	if err == ErrNoSegmentsAvailable {
		return // no problem
	}
	if err != nil {
		c.reporter.ReportEvent(Event{
			Op: "moveToTrash", Error: err,
			Msg: "fetching Trashable read segments failed",
		})
		return
	}
	for _, segment := range readSegments {
		if err := segment.Trash(); err != nil {
			// We can't do anything but log the error.
			c.reporter.ReportEvent(Event{
				Op: "moveToTrash", Error: err,
				Msg: "Trashing a read segment failed",
			})
		}
	}
}

func (c *Compacter) emptyTrash() {
	oldestModTime := time.Now().Add(-c.purge)
	trashSegments, err := c.log.Purgeable(oldestModTime)
	if err == ErrNoSegmentsAvailable {
		return // no problem
	}
	if err != nil {
		c.reporter.ReportEvent(Event{
			Op: "emptyTrash", Error: err,
			Msg: "fetching Purgeable segments failed",
		})
		return
	}
	for _, segment := range trashSegments {
		if err := segment.Purge(); err != nil {
			// We can't do anything but log the error.
			c.reporter.ReportEvent(Event{
				Op: "emptyTrash", Error: err,
				Msg: "Purging a read segment failed",
			})
		}
	}
}
