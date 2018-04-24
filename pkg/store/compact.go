package store

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/oklog/oklog/pkg/event"
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
	compactDuration   *prometheus.HistogramVec
	trashSegments     *prometheus.CounterVec
	purgeSegments     *prometheus.CounterVec
	reporter          event.Reporter

	ops []func()
}

// NewCompacter creates a Compacter.
// Don't forget to Run it.
func NewCompacter(
	log Log,
	segmentTargetSize int64, retain time.Duration, purge time.Duration,
	compactDuration *prometheus.HistogramVec, trashSegments, purgeSegments *prometheus.CounterVec,
	reporter event.Reporter,
) *Compacter {
	c := &Compacter{
		log:               log,
		segmentTargetSize: segmentTargetSize,
		retain:            retain,
		purge:             purge,
		trashSegments:     trashSegments,
		purgeSegments:     purgeSegments,
		compactDuration:   compactDuration,
		reporter:          reporter,
	}
	c.ops = []func(){
		func() { c.compact("Overlapping", c.log.Overlapping) },
		func() { c.compact("Sequential", c.log.Sequential) },
		func() { c.moveToTrash() },
		func() { c.emptyTrash() },
	}
	return c
}

// Next runs the next compacter stage.
func (c *Compacter) Next() {
	c.ops[0]()
	c.ops = append(c.ops[1:], c.ops[0])
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
		c.reporter.ReportEvent(event.Event{
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
				c.reporter.ReportEvent(event.Event{
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
	if _, err := mergeRecordsToLog(c.log, c.segmentTargetSize, readers...); err != nil {
		c.reporter.ReportEvent(event.Event{
			Op: "compact", Error: err,
			Msg: fmt.Sprintf("compact %s failed during mergeRecordsToLog", kind),
		})
		return 0, "Error"
	}

	// We've successfully written the merged segment(s).
	// Purge the read segments that were compacted.
	for _, readSegment := range readSegments {
		// If the purge fails, it's OK. We'll have extra duplicate records,
		// which is no big deal, and we might catch it in another pass of the
		// compacter.
		if err := readSegment.Purge(); err != nil {
			c.reporter.ReportEvent(event.Event{
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
		c.reporter.ReportEvent(event.Event{
			Op: "moveToTrash", Error: err,
			Msg: "fetching Trashable read segments failed",
		})
		return
	}
	for _, segment := range readSegments {
		if err := segment.Trash(); err != nil {
			// We can't do anything but log the error.
			c.reporter.ReportEvent(event.Event{
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
		c.reporter.ReportEvent(event.Event{
			Op: "emptyTrash", Error: err,
			Msg: "fetching Purgeable segments failed",
		})
		return
	}
	for _, segment := range trashSegments {
		if err := segment.Purge(); err != nil {
			// We can't do anything but log the error.
			c.reporter.ReportEvent(event.Event{
				Op: "emptyTrash", Error: err,
				Msg: "Purging a read segment failed",
			})
		}
	}
}

// CompacterFactory returns a new Compacter for a Log.
type CompacterFactory func(string, Log) *Compacter

// TopicCompacters runs compacters for a set of topic logs.
type TopicCompacters struct {
	topics   TopicLogs
	cfac     CompacterFactory
	comps    map[string]*Compacter
	stopc    chan chan struct{}
	reporter event.Reporter
}

// NewTopicCompacters returns a new TopicCompacter that creates new compacters with a factory.
func NewTopicCompacters(topics TopicLogs, cfac CompacterFactory, reporter event.Reporter) *TopicCompacters {
	return &TopicCompacters{
		topics:   topics,
		cfac:     cfac,
		comps:    map[string]*Compacter{},
		stopc:    make(chan chan struct{}),
		reporter: reporter,
	}
}

// Run starts running compaction steps for all topics.
func (tc *TopicCompacters) Run() {
	for {
		select {
		case donec := <-tc.stopc:
			close(donec)
			return
		default:
			tc.next()
		}
	}
}

// Stop terminates the topic compacter. It blocks until currently processing
// compactions are completed.
func (tc *TopicCompacters) Stop() {
	donec := make(chan struct{})
	tc.stopc <- donec
	<-donec
}

// next instantiates new compacters for new topics and runs the next compacter
// step for all of them.
func (tc *TopicCompacters) next() {
	all, err := tc.topics.All()
	if err != nil {
		tc.reporter.ReportEvent(event.Event{
			Op: "getTopics", Error: err,
			Msg: "get all topics",
		})
		return
	}
	// Ensure we have a compacter for all topics and run the next stage for each.
	for t, l := range all {
		if _, ok := tc.comps[t]; !ok {
			tc.comps[t] = tc.cfac(t, l)
		}
	}
	for _, c := range tc.comps {
		c.Next()
	}
}
