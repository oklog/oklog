package store

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/oklog/oklog/pkg/event"
	"github.com/oklog/oklog/pkg/record"
	"github.com/oklog/ulid"
)

// Demuxer promotes mixed-topic segments from the staging area to sub-segments
// in their respective topics.
type Demuxer struct {
	staging   Log
	topicLogs TopicLogs
	topics    map[string]Log
	stopc     chan chan struct{}
	reporter  event.Reporter
}

// NewDemuxer returns a new Demuxer with the given staging and topic paths.
func NewDemuxer(staging Log, topicsLogs TopicLogs, reporter event.Reporter) *Demuxer {
	return &Demuxer{
		staging:   staging,
		topicLogs: topicsLogs,
		topics:    map[string]Log{},
		stopc:     make(chan chan struct{}),
		reporter:  reporter,
	}
}

// Run demuxes new segments in the staging log. If no segments are found, it retries
// after the given interval.
func (d *Demuxer) Run(interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case donec := <-d.stopc:
			close(donec)
			return
		case <-tick.C:
		}
		for {
			err := d.next()
			if err == nil {
				continue
			}
			// Wait for the next tick on any error we encounter to avoid spinning
			// in unrecovering conditions.
			if err != ErrNoSegmentsAvailable {
				d.reporter.ReportEvent(event.Event{
					Op: "demux", Error: err,
					Msg: fmt.Sprintf("demux failed"),
				})
			}
			break
		}
	}
}

// Stop terminate the demuxer. It blocks until any current processing completed.
func (d *Demuxer) Stop() {
	donec := make(chan struct{})
	d.stopc <- donec
	<-donec
}

// next looks for the next staging segment and demuxes it. It returns ErrNoSegmentFound
// if no segments need to be demuxed.
// It returns ErrNoSegmentAvailable if there are no more segments to process.
func (d *Demuxer) next() (err error) {
	s, err := d.staging.Oldest()
	if err != nil {
		return err
	}
	// Purge segment from staging log on succes.
	// Fail whole segment if we encounter an error on demuxing.
	if err := d.demux(s); err != nil {
		// TODO(fabxc): irrecoverable errors (e.g bad content) will cause indefinitie
		// retries. Move segment into a "quarantine" dir?
		s.Reset()
		return err
	}
	err = s.Purge()
	return err
}

func (d *Demuxer) demux(s ReadSegment) (err error) {
	type entry struct {
		started   bool
		low, high ulid.ULID
		seg       WriteSegment
		w         *bufio.Writer
	}
	m := map[string]*entry{}
	r := record.NewReader(s)

	defer func() {
		if err != nil {
			for _, e := range m {
				e.seg.Delete()
			}
		}
	}()
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		t, id, rec, err := demuxRecord(rec)
		if err != nil {
			return err
		}
		e, ok := m[t]
		if !ok {
			s, err := d.topicLogs.Create(t)
			if err != nil {
				return err
			}
			e = &entry{seg: s, w: bufio.NewWriterSize(s, 2*1024*1024)}
			m[t] = e
		}
		if !e.started {
			e.low = id
			e.started = true
		}
		e.high = id

		if _, err := e.w.Write(rec); err != nil {
			return err
		}
	}

	// Flush and finalize all new topic segments.
	for _, e := range m {
		if err := e.w.Flush(); err != nil {
			return err
		}
		if err := e.seg.Close(e.low, e.high); err != nil {
			return err
		}
	}
	return nil
}

// demuxRecord extracts the topic from record b and returns a record with the topc
// prefix stripped away.
func demuxRecord(b []byte) (t string, id ulid.ULID, rec []byte, err error) {
	p := bytes.SplitN(b, []byte(" "), 3)
	if len(p) != 3 || len(p[0]) != ulid.EncodedSize {
		return "", id, nil, errors.New("bad format")
	}
	id, err = ulid.Parse(string(b[:ulid.EncodedSize]))
	if err != nil {
		return "", id, nil, err
	}
	// Execution of return arguments is not ordered. We must cast the topic
	// to a string separately.
	t = string(p[1])
	return t, id, append(b[:ulid.EncodedSize+1], p[2]...), nil
}
