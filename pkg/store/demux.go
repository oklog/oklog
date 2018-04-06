package store

import (
	"bufio"
	"bytes"
	"errors"
	"io"

	"github.com/oklog/ulid"
)

// Demuxer promotes mixed-topic segments from the staging area to sub-segments
// in their respective topics.
type Demuxer struct {
	next       func() (ReadSegment, error)
	newSegment func(string) (WriteSegment, error)
	topics     map[string]Log
}

// NewDemuxer returns a new Demuxer with the given staging and topic paths.
func NewDemuxer(
	next func() (ReadSegment, error),
	newSegment func(string) (WriteSegment, error),
) *Demuxer {
	return &Demuxer{
		next:       next,
		newSegment: newSegment,
		topics:     map[string]Log{},
	}
}

// Next looks for the next staging segment and demuxes it. It returns ErrNoSegmentFound
// if no segments need to be demuxed.
func (d *Demuxer) Next() (err error) {
	s, err := d.next()
	if err == ErrNoSegmentsAvailable {
		return nil
	}
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
	return s.Purge()
}

func (d *Demuxer) demux(s ReadSegment) (err error) {
	type entry struct {
		started   bool
		low, high ulid.ULID
		seg       WriteSegment
		w         *bufio.Writer
	}
	r := bufio.NewReader(s)
	m := map[string]*entry{}

	defer func() {
		if err != nil {
			for _, e := range m {
				e.seg.Delete()
			}
		}
	}()
	for {
		rec, err := r.ReadBytes('\n')
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
			s, err := d.newSegment(t)
			if err != nil {
				return err
			}
			e = &entry{seg: s, w: bufio.NewWriter(s)}
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
