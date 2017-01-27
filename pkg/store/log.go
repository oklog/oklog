package store

import (
	"errors"
	"io"
	"time"

	"github.com/oklog/ulid"
)

// Log is an abstraction for segments on a storage node.
type Log interface {
	// Create a new segment for writes.
	Create() (WriteSegment, error)

	// Query written and closed segments.
	Query(qp QueryParams, statsOnly bool) (QueryResult, error)

	// Overlapping returns segments that have a high degree of time overlap and
	// can be compacted.
	Overlapping() ([]ReadSegment, error)

	// Sequential returns segments that are small and sequential and can be
	// compacted.
	Sequential() ([]ReadSegment, error)

	// Trashable segments are read segments whose newest record is older than
	// the given time. They may be trashed, i.e. made unavailable for querying.
	Trashable(oldestRecord time.Time) ([]ReadSegment, error)

	// Purgable segments are trash segments whose modification time (i.e. the
	// time they were trashed) is older than the given time. They may be purged,
	// i.e. hard deleted.
	Purgeable(oldestModTime time.Time) ([]TrashSegment, error)

	// Stats of the current state of the store log.
	Stats() (LogStats, error)

	// Close the log, releasing any claimed lock.
	Close() error
}

// ErrNoSegmentsAvailable is returned by various methods to
// indicate no qualifying segments are currently available.
var ErrNoSegmentsAvailable = errors.New("no segments available")

// WriteSegment can be written to, and either closed or deleted.
type WriteSegment interface {
	io.Writer
	Close(low, high ulid.ULID) error
	Delete() error
}

// ReadSegment can be read from, reset (back to flushed state), trashed (made
// unavailable for queries), or purged (hard deleted).
type ReadSegment interface {
	io.Reader
	Reset() error
	Trash() error
	Purge() error
}

// TrashSegment may only be purged (hard deleted).
type TrashSegment interface {
	Purge() error
}

// LogStats describe the current state of the store log.
type LogStats struct {
	ActiveSegments  int64
	ActiveBytes     int64
	FlushedSegments int64
	FlushedBytes    int64
	ReadingSegments int64
	ReadingBytes    int64
	TrashedSegments int64
	TrashedBytes    int64
}
