package ingest

import (
	"errors"
	"io"
)

// Log is an abstraction for segments on an ingest node.
// A new active segment may be created and written to.
// The oldest flushed segment may be selected and read from.
type Log interface {
	Create() (WriteSegment, error)
	Oldest() (ReadSegment, error)
	Stats() (LogStats, error)
	Close() error
}

// WriteSegment is a segment that can be written to.
// It may be optionally synced to disk manually.
// When writing is complete, it may be closed and flushed.
// If it would be closed with size 0, it may be deleted instead.
type WriteSegment interface {
	io.Writer
	Sync() error
	Close() error
	Delete() error
}

// ReadSegment is a segment that can be read from.
// Once read, it may be committed and thus deleted.
// Or it may be failed, and made available for selection again.
type ReadSegment interface {
	io.Reader
	Commit() error
	Failed() error
	Size() int64
}

// ErrNoSegmentsAvailable is returned by IngestLog Oldest,
// when no segments are available for reading.
var ErrNoSegmentsAvailable = errors.New("no segments available")

// LogStats describe the current state of the ingest log.
type LogStats struct {
	ActiveSegments  int64
	ActiveBytes     int64
	FlushedSegments int64
	FlushedBytes    int64
	PendingSegments int64
	PendingBytes    int64
}
