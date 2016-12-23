package ingest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pborman/uuid"
)

// NewFileLog returns a Log backed by the filesystem at path root.
// TODO(pb): abstract the filesystem to make this unit-testable.
func NewFileLog(root string) (Log, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	return &fileLog{
		root: root,
	}, nil
}

type fileLog struct {
	root string
}

const (
	extActive  = ".active"
	extFlushed = ".flushed"
	extPending = ".pending"
)

// Create returns a new writable segment.
func (log *fileLog) Create() (WriteSegment, error) {
	filename := filepath.Join(log.root, fmt.Sprintf("%s%s", uuid.New(), extActive))

	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return fileWriteSegment{f}, nil
}

// Oldest returns the oldest flushed segment.
func (log *fileLog) Oldest() (ReadSegment, error) {
	var (
		oldest = time.Now()
		chosen string
	)
	filepath.Walk(log.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // recurse
		}
		if filepath.Ext(path) != extFlushed {
			return nil // skip
		}
		if t := info.ModTime(); t.Before(oldest) {
			chosen, oldest = path, t
		}
		return nil
	})
	if chosen == "" {
		return nil, ErrNoSegmentsAvailable
	}

	// This can be racy. But if the rename fails, no problem.
	// Someone else got it; our client can just try again.
	newname := modifyExtension(chosen, extPending)
	if err := os.Rename(chosen, newname); err != nil {
		return nil, errors.New("race when fetching oldest; please try again")
	}

	f, err := os.Open(newname)
	if err != nil {
		if err := os.Rename(newname, chosen); err != nil {
			panic(err)
		}
		return nil, err
	}

	return fileReadSegment{f}, nil
}

func (log *fileLog) Stats() (LogStats, error) {
	var stats LogStats
	filepath.Walk(log.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // recurse
		}
		switch filepath.Ext(path) {
		case extActive:
			stats.ActiveSegments++
			stats.ActiveBytes += info.Size()
		case extFlushed:
			stats.FlushedSegments++
			stats.FlushedBytes += info.Size()
		case extPending:
			stats.PendingSegments++
			stats.PendingBytes += info.Size()
		}
		return nil
	})
	return stats, nil
}

type fileWriteSegment struct {
	*os.File
}

// Close closes the segment and makes it available for read.
func (w fileWriteSegment) Close() error {
	if err := w.File.Close(); err != nil {
		return err
	}
	oldname := w.File.Name()
	newname := modifyExtension(oldname, extFlushed)
	return os.Rename(oldname, newname)
}

func (w fileWriteSegment) Delete() error {
	if err := w.File.Close(); err != nil {
		return err
	}
	return os.Remove(w.File.Name())
}

type fileReadSegment struct {
	*os.File
}

// Commit closes and deletes the segment.
func (r fileReadSegment) Commit() error {
	if err := r.File.Close(); err != nil {
		return err
	}
	return os.Remove(r.File.Name())
}

// Failed closes the segment and makes it available again.
func (r fileReadSegment) Failed() error {
	if err := r.File.Close(); err != nil {
		return err
	}
	oldname := r.File.Name()
	newname := modifyExtension(oldname, extFlushed)
	return os.Rename(oldname, newname)
}

func modifyExtension(filename, newExt string) string {
	return filename[:len(filename)-len(filepath.Ext(filename))] + newExt
}
