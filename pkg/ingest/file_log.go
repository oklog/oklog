package ingest

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"

	"github.com/oklog/oklog/pkg/fs"
)

const (
	extActive  = ".active"
	extFlushed = ".flushed"
	extPending = ".pending"

	lockFile = "LOCK"
)

// NewFileLog returns a Log implemented via the filesystem.
// All filesystem ops will be rooted at path root.
func NewFileLog(filesys fs.Filesystem, root string) (Log, error) {
	if err := filesys.MkdirAll(root); err != nil {
		return nil, errors.Wrapf(err, "creating path %s", root)
	}
	lock := filepath.Join(root, lockFile)
	r, existed, err := filesys.Lock(lock)
	if err != nil {
		return nil, errors.Wrapf(err, "locking %s", lock)
	}
	if existed {
		// The previous owner crashed, but we still got the lock.
		// So this is like Prometheus "crash recovery" mode.
		// But we don't have anything special we need to do.
	}
	if err := recoverSegments(filesys, root); err != nil {
		return nil, errors.Wrap(err, "during recovery")
	}
	return &fileLog{
		root:     root,
		filesys:  filesys,
		releaser: r,
	}, nil
}

type fileLog struct {
	root     string
	filesys  fs.Filesystem
	releaser fs.Releaser
}

// Create returns a new writable segment.
func (log *fileLog) Create() (WriteSegment, error) {
	filename := filepath.Join(log.root, fmt.Sprintf("%s%s", uuid.New(), extActive))

	f, err := log.filesys.Create(filename)
	if err != nil {
		return nil, err
	}

	return fileWriteSegment{log.filesys, f}, nil
}

// Oldest returns the oldest flushed segment.
func (log *fileLog) Oldest() (ReadSegment, error) {
	var (
		oldest = time.Now()
		chosen string
	)
	log.filesys.Walk(log.root, func(path string, info os.FileInfo, err error) error {
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
	if err := log.filesys.Rename(chosen, newname); err != nil {
		return nil, errors.New("race when fetching oldest; please try again")
	}

	f, err := log.filesys.Open(newname)
	if err != nil {
		if renameErr := log.filesys.Rename(newname, chosen); renameErr != nil {
			panic(renameErr)
		}
		return nil, err
	}

	return fileReadSegment{log.filesys, f}, nil
}

func (log *fileLog) Stats() (LogStats, error) {
	var stats LogStats
	log.filesys.Walk(log.root, func(path string, info os.FileInfo, err error) error {
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

func (log *fileLog) Close() error {
	return log.releaser.Release()
}

func recoverSegments(filesys fs.Filesystem, root string) error {
	var toRename []string
	filesys.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // recurse
		}
		switch filepath.Ext(path) {
		case extActive, extPending:
			toRename = append(toRename, path)
		}
		return nil
	})
	for _, path := range toRename {
		var (
			oldname = path
			newname = modifyExtension(oldname, extFlushed)
		)
		if err := filesys.Rename(oldname, newname); err != nil {
			return err
		}
	}
	return nil
}

type fileWriteSegment struct {
	fs fs.Filesystem
	f  fs.File
}

func (w fileWriteSegment) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

func (w fileWriteSegment) Sync() error {
	return w.f.Sync()
}

// Close closes the segment and makes it available for read.
func (w fileWriteSegment) Close() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	oldname := w.f.Name()
	newname := modifyExtension(oldname, extFlushed)
	return w.fs.Rename(oldname, newname)
}

func (w fileWriteSegment) Delete() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	return w.fs.Remove(w.f.Name())
}

type fileReadSegment struct {
	fs fs.Filesystem
	f  fs.File
}

func (r fileReadSegment) Read(p []byte) (int, error) {
	return r.f.Read(p)
}

// Commit closes and deletes the segment.
func (r fileReadSegment) Commit() error {
	if err := r.f.Close(); err != nil {
		return err
	}
	return r.fs.Remove(r.f.Name())
}

// Failed closes the segment and makes it available again.
func (r fileReadSegment) Failed() error {
	if err := r.f.Close(); err != nil {
		return err
	}
	oldname := r.f.Name()
	newname := modifyExtension(oldname, extFlushed)
	return r.fs.Rename(oldname, newname)
}

func (r fileReadSegment) Size() int64 {
	return r.f.Size()
}

func modifyExtension(filename, newExt string) string {
	return filename[:len(filename)-len(filepath.Ext(filename))] + newExt
}
