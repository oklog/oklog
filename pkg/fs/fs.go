// Package fs abstracts the filesystem.
package fs

import (
	"io"
	"path/filepath"
	"time"
)

// Filesystem collects the operations we need from a filesystem.
type Filesystem interface {
	Create(path string) (File, error)
	Open(path string) (File, error)
	Remove(path string) error
	Rename(oldname, newname string) error
	Exists(path string) bool
	MkdirAll(path string) error
	Chtimes(path string, atime, mtime time.Time) error
	Walk(root string, walkFn filepath.WalkFunc) error
	Lock(path string) (r Releaser, existed bool, err error)
}

// File is the subset of methods we use on an *os.File.
type File interface {
	io.Reader
	io.Writer
	io.Closer
	Name() string
	Size() int64
	Sync() error
}

// Releaser is returned by Lock calls.
type Releaser interface {
	Release() error
}
