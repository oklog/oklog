package fs

import (
	"path/filepath"
	"time"
)

type nopFilesystem struct{}

// NewNopFilesystem has methods that always succeed but do nothing.
func NewNopFilesystem() Filesystem {
	return nopFilesystem{}
}

func (nopFilesystem) Create(path string) (File, error)                  { return nopFile{}, nil }
func (nopFilesystem) Open(path string) (File, error)                    { return nopFile{}, nil }
func (nopFilesystem) Remove(path string) error                          { return nil }
func (nopFilesystem) Rename(oldname, newname string) error              { return nil }
func (nopFilesystem) Exists(path string) bool                           { return false }
func (nopFilesystem) MkdirAll(path string) error                        { return nil }
func (nopFilesystem) Chtimes(path string, atime, mtime time.Time) error { return nil }
func (nopFilesystem) Walk(root string, walkFn filepath.WalkFunc) error  { return nil }
func (nopFilesystem) Lock(path string) (Releaser, bool, error)          { return nopReleaser{}, false, nil }

type nopFile struct{}

func (nopFile) Read(p []byte) (int, error)  { return len(p), nil }
func (nopFile) Write(p []byte) (int, error) { return len(p), nil }
func (nopFile) Close() error                { return nil }
func (nopFile) Name() string                { return "" }
func (nopFile) Size() int64                 { return 0 }
func (nopFile) Sync() error                 { return nil }

type nopReleaser struct{}

func (nopReleaser) Release() error { return nil }
