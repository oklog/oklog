package fs

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/oklog/prototype/pkg/ioext"
	"github.com/oklog/prototype/pkg/mmap"
)

const mkdirAllMode = 0755

// NewRealFilesystem yields a real disk filesystem with optional memory
// mapping for file reading.
func NewRealFilesystem(mmap bool) Filesystem {
	return realFilesystem{mmap: mmap}
}

type realFilesystem struct {
	mmap bool
}

func (realFilesystem) Create(path string) (File, error) {
	f, err := os.Create(path)
	return &realFile{File: f}, err
}

func (fs realFilesystem) Open(path string) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	rf := &realFile{File: f}
	if fs.mmap {
		r, err := mmap.New(f)
		if err != nil {
			return nil, err
		}
		rf.r = ioext.OffsetReader(r, 0)
	}

	return rf, nil
}

func (realFilesystem) Remove(path string) error {
	return os.Remove(path)
}

func (realFilesystem) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (realFilesystem) Exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func (realFilesystem) MkdirAll(path string) error {
	return os.MkdirAll(path, mkdirAllMode)
}

func (realFilesystem) Chtimes(path string, atime, mtime time.Time) error {
	return os.Chtimes(path, atime, mtime)
}

func (realFilesystem) Walk(root string, walkFn filepath.WalkFunc) error {
	return filepath.Walk(root, walkFn)
}

type realFile struct {
	r io.Reader
	*os.File
}

func (f *realFile) Read(p []byte) (int, error) {
	if f.r != nil {
		return f.r.Read(p)
	}
	return f.File.Read(p)
}

func (f realFile) Size() int64 {
	fi, err := f.File.Stat()
	if err != nil {
		panic(err)
	}
	return fi.Size()
}
