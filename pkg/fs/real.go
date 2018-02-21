package fs

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oklog/oklog/pkg/flock"
)

const mkdirAllMode = 0755

// NewRealFilesystem yields a real disk filesystem
// with optional memory mapping for file reading.
func NewRealFilesystem() Filesystem {
	return realFilesystem{}
}

type realFilesystem struct{}

func (realFilesystem) Create(path string) (File, error) {
	f, err := os.Create(path)
	return realFile{
		File:   f,
		Reader: f,
		Closer: f,
	}, err
}

func (fs realFilesystem) Open(path string) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return realFile{
		File:   f,
		Reader: f,
		Closer: f,
	}, nil
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

func (realFilesystem) Lock(path string) (r Releaser, existed bool, err error) {
	r, existed, err = flock.New(path)
	r = deletingReleaser{path, r}
	return r, existed, err
}

type deletingReleaser struct {
	path string
	r    Releaser
}

func (dr deletingReleaser) Release() error {
	// Remove before Release should be safe, and prevents a race.
	if err := os.Remove(dr.path); err != nil {
		return err
	}
	return dr.r.Release()
}

type realFile struct {
	*os.File
	io.Reader
	io.Closer
}

func (f realFile) Read(p []byte) (int, error) {
	return f.Reader.Read(p)
}

func (f realFile) Close() error {
	return f.Closer.Close()
}

func (f realFile) Size() int64 {
	fi, err := f.File.Stat()
	if err != nil {
		panic(err)
	}
	return fi.Size()
}

// multiCloser closes all underlying io.Closers.
// If an error is encountered, closings continue.
type multiCloser []io.Closer

func (c multiCloser) Close() error {
	var errs []error
	for _, closer := range c {
		if closer == nil {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return multiCloseError(errs)
	}
	return nil
}

type multiCloseError []error

func (e multiCloseError) Error() string {
	a := make([]string, len(e))
	for i, err := range e {
		a[i] = err.Error()
	}
	return strings.Join(a, "; ")
}
