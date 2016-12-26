package fs

import (
	"os"
	"path/filepath"
	"time"
)

const mkdirAllMode = 0755

// NewRealFilesystem yields a real disk filesystem.
func NewRealFilesystem() Filesystem {
	return realFilesystem{}
}

type realFilesystem struct{}

func (realFilesystem) Create(path string) (File, error) {
	f, err := os.Create(path)
	return realFile{f}, err
}

func (realFilesystem) Open(path string) (File, error) {
	f, err := os.Open(path)
	return realFile{f}, err
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

type realFile struct{ *os.File }

func (f realFile) Size() int64 {
	fi, err := f.File.Stat()
	if err != nil {
		panic(err)
	}
	return fi.Size()
}
