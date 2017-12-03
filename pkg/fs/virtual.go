package fs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// NewVirtualFilesystem yields an in-memory filesystem.
func NewVirtualFilesystem() Filesystem {
	return &virtualFilesystem{
		files: map[string]*virtualFile{},
	}
}

type virtualFilesystem struct {
	mtx   sync.RWMutex
	files map[string]*virtualFile
}

func (fs *virtualFilesystem) Create(path string) (File, error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	// os.Create truncates any existing file. So we do, too.
	f := &virtualFile{
		name:  path,
		atime: time.Now(),
		mtime: time.Now(),
	}
	fs.files[path] = f
	return f, nil
}

func (fs *virtualFilesystem) Open(path string) (File, error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	f, ok := fs.files[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return f, nil
}

func (fs *virtualFilesystem) Remove(path string) error {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	if _, ok := fs.files[path]; !ok {
		return os.ErrNotExist
	}
	delete(fs.files, path)
	return nil
}

func (fs *virtualFilesystem) Rename(oldname, newname string) error {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	f, ok := fs.files[oldname]
	if !ok {
		return os.ErrNotExist
	}
	delete(fs.files, oldname)
	fs.files[newname] = f // potentially destructive to newname!
	return nil
}

func (fs *virtualFilesystem) Exists(path string) bool {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	_, ok := fs.files[path]
	return ok
}

func (fs *virtualFilesystem) MkdirAll(path string) error {
	return nil
}

func (fs *virtualFilesystem) Chtimes(path string, atime, mtime time.Time) error {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()
	f, ok := fs.files[path]
	if !ok {
		return os.ErrNotExist
	}
	f.atime, f.mtime = atime, mtime
	return nil
}

func (fs *virtualFilesystem) Walk(root string, walkFn filepath.WalkFunc) error {
	// Snapshot current files.
	files := map[string]*virtualFile{}
	fs.mtx.Lock()
	for path, file := range fs.files {
		files[path] = file
	}
	fs.mtx.Unlock()

	// Walk snapshot, out of lock.
	for path, f := range files {
		if !strings.HasPrefix(path, root) {
			continue // TODO(pb): this heuristic could be better, if necessary
		}
		if err := walkFn(path, virtualFileInfo{
			name:  f.name,
			size:  int64(f.buf.Len()),
			mtime: f.mtime,
		}, nil); err != nil {
			return err
		}
	}
	return nil
}

func (fs *virtualFilesystem) Lock(path string) (r Releaser, existed bool, err error) {
	fs.mtx.Lock()
	defer fs.mtx.Unlock()

	// Simulate locked as nonempty file, so we can test recovery behavior.
	if file, ok := fs.files[path]; ok {
		existed = true
		if file.Size() > 0 {
			return nil, existed, fmt.Errorf("%s already exists and is locked", path)
		}
	}

	// Copy/paste.
	fs.files[path] = &virtualFile{
		name:  path,
		atime: time.Now(),
		mtime: time.Now(),
	}
	fs.files[path].buf.WriteString("locked!")
	return virtualReleaser(func() error { return fs.Remove(path) }), existed, nil
}

type virtualFile struct {
	name  string
	mtx   sync.Mutex
	buf   bytes.Buffer
	atime time.Time
	mtime time.Time
}

func (f *virtualFile) Read(p []byte) (int, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.buf.Read(p)
}

func (f *virtualFile) Write(p []byte) (int, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.buf.Write(p)
}

func (f *virtualFile) Close() error { return nil }
func (f *virtualFile) Name() string { return f.name }

func (f *virtualFile) Size() int64 {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return int64(f.buf.Len())
}

func (f *virtualFile) Sync() error { return nil }

type virtualFileInfo struct {
	name  string
	size  int64
	mtime time.Time
}

func (fi virtualFileInfo) Name() string       { return fi.name }
func (fi virtualFileInfo) Size() int64        { return fi.size }
func (fi virtualFileInfo) Mode() os.FileMode  { return os.FileMode(0666) }
func (fi virtualFileInfo) ModTime() time.Time { return fi.mtime }
func (fi virtualFileInfo) IsDir() bool        { return false }
func (fi virtualFileInfo) Sys() interface{}   { return nil }

type virtualReleaser func() error

func (r virtualReleaser) Release() error { return r() }
