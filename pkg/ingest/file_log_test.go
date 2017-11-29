package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/oklog/oklog/pkg/fs"
)

func TestRecoverSegments(t *testing.T) {
	t.Parallel()

	filesys := fs.NewVirtualFilesystem()
	for _, filename := range []string{
		"ACTIVE." + extActive,
		"PENDING." + extPending,
		"FLUSHED." + extFlushed,
		"IGNORED.ignored",
	} {
		f, err := filesys.Create(filename)
		if err != nil {
			t.Fatalf("%s: Create: %v", filename, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("%s: Close: %v", filename, err)
		}
	}

	filelog, err := NewFileLog(filesys, "")
	if err != nil {
		t.Fatalf("NewFileLog: %v", err)
	}
	defer filelog.Close()

	files := map[string]bool{
		// file                  expected
		lockFile:                true,
		"ACTIVE." + extFlushed:  true,
		"PENDING." + extFlushed: true,
		"FLUSHED." + extFlushed: true,
		"IGNORED.ignored":       true,
	}
	filesys.Walk("", func(path string, info os.FileInfo, err error) error {
		if _, ok := files[path]; ok {
			delete(files, path) // found expected file, great
		} else {
			files[path] = false // found unexpected file, oh no
		}
		return nil
	})

	for file, expected := range files {
		if expected {
			t.Errorf("didn't find expected file %s", file)
		} else {
			t.Errorf("found unexpected file %s", file)
		}
	}
}

func TestLockBehavior(t *testing.T) {
	t.Parallel()

	root := filepath.Join("testdata", "TestStaleLockSucceeds")
	for name, factory := range map[string]func() fs.Filesystem{
		"virtual": fs.NewVirtualFilesystem,
		"real":    fs.NewRealFilesystem,
	} {
		t.Run(name, func(t *testing.T) {
			// Generate a filesystem and rootpath.
			filesys := factory()
			if err := filesys.MkdirAll(root); err != nil {
				t.Fatalf("MkdirAll(%s): %v", root, err)
			}
			defer filesys.Remove(root)

			// Create a (stale) lock file.
			f, err := filesys.Create(filepath.Join(root, lockFile))
			if err != nil {
				t.Fatalf("Create(%s): %v", lockFile, err)
			}
			f.Close()

			// NewFileLog should manage this fine.
			filelog, err := NewFileLog(filesys, root)
			if err != nil {
				t.Fatalf("initial NewFileLog: %v", err)
			}

			// But a second FileLog should fail.
			if _, err := NewFileLog(filesys, root); err == nil {
				t.Fatalf("second NewFileLog: want error, have none")
			} else {
				t.Logf("second NewFileLog: got expected error: %v", err)
			}
			filelog.Close()
		})
	}
}
