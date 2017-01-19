package ingest

import (
	"os"
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
		"LOCK":                  true,
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
