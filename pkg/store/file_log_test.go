package store

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/oklog/oklog/pkg/fs"
)

func TestChooseFirstSequential(t *testing.T) {
	t.Parallel()

	const targetSize = 100 * 1024 // 100KB
	for _, testcase := range []struct {
		name    string
		input   []segmentInfo
		minimum int
		want    []string
	}{
		{
			name: "catch all",
			input: []segmentInfo{
				{lowID: "100", path: "A", size: 123},
				{lowID: "200", path: "B", size: 123},
				{lowID: "300", path: "C", size: 123},
				{lowID: "400", path: "D", size: 123},
				{lowID: "500", path: "E", size: 123},
			},
			minimum: 2,
			want:    []string{"A", "B", "C", "D", "E"},
		},
		{
			name: "size limit",
			input: []segmentInfo{
				{lowID: "100", path: "A", size: 100000},
				{lowID: "200", path: "B", size: 2000},
				{lowID: "300", path: "C", size: 2000},
				{lowID: "400", path: "D", size: 2000},
			},
			minimum: 2,
			want:    []string{"A", "B"},
		},
		{
			name: "size limit initial skip",
			input: []segmentInfo{
				{lowID: "100", path: "A", size: 118401},
				{lowID: "200", path: "B", size: 623},
				{lowID: "300", path: "C", size: 1000},
			},
			minimum: 2,
			want:    []string{"B", "C"},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			have := chooseFirstSequential(testcase.input, testcase.minimum, targetSize)
			if want := testcase.want; !reflect.DeepEqual(want, have) {
				t.Fatalf("want %v, have %v", want, have)
			}
		})
	}
}

func TestRecoverSegments(t *testing.T) {
	t.Parallel()

	filesys := fs.NewVirtualFilesystem()
	for _, filename := range []string{
		"ACTIVE." + extActive,
		"FLUSHED." + extFlushed,
		"READING." + extReading,
		"TRASHED." + extTrashed,
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

	const (
		segmentTargetSize = 10 * 1024
		segmentBufferSize = 1024
	)
	filelog, err := NewFileLog(filesys, "", segmentTargetSize, segmentBufferSize)
	if err != nil {
		t.Fatalf("NewFileLog: %v", err)
	}
	defer filelog.Close()

	files := map[string]bool{
		// file                  expected
		lockFile:                true,
		"ACTIVE." + extFlushed:  true,
		"FLUSHED." + extFlushed: true,
		"READING." + extFlushed: true,
		"TRASHED." + extTrashed: true,
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
		"real":    func() fs.Filesystem { return fs.NewRealFilesystem(false) },
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
			filelog, err := NewFileLog(filesys, root, 1024, 1024)
			if err != nil {
				t.Fatalf("initial NewFileLog: %v", err)
			}

			// But a second FileLog should fail.
			if _, err := NewFileLog(filesys, root, 1024, 1024); err == nil {
				t.Fatalf("second NewFileLog: want error, have none")
			} else {
				t.Logf("second NewFileLog: got expected error: %v", err)
			}
			filelog.Close()
		})
	}
}
