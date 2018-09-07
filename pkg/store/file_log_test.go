package store

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/oklog/ulid"

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
	for filename, contents := range map[string]string{
		"ACTIVE" + extActive:   "01ARYZ6S41TSV4RRFFQ69G5FAV One\n01ARYZ6S41TSV4RRFFZZZZZZZZ Two\n",
		"FLUSHED" + extFlushed: "Contents ignoredt",
		"READING" + extReading: "Contents ignored",
		"TRASHED" + extTrashed: "Contents ignored",
		"IGNORED.ignored":      "Contents ignored",
	} {
		f, err := filesys.Create(filename)
		if err != nil {
			t.Fatalf("%s: Create: %v", filename, err)
		}
		if _, err := f.Write([]byte(contents)); err != nil {
			t.Fatalf("%s: Write: %v", filename, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("%s: Close: %v", filename, err)
		}
	}

	const (
		segmentTargetSize = 10 * 1024
		segmentBufferSize = 1024
	)
	filelog, err := NewFileLog(filesys, "", segmentTargetSize, segmentBufferSize, "", nil)
	if err != nil {
		t.Fatalf("NewFileLog: %v", err)
	}
	defer filelog.Close()

	files := map[string]bool{ // file: expected
		"01ARYZ6S41TSV4RRFFQ69G5FAV-01ARYZ6S41TSV4RRFFZZZZZZZZ" + extFlushed: true,
		"FLUSHED" + extFlushed:                                               true,
		"READING" + extFlushed:                                               true,
		"TRASHED" + extTrashed:                                               true,
		"IGNORED.ignored":                                                    true,
		lockFile:                                                             true,
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
			filelog, err := NewFileLog(filesys, root, 1024, 1024, "", nil)
			if err != nil {
				t.Fatalf("initial NewFileLog: %v", err)
			}

			// But a second FileLog should fail.
			if _, err := NewFileLog(filesys, root, 1024, 1024, "", nil); err == nil {
				t.Fatalf("second NewFileLog: want error, have none")
			} else {
				t.Logf("second NewFileLog: got expected error: %v", err)
			}
			filelog.Close()
		})
	}
}

func TestBadSegment(t *testing.T) {
	t.Parallel()

	// Create some filenames.
	var (
		t0        = time.Now().Add(-time.Minute)
		entropy   = rand.New(rand.NewSource(7))
		mktime    = func(i int) time.Time { return t0.Add(time.Duration(i) * time.Second) }
		mkulid    = func(i int) ulid.ULID { return ulid.MustNew(ulid.Timestamp(mktime(i)), entropy) }
		mkulidstr = func(i int) string { return mkulid(i).String() }
		goodfile1 = fmt.Sprintf("%s-%s%s", mkulidstr(0), mkulidstr(3), extFlushed)
		goodfile2 = fmt.Sprintf("%s-%s%s", mkulidstr(1), mkulidstr(5), extFlushed)
		badfile   = fmt.Sprintf("%s-%s%s", mkulidstr(2), "xxx", extFlushed)
	)

	// Populate a filesys with those files.
	filesys := fs.NewVirtualFilesystem()
	for _, filename := range []string{goodfile1, goodfile2, badfile} {
		f, _ := filesys.Create("/" + filename)
		f.Close()
	}

	// Create a filelog around that filesys.
	filelog, _ := NewFileLog(filesys, "/", 1024, 1024, "", nil)

	// Perform some read op on the filelog, to trigger rm.
	// Main thing here is just that it doesn't panic.
	filelog.Overlapping()

	// Hopefully we've eliminated the bad file.
	// TODO(pb): in the future, write some valid entries and test they're recovered somewhere
	found := map[string]bool{}
	filesys.Walk("/", func(path string, info os.FileInfo, err error) error {
		found[filepath.Base(path)] = true
		return nil
	})
	if _, ok := found[badfile]; ok {
		t.Fatalf("%s wasn't deleted", badfile)
	}
}
