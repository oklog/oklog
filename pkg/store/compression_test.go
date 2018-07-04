package store

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"compress/gzip"
	"github.com/oklog/ulid"
	"github.com/valyala/gozstd"

	"github.com/oklog/oklog/pkg/fs"
)

type byteReader struct {
	b byte
}

func (r byteReader) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i++ {
		p[i] = r.b
	}

	return len(p), nil
}

// Check, that file log writes compressed records to proper file
// and compresses data correctly
func checkCompressionWrite(t *testing.T, compression, ext string) {
	t.Parallel()

	const (
		segmentTargetSize = 10 * 1024
		segmentBufferSize = 1024
	)

	filesys := fs.NewVirtualFilesystem()

	filelog, err := NewFileLog(filesys, "", segmentTargetSize, segmentBufferSize, compression, nil)
	if err != nil {
		t.Fatalf("NewFileLog: %v", err)
	}
	defer filelog.Close()

	ws, err := filelog.Create()

	var lo, hi *ulid.ULID
	r := byteReader{'x'}
	var data bytes.Buffer
	w := io.MultiWriter(ws, &data)
	for i := 0; i < 100; i++ {
		ul := ulid.MustNew(uint64(i), r)
		if lo == nil {
			lo = &ul
		}
		hi = &ul

		s := fmt.Sprintf("%s %s\n", ul, strings.Repeat("test ", 50))
		w.Write([]byte(s))
	}

	ws.Close(*lo, *hi)

	segmentName := fmt.Sprintf("%s-%s%s%s", lo, hi, extFlushed, ext)
	/* there should 2 2 files: LOCK and our segment */
	files := []string{}
	filesys.Walk("", func(path string, info os.FileInfo, err error) error {
		switch path {
		case "LOCK":
		case segmentName:
			if info.Size() > int64(data.Len()/2) {
				t.Errorf("File seems to be not compressed, uncompressed size=%d, compressed=%d", data.Len(), info.Size())
			}
		default:
			t.Errorf("Unknown file in dir: %s", path)
		}
		files = append(files, path)
		return nil
	})

	if len(files) != 2 {
		t.Errorf("Some files are missing, want 'LOCK' and segment, have: %v", files)
	}

	// Check file content
	cf, err := filesys.Open(segmentName)
	if err != nil {
		t.Fatalf("Error opening %s: %v", segmentName, err)
	}

	var f io.Reader

	switch compression {
	case "zstd":
		f = gozstd.NewReader(cf)
	case "gzip":
		f, err = gzip.NewReader(cf)
		if err != nil {
			t.Fatalf("Error creating gzip input stream: %v", err)
		}
	}

	data2, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("Error reading from %s: %v", segmentName, err)
	}

	if !bytes.Equal(data.Bytes(), data2) {
		t.Errorf("Data in segment file is wrong")
	}
}

func TestCompressionWrite(t *testing.T) {
	t.Run("zstd", func(t *testing.T) {
		checkCompressionWrite(t, "zstd", ".zst")
	})

	t.Run("gzip", func(t *testing.T) {
		checkCompressionWrite(t, "gzip", ".gz")
	})
}

// Check that fileReadSegment correctly decompresses log files
// Create compressed file without help of tested functions then
// open and read it with fileReadSegment and compare data
func checkCompressionRead(t *testing.T, compression, ext string) {
	t.Parallel()

	const (
		segmentTargetSize = 10 * 1024
		segmentBufferSize = 1024
	)

	filesys := fs.NewVirtualFilesystem()

	path := fmt.Sprintf("test-file%s%s", extFlushed, ext)
	cf, err := filesys.Create(path)
	if err != nil {
		t.Fatalf("Error creating file %s: %v", cf, err)
	}

	var f io.WriteCloser

	switch compression {
	case "zstd":
		tmp := gozstd.NewWriter(cf)
		defer tmp.Release()
		f = tmp
	case "gzip":
		f = gzip.NewWriter(cf)
	}

	data := []byte(strings.Repeat("some text ", 30) + strings.Repeat("some other text ", 50))

	_, err = f.Write(data)
	f.Close()
	cf.Close()

	r, err := newFileReadSegment(filesys, path)
	if err != nil {
		t.Fatalf("Error opening file %s: %v", path, err)
	}

	data2, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("Error reading: %v", err)
	}

	if !bytes.Equal(data, data2) {
		t.Errorf("Invalid data was produced by fileReadSegment")
	}
}

func TestCompressionRead(t *testing.T) {
	t.Run("zstd", func(t *testing.T) {
		checkCompressionRead(t, "zstd", ".zst")
	})

	t.Run("gzip", func(t *testing.T) {
		checkCompressionRead(t, "gzip", ".gz")
	})
}
