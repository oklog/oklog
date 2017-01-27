package store

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/ulid"
)

func TestBatchSegments(t *testing.T) {
	t.Parallel()

	for _, testcase := range []struct {
		name  string
		input []string
		want  [][]string
	}{
		{
			"no elements",
			[]string{},
			[][]string{},
		},
		{
			"one element",
			[]string{"A-Z"},
			[][]string{{"A-Z"}},
		},
		{
			"no overlap",
			[]string{"A-M", "N-Z"},
			[][]string{{"A-M"}, {"N-Z"}},
		},
		{
			"basic overlap",
			[]string{"A-M", "N-R", "O-S", "W-Z"},
			[][]string{{"A-M"}, {"N-R", "O-S"}, {"W-Z"}},
		},
		{
			"complete staggering overlap",
			[]string{"A-M", "L-R", "O-S", "Q-Z"},
			[][]string{{"A-M", "L-R", "O-S", "Q-Z"}},
		},
		{
			"mostly overlap",
			[]string{"A-M", "L-R", "O-S", "W-Z"},
			[][]string{{"A-M", "L-R", "O-S"}, {"W-Z"}},
		},
		{
			"equality should count as overlap",
			[]string{"A-B", "B-C", "C-D", "E-F", "F-G"},
			[][]string{{"A-B", "B-C", "C-D"}, {"E-F", "F-G"}},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			if want, have := testcase.want, batchSegments(testcase.input); !reflect.DeepEqual(want, have) {
				t.Fatalf("want %v, have %v", want, have)
			}
		})
	}
}

func TestMergeReadCloser(t *testing.T) {
	t.Parallel()

	var (
		u100 = ulid.MustNew(100, nil).String()
		u150 = ulid.MustNew(150, nil).String()
		u200 = ulid.MustNew(200, nil).String()
		u250 = ulid.MustNew(250, nil).String()
		u300 = ulid.MustNew(300, nil).String()
		u350 = ulid.MustNew(350, nil).String()
		u400 = ulid.MustNew(400, nil).String()
		u450 = ulid.MustNew(450, nil).String()
		u500 = ulid.MustNew(500, nil).String()
	)
	for _, testcase := range []struct {
		name  string
		input [][]string
		want  []string
	}{
		{
			"no elements",
			[][]string{},
			[]string{},
		},
		{
			"one element",
			[][]string{{u150}},
			[]string{u150},
		},
		{
			"simple merge",
			[][]string{{u100, u200, u300}, {u150, u250, u350}},
			[]string{u100, u150, u200, u250, u300, u350},
		},
		{
			"complex merge",
			[][]string{{u100, u200, u300}, {u250, u350, u450}, {u150, u400, u500}},
			[]string{u100, u150, u200, u250, u300, u350, u400, u450, u500},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			// Convert string slice to a set of readers.
			rcs := make([]io.ReadCloser, len(testcase.input))
			for i, s := range testcase.input {
				segment := strings.Join(s, "\n") + "\n"
				rcs[i] = ioutil.NopCloser(strings.NewReader(segment))
			}

			// Construct the merge reader from the set of readers.
			rc, err := newMergeReadCloser(rcs)
			if err != nil {
				t.Fatal(err)
			}

			// Take lines from the merge reader until EOF.
			have := []string{}
			s := bufio.NewScanner(rc)
			for s.Scan() {
				have = append(have, s.Text())
			}

			// Make sure we got what we want!
			if want := testcase.want; !reflect.DeepEqual(want, have) {
				t.Fatalf("want %v, have %v", want, have)
			}
		})
	}
}

// NOTE(tsenart): Profiling the benchmark with already generated test data
// yields more meaningful and easy to understand results.
//
//   go test -c ./pkg/store
//   ./store.test -test.run=XXX -test.benchmem -test.cpuprofile=out -test.bench=MergeReader
//   go tool pprof -web store.test out
//
func BenchmarkMergeReadCloser(b *testing.B) {
	const size = 32 * 1024 * 1024
	r, err := newMergeReadCloser(generateSegments(b, 128, size, "testdata/segments"))
	if err != nil {
		b.Fatal(err)
	}
	p := make([]byte, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Read(p)
	}
}

func generateSegments(b testing.TB, count, size int, datadir string) (segs []io.ReadCloser) {
	if err := os.MkdirAll(datadir, 0755); err != nil {
		b.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Join(datadir, "*.segment"))
	if err != nil {
		b.Fatal(err)
	}

	for _, segment := range matches {
		f, err := os.Open(segment)
		if err != nil {
			b.Fatal(err)
		}
		segs = append(segs, f)
	}
	if len(segs) > 0 {
		return segs
	}

	ts := uint64(0)
	body := append(bytes.Repeat([]byte("0"), 512), '\n')
	record := make([]byte, ulid.EncodedSize+1+len(body))
	record[ulid.EncodedSize] = ' '
	copy(record[ulid.EncodedSize+1:], body)

	buf := bytes.NewBuffer(make([]byte, 0, size))
	for i := 0; i < count; i++ {
		from := ulid.MustNew(ts, nil)
		hi := ts
		for buf.Len() < size {
			ulid.MustNew(hi, nil).MarshalTextTo(record[:ulid.EncodedSize])
			_, err := buf.Write(record)
			if err != nil {
				b.Fatal(err)
			}
			hi++
		}

		to := ulid.MustNew(hi, nil)
		name := filepath.Join(datadir, fmt.Sprintf("%s-%s.segment", from, to))
		f, err := os.Create(name)
		if err != nil {
			b.Fatal(err)
		}

		if _, err = buf.WriteTo(f); err != nil {
			b.Fatal(err)
		}

		buf.Reset()
		segs = append(segs, f)
		ts = hi
	}

	return segs
}

func TestConcurrentFilteringReadCloser(t *testing.T) {
	t.Parallel()

	var input bytes.Buffer
	for i := 0; i < 5; i++ {
		id := ulid.MustNew(uint64(i), nil)
		fmt.Fprintln(&input, id, strconv.Itoa(i))
	}

	// Takes records from the filtering reader until EOF.
	records := func(r io.Reader) []string {
		have := []string{}
		s := bufio.NewScanner(r)
		for s.Scan() {
			have = append(have, strings.Fields(s.Text())[1])
		}
		return have
	}

	for _, testcase := range []struct {
		name     string
		from, to ulid.ULID
		q        string
		want     []string
	}{
		{
			name: "all",
			from: ulid.MustNew(0, nil),
			to:   ulid.MustNew(ulid.MaxTime(), nil),
			want: []string{"0", "1", "2", "3", "4"},
		},
		{
			name: "some",
			from: ulid.MustNew(1, nil),
			to:   ulid.MustNew(3, nil),
			want: []string{"1", "2", "3"},
		},
		{
			name: "some matching query",
			from: ulid.MustNew(1, nil),
			to:   ulid.MustNew(3, nil),
			q:    "1|3",
			want: []string{"1", "3"},
		},
		{
			name: "none",
			from: ulid.MustNew(0, nil),
			to:   ulid.MustNew(ulid.MaxTime(), nil),
			q:    "6",
			want: []string{},
		},
		{
			name: "query doesn't match record ids",
			from: ulid.MustNew(0, nil),
			to:   ulid.MustNew(ulid.MaxTime(), nil),
			q:    ulid.MustNew(0, nil).String(),
			want: []string{},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			in := bytes.NewReader(input.Bytes())
			re := regexp.MustCompile(testcase.q)
			pass := recordFilterBoundedRegex(testcase.from, testcase.to, re)
			rc := newConcurrentFilteringReadCloser(ioutil.NopCloser(in), pass, 1024)
			if want, have := testcase.want, records(rc); !reflect.DeepEqual(want, have) {
				t.Errorf("want %v, have %v", want, have)
			}
		})
	}
}

func TestIssue23(t *testing.T) {
	var (
		readerBufSize   = 4096 // bufio.go defaultBufSz
		bigRecordLength = readerBufSize * 2
		bigRecord       = fmt.Sprintf("%s %s", "01B7FTVBVH4CVXC39RBF486QKN", bytes.Repeat([]byte{'A'}, bigRecordLength))
		input           = fmt.Sprintf("%s\n%s\n%s\n", "01B7FTVBVHZK5GCKXK65JSY4CA Alpha", bigRecord, "01B7FTVBVHYDQC5JDEKC76J0QT Beta")
		src             = ioutil.NopCloser(strings.NewReader(input))
		pass            = func([]byte) bool { return true }
		pipeBufSz       = 1024 * 1024 // different than bufio.Reader bufsz
		rc              = newConcurrentFilteringReadCloser(src, pass, int64(pipeBufSz))
	)
	output, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if want, have := input, string(output); want != have {
		t.Errorf("want %d bytes, have %d bytes", len(want), len(have))
	}
}

func TestIssue41(t *testing.T) {
	var (
		filesys  = fs.NewVirtualFilesystem()
		filename = "extant"
		segments = []string{filename, "nonexistant"}
		pass     = func([]byte) bool { return true }
		bufsz    = int64(1024)
	)

	f, err := filesys.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Should error but not panic.
	if _, _, err = makeConcurrentFilteringReadClosers(filesys, segments, pass, bufsz); err == nil {
		t.Errorf("expected error, got none")
	}
}
