package store

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/ulid"
)

func TestMergeRecords(t *testing.T) {
	t.Parallel()

	var (
		u100 = ulid.MustNew(100, nil).String()
		u101 = ulid.MustNew(101, nil).String()
		u149 = ulid.MustNew(149, nil).String()
		u150 = ulid.MustNew(150, nil).String()
		u151 = ulid.MustNew(151, nil).String()
		u152 = ulid.MustNew(152, nil).String()
		u200 = ulid.MustNew(200, nil).String()
		u201 = ulid.MustNew(201, nil).String()
		u300 = ulid.MustNew(300, nil).String()
		u301 = ulid.MustNew(301, nil).String()
		u302 = ulid.MustNew(302, nil).String()
	)
	for i, testcase := range []struct {
		input [][]string
		want  []string
	}{
		{
			input: nil,
			want:  nil,
		},
		{
			input: [][]string{},
			want:  []string{},
		},
		{
			input: [][]string{
				{u100 + " Foo", u200 + " Bar", u300 + " Baz"},
			},
			want: []string{u100 + " Foo", u200 + " Bar", u300 + " Baz"},
		},
		{
			input: [][]string{
				{u100 + " Foo", u200 + " Bar", u300 + " Baz"},
				{u101 + " Foo", u201 + " Bar", u301 + " Baz"},
			},
			want: []string{u100 + " Foo", u101 + " Foo", u200 + " Bar", u201 + " Bar", u300 + " Baz", u301 + " Baz"},
		},
		{
			input: [][]string{
				{u100, u200, u300},
				{u150, u151, u152},
				{u101, u301, u302},
			},
			want: []string{u100, u101, u150, u151, u152, u200, u300, u301, u302},
		},
		{
			input: [][]string{
				{u100},
				{u150 + " A", u151},
				{u101, u149, u150 + " B"},
			},
			want: []string{u100, u101, u149, u150 + " A", u151}, // dedupe
		},
		{
			input: [][]string{
				{u100 + " A", u101 + " A"},
				{u101 + " B", u149 + " B"},
				{u149 + " C", u150 + " C"},
			},
			want: []string{u100 + " A", u101 + " A", u149 + " B", u150 + " C"}, // more dedupe
		},
	} {
		readers := make([]io.Reader, len(testcase.input))
		for i, slice := range testcase.input {
			str := strings.Join(slice, "\n")
			if str != "" {
				str += "\n"
			}
			readers[i] = strings.NewReader(str)
		}

		var buf bytes.Buffer
		if _, _, _, err := mergeRecords(&buf, readers...); err != nil {
			t.Error(err)
			continue
		}

		wantStr := strings.Join(testcase.want, "\n")
		if wantStr != "" {
			wantStr += "\n"
		}
		if want, have := wantStr, buf.String(); want != have {
			t.Errorf("%d: want %v, have %v", i, testcase.want, strings.FieldsFunc(have, func(r rune) bool { return r == '\n' }))
		}
	}
}

func TestMergeRecordsToLog(t *testing.T) {
	t.Parallel()

	records := []string{
		0:  fmt.Sprintf("%s A\n", ulid.MustNew(100, nil).String()),
		1:  fmt.Sprintf("%s B\n", ulid.MustNew(101, nil).String()),
		2:  fmt.Sprintf("%s C\n", ulid.MustNew(102, nil).String()),
		3:  fmt.Sprintf("%s D\n", ulid.MustNew(103, nil).String()),
		4:  fmt.Sprintf("%s E\n", ulid.MustNew(104, nil).String()),
		5:  fmt.Sprintf("%s F\n", ulid.MustNew(105, nil).String()),
		6:  fmt.Sprintf("%s G\n", ulid.MustNew(106, nil).String()),
		7:  fmt.Sprintf("%s H\n", ulid.MustNew(107, nil).String()),
		8:  fmt.Sprintf("%s I\n", ulid.MustNew(108, nil).String()),
		9:  fmt.Sprintf("%s J\n", ulid.MustNew(109, nil).String()),
		10: fmt.Sprintf("%s K\n", ulid.MustNew(110, nil).String()),
		11: fmt.Sprintf("%s L\n", ulid.MustNew(111, nil).String()),
	}
	var (
		dst               = &mockLog{&bytes.Buffer{}}
		segmentTargetSize = int64(100000)
		readers           = []io.Reader{
			strings.NewReader(records[0] + records[3] + records[10]),
			strings.NewReader(records[2] + records[4] + records[5] + records[11]),
			strings.NewReader(records[1] + records[6] + records[7] + records[8]),
			strings.NewReader(records[9]),
		}
	)

	n, err := mergeRecordsToLog(dst, segmentTargetSize, readers...)
	if err != nil {
		t.Error(err)
	}

	var totalSize int
	for _, s := range records {
		totalSize += len(s)
	}
	if want, have := int64(totalSize), n; want != have {
		t.Errorf("n: want %d, have %d", want, have)
	}

	if want, have := strings.Join(records, ""), dst.Buffer.String(); want != have {
		t.Errorf("Result: want %q, have %q", want, have)
	}
}

func BenchmarkMergeRecordsToLog(b *testing.B) {
	// Need large number of big (1k) records to get the performance problems
	// with bytes.FieldsFunc to really show themselves. If you drop these
	// numbers too low, you don't see much of a difference.
	const (
		readerCount       = 10
		recordCount       = 10000
		recordSize        = 1024
		segmentTargetSize = recordCount * recordSize * (readerCount / 5)
		segmentBufferSize = 1024
		charset           = "0123456789ABCDEFGHJKMNPQRSTVWXYZ "
	)

	dst, err := NewFileLog(fs.NewNopFilesystem(), "/", segmentTargetSize, segmentBufferSize, nil)
	if err != nil {
		b.Fatal(err)
	}

	startStamp := ulid.Now()
	rand.Seed(12345678)
	populate := func(w io.Writer) {
		stamp := startStamp
		for i := 0; i < recordCount; i++ {
			stamp += uint64(rand.Int63n(10))
			id := ulid.MustNew(stamp, nil) // ascending records plz
			record := make([]rune, recordSize-1-ulid.EncodedSize-1)
			for j := range record {
				record[j] = rune(charset[rand.Intn(len(charset))])
			}
			fmt.Fprintf(w, "%s %s\n", id, string(record))
		}
	}

	readers := make([]io.Reader, readerCount)
	for i := 0; i < readerCount; i++ {
		var buf bytes.Buffer
		populate(&buf)
		readers[i] = &buf
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n, err := mergeRecordsToLog(dst, segmentTargetSize, readers...); err != nil {
			b.Errorf("n=%d err=%v", n, err)
		}
	}
}

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
			// Transform strings to nop readSegments.
			segments := make([]readSegment, len(testcase.input))
			for i, path := range testcase.input {
				segments[i] = readSegment{path: path, file: nopReadCloser{}}
			}

			// Batch them.
			result := batchSegments(segments)

			// Extract ranges.
			have := make([][]string, len(result))
			for i, segments := range result {
				have[i] = make([]string, len(segments))
				for j, segment := range segments {
					have[i][j] = segment.path
				}
			}

			// Check.
			if want, have := testcase.want, have; !reflect.DeepEqual(want, have) {
				t.Fatalf("want %v, have %v", want, have)
			}
		})
	}
}

type nopReadCloser struct{}

func (nopReadCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (nopReadCloser) Close() error             { return nil }

func TestMergeReadCloser(t *testing.T) {
	t.Parallel()

	var (
		u100 = ulid.MustNew(100, nil).String() + "\n"
		u150 = ulid.MustNew(150, nil).String() + "\n"
		u200 = ulid.MustNew(200, nil).String() + "\n"
		u250 = ulid.MustNew(250, nil).String() + "\n"
		u300 = ulid.MustNew(300, nil).String() + "\n"
		u350 = ulid.MustNew(350, nil).String() + "\n"
		u400 = ulid.MustNew(400, nil).String() + "\n"
		u450 = ulid.MustNew(450, nil).String() + "\n"
		u500 = ulid.MustNew(500, nil).String() + "\n"
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
			for i, segment := range testcase.input {
				rcs[i] = ioutil.NopCloser(strings.NewReader(strings.Join(segment, "")))
			}

			// Construct the merge reader from the set of readers.
			rc, err := newMergeReadCloser(rcs)
			if err != nil {
				t.Fatal(err)
			}

			// Take lines from the merge reader until EOF.
			have := []string{}
			s := bufio.NewScanner(rc)
			s.Split(scanLinesPreserveNewline)
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
		s.Split(scanLinesPreserveNewline)
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
	t.Parallel()

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
	t.Parallel()

	var (
		filesys  = fs.NewVirtualFilesystem()
		filename = "extant"
		segments = []readSegment{{path: filename, file: nopReadCloser{}}, {path: "nonexistant", file: nopReadCloser{}}}
		pass     = func([]byte) bool { return true }
		bufsz    = int64(1024)
	)

	f, err := filesys.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Should not panic.
	makeConcurrentFilteringReadClosers(filesys, segments, pass, bufsz)
}

type mockLog struct {
	*bytes.Buffer
}

func (log *mockLog) Create() (WriteSegment, error) {
	return &mockWriteSegment{log.Buffer}, nil
}

func (log *mockLog) Query(qp QueryParams, statsOnly bool) (QueryResult, error) {
	return QueryResult{}, errors.New("not implemented")
}

func (log *mockLog) Overlapping() ([]ReadSegment, error) {
	return nil, errors.New("not implemented")
}

func (log *mockLog) Sequential() ([]ReadSegment, error) {
	return nil, errors.New("not implemented")
}

func (log *mockLog) Trashable(oldestRecord time.Time) ([]ReadSegment, error) {
	return nil, errors.New("not implemented")
}

func (log *mockLog) Purgeable(oldestModTime time.Time) ([]TrashSegment, error) {
	return nil, errors.New("not implemented")
}

func (log *mockLog) Stats() (LogStats, error) {
	return LogStats{}, errors.New("not implemented")
}

func (log *mockLog) Close() error { return nil }

type mockWriteSegment struct{ *bytes.Buffer }

func (mockWriteSegment) Close(ulid.ULID, ulid.ULID) error { return nil }
func (mockWriteSegment) Delete() error                    { return nil }
