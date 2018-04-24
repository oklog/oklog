package record

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
	"time"

	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/ulid"
)

func TestQueryResultMerge(t *testing.T) {
	t.Skip("TODO(pb)")
}

func TestULIDOrTimeParse(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]ULIDOrTime{
		"01BC3NABW20000000000000000": {
			ulid.MustParse("01BC3NABW20000000000000000"),
			mustParseRFC3339("2017-03-25T21:17:54.946Z"), // truncated
		},
		"01BC3NABW21234567890123456": {
			ulid.MustParse("01BC3NABW21234567890123456"),
			mustParseRFC3339("2017-03-25T21:17:54.946Z"), // truncated
		},
		"2017-03-25T21:18:20.05731891Z": {
			ulid.MustParse("01BC3NB4CS0000000000000000"),
			mustParseRFC3339("2017-03-25T21:18:20.05731891Z"),
		},
	} {
		t.Run(input, func(t *testing.T) {
			var have ULIDOrTime
			if err := have.Parse(input); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, have) {
				t.Fatalf("want %+v, have %+v", want, have)
			}
		})
	}
}

func mustParseRFC3339(s string) time.Time {
	t, err := time.ParseInLocation(time.RFC3339Nano, s, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

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
		if _, _, _, err := MergeRecords(&buf, readers...); err != nil {
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
			segments := make([]QuerySegment, len(testcase.input))
			for i, path := range testcase.input {
				segments[i] = QuerySegment{Path: path, Reader: nopReadCloser{}}
			}

			// Batch them.
			result := batchSegments(segments)

			// Extract ranges.
			have := make([][]string, len(result))
			for i, segments := range result {
				have[i] = make([]string, len(segments))
				for j, segment := range segments {
					have[i][j] = segment.Path
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

func (nopReadCloser) Read() ([]byte, error) { return nil, io.EOF }
func (nopReadCloser) Close() error          { return nil }

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
			rc, err := NewMergeReadCloser(rcs)
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
	r, err := NewMergeReadCloser(generateSegments(b, 128, size, "testdata/segments"))
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
			in := NewReader(bytes.NewReader(input.Bytes()))
			re := regexp.MustCompile(testcase.q)
			pass := FilterBoundedRegex(testcase.from, testcase.to, re)
			rc := newConcurrentFilteringReadCloser(NopCloser(in), pass, 1024)
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
		src             = NopCloser(NewReader(strings.NewReader(input)))
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
		segments = []QuerySegment{{Path: filename, Reader: nopReadCloser{}}, {Path: "nonexistant", Reader: nopReadCloser{}}}
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
