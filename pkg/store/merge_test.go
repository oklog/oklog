package store

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/oklog/ulid"

	"github.com/oklog/oklog/pkg/fs"
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

	dst, err := NewFileLog(fs.NewNopFilesystem(), "/", segmentTargetSize, segmentBufferSize)
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
