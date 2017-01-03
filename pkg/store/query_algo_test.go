package store

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"

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

func TestMergeReader(t *testing.T) {
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
			readers := make([]io.Reader, len(testcase.input))
			for i, s := range testcase.input {
				segment := strings.Join(s, "\n") + "\n"
				readers[i] = strings.NewReader(segment)
			}

			// Construct the merge reader from the set of readers.
			r, err := newMergeReader(readers)
			if err != nil {
				t.Fatal(err)
			}

			// Take lines from the merge reader until EOF.
			have := []string{}
			s := bufio.NewScanner(r)
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

func TestRecordFilteringReader(t *testing.T) {
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
		err      error
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
			// Build the filtering reader.
			in := bytes.NewReader(input.Bytes())
			r, err := newRecordFilteringReader(in, testcase.from, testcase.to, testcase.q)
			if want, have := testcase.err, err; !reflect.DeepEqual(want, have) {
				t.Errorf("want error %v, have %v", want, have)
			}

			// Make sure we got what we want!
			if want, have := testcase.want, records(r); !reflect.DeepEqual(want, have) {
				t.Errorf("want %v, have %v", want, have)
			}
		})
	}
}
