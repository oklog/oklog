package store

import (
	"bufio"
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/oklog/ulid"
)

func TestBatchSegments(t *testing.T) {
	for _, testcase := range []struct {
		input []string
		want  [][]string
	}{
		{
			[]string{}, // no elements
			[][]string{},
		},
		{
			[]string{"A-Z"}, // one element
			[][]string{{"A-Z"}},
		},
		{
			[]string{"A-M", "N-Z"}, // no overlap
			[][]string{{"A-M"}, {"N-Z"}},
		},
		{
			[]string{"A-M", "N-R", "O-S", "W-Z"}, // basic overlap
			[][]string{{"A-M"}, {"N-R", "O-S"}, {"W-Z"}},
		},
		{
			[]string{"A-M", "L-R", "O-S", "Q-Z"}, // complete, staggering overlap
			[][]string{{"A-M", "L-R", "O-S", "Q-Z"}},
		},
		{
			[]string{"A-M", "L-R", "O-S", "W-Z"}, // mostly overlap
			[][]string{{"A-M", "L-R", "O-S"}, {"W-Z"}},
		},
		{
			[]string{"A-B", "B-C", "C-D", "E-F", "F-G"}, // equality should count as overlap
			[][]string{{"A-B", "B-C", "C-D"}, {"E-F", "F-G"}},
		},
	} {
		if want, have := testcase.want, batchSegments(testcase.input); !reflect.DeepEqual(want, have) {
			t.Errorf("%v: want %v, have %v", testcase.input, want, have)
		}
	}
}

func TestSequentialReader(t *testing.T) {
	for _, testcase := range []struct {
		input [][]string
		want  []string
	}{
		{
			[][]string{},
			[]string{},
		},
		{
			[][]string{{"A"}},
			[]string{"A"},
		},
		{
			[][]string{{"A"}, {"B"}},
			[]string{"A", "B"},
		},
		{
			[][]string{{"A", "B", "C"}, {"D"}, {"E", "F", "G"}},
			[]string{"A", "B", "C", "D", "E", "F", "G"},
		},
	} {
		// Convert string slice to a set of readers.
		readers := make([]io.Reader, len(testcase.input))
		for i, s := range testcase.input {
			segment := strings.Join(s, "\n") + "\n"
			readers[i] = strings.NewReader(segment)
		}

		// Construct the sequential reader from the set of readers.
		r := sequentialReader(readers)

		// Take lines from the sequential reader until EOF.
		have := []string{}
		s := bufio.NewScanner(r)
		for s.Scan() {
			have = append(have, s.Text())
		}

		// Make sure we got what we want!
		if want := testcase.want; !reflect.DeepEqual(want, have) {
			t.Errorf("%v: want %v, have %v", testcase.input, want, have)
		}
	}
}

func TestMergeReader(t *testing.T) {
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
		input [][]string
		want  []string
	}{
		{
			[][]string{},
			[]string{},
		},
		{
			[][]string{{u150}},
			[]string{u150},
		},
		{
			[][]string{{u100, u200, u300}, {u150, u250, u350}},
			[]string{u100, u150, u200, u250, u300, u350},
		},
		{
			[][]string{{u100, u200, u300}, {u250, u350, u450}, {u150, u400, u500}},
			[]string{u100, u150, u200, u250, u300, u350, u400, u450, u500},
		},
	} {
		// Convert string slice to a set of readers.
		readers := make([]io.Reader, len(testcase.input))
		for i, s := range testcase.input {
			segment := strings.Join(s, "\n") + "\n"
			readers[i] = strings.NewReader(segment)
		}

		// Construct the merge reader from the set of readers.
		r, err := newMergeReader(readers)
		if err != nil {
			t.Errorf("%v: %v", testcase.input, err)
			continue
		}

		// Take lines from the merge reader until EOF.
		have := []string{}
		s := bufio.NewScanner(r)
		for s.Scan() {
			have = append(have, s.Text())
		}

		// Make sure we got what we want!
		if want := testcase.want; !reflect.DeepEqual(want, have) {
			t.Errorf("%v: want %v, have %v", testcase.input, want, have)
		}
	}
}

func TestRecordFilteringReader(t *testing.T) {
	input := "1\n22\n333\n4444\n55555"
	for i, testcase := range []struct {
		filters []func([]byte) bool
		want    []string
	}{
		{
			filters: nil,
			want:    []string{"1", "22", "333", "4444", "55555"},
		},
		{
			filters: []func([]byte) bool{func(b []byte) bool { return bytes.Equal(bytes.TrimSpace(b), []byte("1")) }},
			want:    []string{"1"},
		},
		{
			filters: []func([]byte) bool{func(b []byte) bool { return !strings.HasPrefix(string(b), "1") }},
			want:    []string{"22", "333", "4444", "55555"},
		},
		{
			filters: []func([]byte) bool{func(b []byte) bool { return len(bytes.TrimSpace(b))%2 == 1 }},
			want:    []string{"1", "333", "55555"},
		},
		{
			filters: []func([]byte) bool{ // test AND semantics of filters
				func(b []byte) bool { return len(bytes.TrimSpace(b))%2 == 1 },     // 1, 333, 55555
				func(b []byte) bool { return !strings.HasPrefix(string(b), "1") }, // 22, 333, 4444, 55555
			},
			want: []string{"333", "55555"},
		},
	} {
		// Build the filtering reader.
		r := newRecordFilteringReader(strings.NewReader(input), testcase.filters...)

		// Take lines from the filtering reader until EOF.
		have := []string{}
		s := bufio.NewScanner(r)
		for s.Scan() {
			have = append(have, s.Text())
		}

		// Make sure we got what we want!
		if want := testcase.want; !reflect.DeepEqual(want, have) {
			t.Errorf("%d: want %v, have %v", i, want, have)
		}
	}
}
