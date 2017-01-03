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

	input := "1\n22\n333\n4444\n55555"
	for i, testcase := range []struct {
		name    string
		filters []func([]byte) bool
		want    []string
	}{
		{
			name:    "no filters",
			filters: nil,
			want:    []string{"1", "22", "333", "4444", "55555"},
		},
		{
			name:    "pass one",
			filters: []func([]byte) bool{func(b []byte) bool { return bytes.Equal(bytes.TrimSpace(b), []byte("1")) }},
			want:    []string{"1"},
		},
		{
			name:    "fail one",
			filters: []func([]byte) bool{func(b []byte) bool { return !strings.HasPrefix(string(b), "1") }},
			want:    []string{"22", "333", "4444", "55555"},
		},
		{
			name:    "pass some",
			filters: []func([]byte) bool{func(b []byte) bool { return len(bytes.TrimSpace(b))%2 == 1 }},
			want:    []string{"1", "333", "55555"},
		},
		{
			name: "filters have AND semantics",
			filters: []func([]byte) bool{
				func(b []byte) bool { return len(bytes.TrimSpace(b))%2 == 1 },     // 1, 333, 55555
				func(b []byte) bool { return !strings.HasPrefix(string(b), "1") }, // 22, 333, 4444, 55555
			},
			want: []string{"333", "55555"},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
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
		})
	}
}
