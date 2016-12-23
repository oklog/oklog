package store

import (
	"reflect"
	"testing"

	"github.com/oklog/ulid"
)

func TestChooseFirstSequential(t *testing.T) {
	const targetSize = 100 * 1024 // 100KB
	for i, testcase := range []struct {
		input   sortableSegments
		minimum int
		want    []string
	}{
		{
			input: sortableSegments{
				sortableSegment{low: ulid.MustNew(100, nil), path: "A", size: 123},
				sortableSegment{low: ulid.MustNew(200, nil), path: "B", size: 123},
				sortableSegment{low: ulid.MustNew(300, nil), path: "C", size: 123},
				sortableSegment{low: ulid.MustNew(400, nil), path: "D", size: 123},
				sortableSegment{low: ulid.MustNew(500, nil), path: "E", size: 123},
			},
			minimum: 2,
			want:    []string{"A", "B", "C", "D", "E"},
		},
		{
			input: sortableSegments{
				sortableSegment{low: ulid.MustNew(100, nil), path: "A", size: 100000},
				sortableSegment{low: ulid.MustNew(200, nil), path: "B", size: 2000},
				sortableSegment{low: ulid.MustNew(300, nil), path: "C", size: 2000},
				sortableSegment{low: ulid.MustNew(400, nil), path: "D", size: 2000},
			},
			minimum: 2,
			want:    []string{"A", "B"},
		},
		{
			input: sortableSegments{
				sortableSegment{low: ulid.MustNew(100, nil), path: "A", size: 118401},
				sortableSegment{low: ulid.MustNew(200, nil), path: "B", size: 623},
				sortableSegment{low: ulid.MustNew(300, nil), path: "C", size: 1000},
			},
			minimum: 2,
			want:    []string{"B", "C"},
		},
	} {
		have := chooseFirstSequential(testcase.input, testcase.minimum, targetSize)
		if want := testcase.want; !reflect.DeepEqual(want, have) {
			t.Errorf("testcase index %d: want %v, have %v", i, want, have)
		}
	}
}
