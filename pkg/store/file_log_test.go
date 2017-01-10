package store

import (
	"reflect"
	"testing"
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
