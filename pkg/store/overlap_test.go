package store

import (
	"testing"

	"github.com/oklog/ulid"
)

func TestOverlap(t *testing.T) {
	t.Parallel()

	for _, testcase := range []struct {
		name       string
		a, b, c, d ulid.ULID
		want       bool
	}{
		{
			name: "basic nonoverlap",
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(201, nil),
			d:    ulid.MustNew(301, nil),
			want: false,
		},
		{
			name: "basic overlap",
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(199, nil),
			d:    ulid.MustNew(300, nil),
			want: true,
		},
		{
			name: "touching",
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(200, nil),
			d:    ulid.MustNew(300, nil),
			want: true,
		},
		{
			name: "not touching",
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(201, nil),
			d:    ulid.MustNew(300, nil),
			want: false,
		},
		{
			name: "a b swap",
			a:    ulid.MustNew(200, nil),
			b:    ulid.MustNew(100, nil),
			c:    ulid.MustNew(199, nil),
			d:    ulid.MustNew(300, nil),
			want: true,
		},
		{
			name: "c d swap",
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(300, nil), // test c, d swap
			d:    ulid.MustNew(199, nil),
			want: true,
		},
		{
			name: "ab cd swap",
			a:    ulid.MustNew(199, nil),
			b:    ulid.MustNew(300, nil),
			c:    ulid.MustNew(100, nil),
			d:    ulid.MustNew(200, nil),
			want: true,
		},
		{
			name: "all swaps overlap",
			a:    ulid.MustNew(300, nil),
			b:    ulid.MustNew(199, nil),
			c:    ulid.MustNew(200, nil),
			d:    ulid.MustNew(100, nil),
			want: true,
		},
		{
			name: "all swaps nonoverlap",
			a:    ulid.MustNew(300, nil),
			b:    ulid.MustNew(201, nil),
			c:    ulid.MustNew(200, nil),
			d:    ulid.MustNew(100, nil),
			want: false,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			a, b, c, d := testcase.a, testcase.b, testcase.c, testcase.d
			if want, have := testcase.want, overlap(a, b, c, d); want != have {
				t.Fatalf("want %v, have %v", want, have)
			}
		})
	}
}
