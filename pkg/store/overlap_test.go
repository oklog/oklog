package store

import (
	"testing"

	"github.com/oklog/ulid"
)

func TestOverlap(t *testing.T) {
	for _, testcase := range []struct {
		a, b, c, d ulid.ULID
		want       bool
	}{
		{
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(201, nil),
			d:    ulid.MustNew(301, nil),
			want: false,
		},
		{
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(199, nil),
			d:    ulid.MustNew(300, nil),
			want: true,
		},
		{
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(200, nil),
			d:    ulid.MustNew(300, nil),
			want: true,
		},
		{
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(201, nil),
			d:    ulid.MustNew(300, nil),
			want: false,
		},
		{
			a:    ulid.MustNew(200, nil), // test a, b swap
			b:    ulid.MustNew(100, nil),
			c:    ulid.MustNew(199, nil),
			d:    ulid.MustNew(300, nil),
			want: true,
		},
		{
			a:    ulid.MustNew(100, nil),
			b:    ulid.MustNew(200, nil),
			c:    ulid.MustNew(300, nil), // test c, d swap
			d:    ulid.MustNew(199, nil),
			want: true,
		},
		{
			a:    ulid.MustNew(199, nil), // test [a b], [c d] swap
			b:    ulid.MustNew(300, nil),
			c:    ulid.MustNew(100, nil),
			d:    ulid.MustNew(200, nil),
			want: true,
		},
		{
			a:    ulid.MustNew(300, nil), // test all swaps
			b:    ulid.MustNew(199, nil),
			c:    ulid.MustNew(200, nil),
			d:    ulid.MustNew(100, nil),
			want: true,
		},
		{
			a:    ulid.MustNew(300, nil), // test all swaps, negative result
			b:    ulid.MustNew(201, nil),
			c:    ulid.MustNew(200, nil),
			d:    ulid.MustNew(100, nil),
			want: false,
		},
	} {
		a, b, c, d := testcase.a, testcase.b, testcase.c, testcase.d
		if want, have := testcase.want, overlap(a, b, c, d); want != have {
			t.Errorf("overlap(%s, %s, %s, %s): want %v, have %v", a, b, c, d, want, have)
		}
	}
}
