package store

import (
	"bytes"

	"github.com/oklog/ulid"
)

// overlap returns true if the range [a, b] overlaps with [c, d].
func overlap(a, b, c, d ulid.ULID) bool {
	as, bs, cs, ds := a[:], b[:], c[:], d[:]
	if bytes.Compare(as, bs) > 0 {
		as, bs = bs, as
	}
	if bytes.Compare(cs, ds) > 0 {
		cs, ds = ds, cs
	}
	if bytes.Compare(as, cs) > 0 {
		as, bs, cs, ds = cs, ds, as, bs
	}
	if bytes.Compare(bs, cs) < 0 {
		return false
	}
	return true
}
