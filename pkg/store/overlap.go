package store

import (
	"github.com/oklog/ulid"
)

// overlap returns true if the range [a, b] overlaps with [c, d].
func overlap(a, b, c, d ulid.ULID) bool {
	// sort bound
	if a.Compare(b) > 0 {
		a, b = b, a
	}
	if c.Compare(d) > 0 {
		c, d = d, c
	}

	// [a, b] ∩  [c, d] == nil, return false
	if b.Compare(c) < 0 || d.Compare(a) < 0 {
		return false
	}
	return true
}
