package store

import (
	"bufio"
	"bytes"
	"io"
	"strings"

	"github.com/oklog/prototype/pkg/fs"
	"github.com/oklog/ulid"
)

// newBatchReader converts a batch of segment files to a single io.Reader.
// Records are yielded in time order, oldest first, hopefully efficiently!
func newBatchReader(fs fs.Filesystem, segments []string) (io.Reader, error) {
	// Batch the segments, and construct a reader for each batch.
	var readers []io.Reader
	for _, batch := range batchSegments(segments) {
		var (
			r   io.Reader
			err error
		)
		switch len(batch) {
		case 0:
			continue // weird
		case 1:
			r, err = fs.Open(batch[0]) // optimization!
		default:
			r, err = newFileMergeReader(fs, batch)
		}
		if err != nil {
			return nil, err
		}
		readers = append(readers, r)
	}

	// The sequential reader drains each reader in sequence.
	return sequentialReader(readers), nil
}

// batchSegments batches segments together if they overlap in time.
func batchSegments(segments []string) [][]string {
	// First, parse ranges from filename.
	type lexrange struct{ a, b string }
	ranges := make([]lexrange, len(segments))
	for i := range segments {
		f := strings.SplitN(basename(segments[i]), "-", 2)
		ranges[i] = lexrange{f[0], f[1]}
	}

	// Now, walk the segments.
	var (
		result = [][]string{}
		group  []string // current
		b      string   // of the group
	)
	for i := range segments {
		switch {
		case len(group) <= 0:
			// If the group is empty, it gets the segment.
			group = []string{segments[i]}
			b = ranges[i].b

		case ranges[i].a > b:
			// If the current segment doesn't overlap with the group,
			// the group is closed and we start a new group.
			result = append(result, group)
			group = []string{segments[i]}
			b = ranges[i].b

		default:
			// The current segment overlaps with the group,
			// so it is absorbed into the group.
			group = append(group, segments[i])
			b = max(b, ranges[i].b)
		}
	}
	if len(group) > 0 {
		result = append(result, group)
	}

	return result
}

func max(a, b string) string {
	if a > b {
		return a
	}
	return b
}

// sequentialReader reads from each reader in sequence.
type sequentialReader []io.Reader

func (r sequentialReader) Read(p []byte) (int, error) {
	// Check if we're not already done.
	if len(r) <= 0 {
		return 0, io.EOF
	}

	// Get ready to read something.
	var (
		n   int
		err = io.EOF
	)
	for err == io.EOF && len(r) > 0 {
		// Read from the first reader.
		n, err = r[0].Read(p)

		// If we read anything, we should return it.
		// Even if we got EOF, return it anyway.
		if n > 0 {
			return n, err
		}

		// We didn't read anything. If we got EOF,
		// try the next reader straight away.
		if err == io.EOF {
			r = r[1:]
			continue
		}

		// We didn't read anything, and we got a non-EOF.
		// This is fatal.
		return n, err
	}

	// We've drained all our readers.
	return n, err
}

// mergeReader performs a K-way merge from multiple readers.
type mergeReader struct {
	scanner []*bufio.Scanner
	ok      []bool
	record  [][]byte
	id      [][]byte
}

func newMergeReader(readers []io.Reader) (io.Reader, error) {
	// Initialize our state.
	r := &mergeReader{
		scanner: make([]*bufio.Scanner, len(readers)),
		ok:      make([]bool, len(readers)),
		record:  make([][]byte, len(readers)),
		id:      make([][]byte, len(readers)),
	}

	// Initialize all of the scanners and their first record.
	for i := 0; i < len(readers); i++ {
		r.scanner[i] = bufio.NewScanner(readers[i])
		if err := r.advance(i); err != nil {
			return nil, err
		}
	}

	// Ready to read.
	return r, nil
}

func newFileMergeReader(fs fs.Filesystem, segments []string) (io.Reader, error) {
	// Convert segments to io.Readers via the filesystem.
	readers := make([]io.Reader, len(segments))
	for i := range segments {
		r, err := fs.Open(segments[i])
		if err != nil {
			return nil, err
		}
		readers[i] = r
	}
	return newMergeReader(readers)
}

func (r *mergeReader) Read(p []byte) (int, error) {
	// Pick the source with the smallest ID.
	// TODO(pb): could be improved with an e.g. tournament tree
	chosen := -1 // index
	for i := range r.id {
		if !r.ok[i] {
			continue // already drained
		}
		if chosen < 0 || bytes.Compare(r.id[i], r.id[chosen]) < 0 {
			chosen = i
		}
	}
	if chosen < 0 {
		return 0, io.EOF // everything is drained
	}

	// Copy the record over.
	src := append(r.record[chosen], '\n')
	n := copy(p, src)
	if n < len(src) {
		panic("short read!") // TODO(pb): obviously needs fixing
	}

	// Advance the chosen source.
	r.advance(chosen)

	// One read is complete.
	return n, nil
}

func (r *mergeReader) advance(i int) error {
	if r.ok[i] = r.scanner[i].Scan(); r.ok[i] {
		r.record[i] = r.scanner[i].Bytes()
		if len(r.record[i]) < ulid.EncodedSize {
			panic("record is too short")
		}
		r.id[i] = r.record[i][:ulid.EncodedSize]
	} else if err := r.scanner[i].Err(); err != nil && err != io.EOF {
		return err
	}
	return nil
}

type recordFilteringReader struct {
	s       *bufio.Scanner
	filters []func([]byte) bool // AND semantics
}

func newRecordFilteringReader(r io.Reader, filters ...func([]byte) bool) io.Reader {
	return recordFilteringReader{
		s:       bufio.NewScanner(r),
		filters: filters,
	}
}

func (r recordFilteringReader) Read(p []byte) (int, error) {
	for {
		// Take the next record.
		if !r.s.Scan() {
			return 0, io.EOF
		}

		// If it passes, write and return.
		if b := append(r.s.Bytes(), '\n'); r.pass(b) {
			n := copy(p, b)
			if n < len(b) {
				panic("short read!") // TODO(pb): obviously needs fixing
			}
			return n, nil
		}
	}
}

func (r recordFilteringReader) pass(b []byte) bool {
	for _, f := range r.filters {
		if !f(b) { // any failure = complete failure
			return false
		}
	}
	return true
}
