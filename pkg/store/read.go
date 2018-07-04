package store

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"

	"github.com/oklog/oklog/pkg/fs"
)

// ErrShortRead is returned when a read is unexpectedly shortened.
var ErrShortRead = errors.New("short read")

// mergeRecords takes a set of io.Readers that contain ULID-prefixed records in
// individually-sorted order, and writes the globally-sorted output to the
// io.Writer.
func mergeRecords(w io.Writer, readers ...io.Reader) (low, high ulid.ULID, n int64, err error) {
	// Optimization and safety.
	if len(readers) == 0 {
		return low, high, 0, nil
	}

	// https://github.com/golang/go/wiki/SliceTricks
	notnil := readers[:0]
	for _, r := range readers {
		if r != nil {
			notnil = append(notnil, r)
		}
	}
	readers = notnil

	// Initialize our state.
	var (
		first   = true
		scanner = make([]*bufio.Scanner, len(readers))
		ok      = make([]bool, len(readers))
		record  = make([][]byte, len(readers))
		id      = make([][]byte, len(readers))
	)

	// Advance moves the next record from a reader into our state.
	advance := func(i int) error {
		if ok[i] = scanner[i].Scan(); ok[i] {
			record[i] = scanner[i].Bytes()
			// Something nice, like bytes.Fields, is too slow!
			//id[i] = bytes.Fields(record[i])[0]
			if len(record[i]) < ulid.EncodedSize {
				panic("short record")
			}
			id[i] = record[i][:ulid.EncodedSize]
		} else if err := scanner[i].Err(); err != nil && err != io.EOF {
			return err
		}
		return nil
	}

	// Initialize all of the scanners and their first record.
	for i := 0; i < len(readers); i++ {
		scanner[i] = bufio.NewScanner(readers[i])
		scanner[i].Split(scanLinesPreserveNewline)
		if err := advance(i); err != nil {
			return low, high, n, err
		}
	}

	// Loop until all readers are drained.
	var (
		chosenIndex int
		chosenULID  ulid.ULID
	)
	for {
		// Choose the smallest ULID from all of the IDs.
		chosenIndex = -1
		for i := 0; i < len(readers); i++ {
			if !ok[i] {
				continue
			}
			if chosenIndex < 0 || bytes.Compare(id[i], id[chosenIndex]) < 0 {
				chosenIndex = i
			}
		}
		if chosenIndex < 0 {
			break // drained all the scanners
		}

		// The first ULID from this batch of readers is the low ULID.
		// The last ULID from this batch of readers is the high ULID.
		// Record the first ULID as low, and the latest ULID as high.
		chosenULID.UnmarshalText(id[chosenIndex])
		if first { // record first as low
			low, first = chosenULID, false
		} else if !first && chosenULID == high { // duplicate!
			if err := advance(chosenIndex); err != nil {
				return low, high, n, err
			}
			continue
		}
		high = chosenULID // record most recent as high

		// Write the record.
		n0, err := w.Write(record[chosenIndex])
		if err != nil {
			return low, high, n, err
		}
		n += int64(n0)

		// Advance the chosen scanner by 1 record.
		if err := advance(chosenIndex); err != nil {
			return low, high, n, err
		}
	}

	return low, high, n, nil
}

// mergeRecordsToLog is a specialization of mergeRecords.
// It enforces segmentTargetSize by creating WriteSegments as necessary.
// It has best-effort semantics, e.g. it won't split large records.
func mergeRecordsToLog(dst Log, segmentTargetSize int64, readers ...io.Reader) (n int64, err error) {
	// Optimization and safety.
	if len(readers) == 0 {
		return 0, nil
	}

	// https://github.com/golang/go/wiki/SliceTricks
	notnil := readers[:0]
	for _, r := range readers {
		if r != nil {
			notnil = append(notnil, r)
		}
	}
	readers = notnil

	// Per-segment state.
	writeSegment, err := dst.Create()
	if err != nil {
		return n, err
	}
	defer func() {
		// Don't leak active segments.
		if writeSegment != nil {
			if deleteErr := writeSegment.Delete(); deleteErr != nil {
				panic(deleteErr)
			}
		}
	}()
	var (
		nSegment int64
		first    = true
		low      ulid.ULID
		high     ulid.ULID
	)

	// Global state.
	var (
		scanner = make([]*bufio.Scanner, len(readers))
		ok      = make([]bool, len(readers))
		record  = make([][]byte, len(readers))
		id      = make([][]byte, len(readers))
	)

	// Advance moves the next record from a reader into our state.
	advance := func(i int) error {
		if ok[i] = scanner[i].Scan(); ok[i] {
			record[i] = scanner[i].Bytes()
			// Something nice, like bytes.Fields, is too slow!
			//id[i] = bytes.Fields(record[i])[0]
			if len(record[i]) < ulid.EncodedSize {
				panic("short record")
			}
			id[i] = record[i][:ulid.EncodedSize]
		} else if err := scanner[i].Err(); err != nil && err != io.EOF {
			return err
		}
		return nil
	}

	// Initialize all of the scanners and their first record.
	for i := 0; i < len(readers); i++ {
		scanner[i] = bufio.NewScanner(readers[i])
		scanner[i].Split(scanLinesPreserveNewline)
		if err := advance(i); err != nil {
			return n, err
		}
	}

	// Loop until all readers are drained.
	var (
		chosenIndex int
		chosenULID  ulid.ULID
	)

	var nCompleted int64 = 0
	for {
		// Choose the smallest ULID.
		chosenIndex = -1
		for i := 0; i < len(readers); i++ {
			if !ok[i] {
				continue
			}
			if chosenIndex < 0 || bytes.Compare(id[i], id[chosenIndex]) < 0 {
				chosenIndex = i
			}
		}
		if chosenIndex < 0 {
			break // drained all the scanners
		}

		// The first ULID from this batch of readers is the low ULID.
		// The last ULID from this batch of readers is the high ULID.
		// Record the first ULID as low, and the latest ULID as high.
		chosenULID.UnmarshalText(id[chosenIndex])
		if first { // record first as low
			low, first = chosenULID, false
		} else if !first && chosenULID == high { // duplicate!
			if err := advance(chosenIndex); err != nil {
				return n, err
			}
			continue
		}
		high = chosenULID // record most recent as high

		// Write the record.
		_, err := writeSegment.Write(record[chosenIndex])
		if err != nil {
			return n, err
		}
		n0 := writeSegment.Size()
		n = nCompleted + int64(n0)
		nSegment = int64(n0)

		if nSegment >= segmentTargetSize {
			// We've met our target size. Close the segment.
			if err := writeSegment.Close(low, high); err != nil {
				return n, err
			}

			// Create a new segment, and reset our per-segment state.
			writeSegment, err = dst.Create()
			if err != nil {
				return n, err
			}

			nCompleted += nSegment
			nSegment = 0
			first = true
		}

		// Advance the chosen scanner by 1 record.
		if err := advance(chosenIndex); err != nil {
			return n, err
		}
	}

	if nSegment > 0 {
		if err = writeSegment.Close(low, high); err != nil {
			return n, err
		}
	} else {
		err = writeSegment.Delete()
	}
	writeSegment = nil
	return n, err
}

// newQueryReadCloser converts a batch of segments to a single io.ReadCloser.
// Records are yielded in time order, oldest first, hopefully efficiently!
// Only records passing the recordFilter are yielded.
// The sz of the segment files can be used as a proxy for read effort.
func newQueryReadCloser(fs fs.Filesystem, segments []readSegment, pass recordFilter, bufsz int64, reporter EventReporter) (rc io.ReadCloser, sz int64, err error) {
	// We will build successive ReadClosers for each batch.
	var rcs []io.ReadCloser

	// Don't leak FDs on error.
	defer func() {
		if err != nil {
			for i, rc := range rcs {
				if cerr := rc.Close(); cerr != nil {
					reporter.ReportEvent(Event{
						Op: "newQueryReadCloser", Error: cerr,
						Msg: fmt.Sprintf("Close of intermediate io.ReadCloser %d/%d in error path failed", i+1, len(rcs)),
					})
				}
			}
			rcs = nil
		}
	}()

	// Batch the segments, and construct a ReadCloser for each batch.
	for _, batch := range batchSegments(segments) {
		switch len(batch) {
		case 0:
			continue // weird

		case 1:
			// A batch of one can be read straight thru.
			sz += batch[0].size
			rcs = append(rcs, newConcurrentFilteringReadCloser(batch[0].file, pass, bufsz))

		default:
			// A batch of N requires a K-way merge.
			cfrcs, batchsz, err := makeConcurrentFilteringReadClosers(fs, batch, pass, bufsz)
			if err != nil {
				return nil, sz, err
			}
			mrc, err := newMergeReadCloser(cfrcs)
			if err != nil {
				return nil, sz, err
			}
			rcs = append(rcs, mrc)
			sz += batchsz
		}
	}

	// MultiReadCloser uses an io.MultiReader under the hood.
	// A MultiReader reads from each reader in sequence.
	rc = newMultiReadCloser(rcs...)
	rcs = nil // ownership of each ReadCloser is passed
	return rc, sz, nil
}

// batchSegments batches segments together if they overlap in time.
func batchSegments(segments []readSegment) [][]readSegment {
	// First, parse ranges from filename.
	// TODO(pb): handle weird filenames better, somehow
	type lexrange struct{ a, b string }
	ranges := make([]lexrange, len(segments))
	for i := range segments {
		f := strings.SplitN(basename(segments[i].path), "-", 2)
		ranges[i] = lexrange{f[0], f[1]}
	}

	// Now, walk the segments.
	var (
		result = [][]readSegment{} // non-nil
		group  []readSegment       // that we're currently building
		b      string              // end ULID of the group
	)
	for i := range segments {
		switch {
		case len(group) <= 0:
			// If the group is empty, it gets the segment.
			group = []readSegment{segments[i]}
			b = ranges[i].b

		case ranges[i].a > b:
			// If the current segment doesn't overlap with the group,
			// the group is closed and we start a new group.
			result = append(result, group)
			group = []readSegment{segments[i]}
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

func makeConcurrentFilteringReadClosers(fs fs.Filesystem, segments []readSegment, pass recordFilter, bufsz int64) (rcs []io.ReadCloser, sz int64, err error) {
	rcs = make([]io.ReadCloser, len(segments))
	for i := range segments {
		sz += segments[i].size
		rcs[i] = newConcurrentFilteringReadCloser(segments[i].file, pass, bufsz)
	}
	return rcs, sz, nil
}

func newConcurrentFilteringReadCloser(src io.ReadCloser, pass recordFilter, bufsz int64) io.ReadCloser {
	r, w := nio.Pipe(buffer.New(bufsz))
	go func() {
		defer src.Close() // close the fs.File when we're done reading

		// TODO(pb): this may be a regression; need to benchmark
		s := bufio.NewScanner(src)
		s.Split(scanLinesPreserveNewline)

		for s.Scan() {
			line := s.Bytes()
			if !pass(line) {
				continue
			}

			switch n, err := w.Write(line); {
			case err == io.ErrClosedPipe:
				return // no need to close
			case err != nil:
				w.CloseWithError(err)
				return
			case n < len(line):
				w.CloseWithError(io.ErrShortWrite)
				return
			}
		}
		w.CloseWithError(s.Err())
	}()
	return r
}

// readSegment models a segment file on disk.
type readSegment struct {
	path string // ULID-ULID.extension
	file io.ReadCloser
	size int64
}

// mergeReadCloser performs a K-way merge from multiple readers.
type mergeReadCloser struct {
	close   []io.Closer
	scanner []*bufio.Scanner
	ok      []bool
	record  [][]byte
	id      [][]byte
}

func newMergeReadCloser(rcs []io.ReadCloser) (io.ReadCloser, error) {
	// Initialize our state.
	rc := &mergeReadCloser{
		close:   make([]io.Closer, len(rcs)),
		scanner: make([]*bufio.Scanner, len(rcs)),
		ok:      make([]bool, len(rcs)),
		record:  make([][]byte, len(rcs)),
		id:      make([][]byte, len(rcs)),
	}

	// Initialize all of the scanners and their first record.
	const (
		scanBufferSize   = 64 * 1024      // 64KB
		scanMaxTokenSize = scanBufferSize // if equal, no allocs
	)
	for i := 0; i < len(rcs); i++ {
		rc.close[i] = rcs[i]
		rc.scanner[i] = bufio.NewScanner(rcs[i])
		rc.scanner[i].Split(scanLinesPreserveNewline)
		rc.scanner[i].Buffer(make([]byte, scanBufferSize), scanMaxTokenSize)
		if err := rc.advance(i); err != nil {
			return nil, err
		}
	}

	// Ready to read.
	return rc, nil
}

func (rc *mergeReadCloser) Read(p []byte) (int, error) {
	// Pick the source with the smallest ID.
	// TODO(pb): could be improved with an e.g. tournament tree
	smallest := -1 // index
	for i := range rc.id {
		if !rc.ok[i] {
			continue // already drained
		}
		switch {
		case smallest < 0, bytes.Compare(rc.id[i], rc.id[smallest]) < 0:
			smallest = i
		case bytes.Compare(rc.id[i], rc.id[smallest]) == 0: // duplicate
			if err := rc.advance(i); err != nil {
				return 0, err
			}
			continue
		}
	}
	if smallest < 0 {
		return 0, io.EOF // everything is drained
	}

	// Copy the record over.
	n := copy(p, rc.record[smallest])
	if n < len(rc.record[smallest]) {
		return n, ErrShortRead
	}

	// Advance the chosen source.
	if err := rc.advance(smallest); err != nil {
		return n, errors.Wrapf(err, "advancing reader %d", smallest)
	}

	// One read is complete.
	return n, nil
}

func (rc *mergeReadCloser) Close() error {
	return multiCloser(rc.close).Close()
}

func (rc *mergeReadCloser) advance(i int) error {
	if rc.ok[i] = rc.scanner[i].Scan(); rc.ok[i] {
		rc.record[i] = rc.scanner[i].Bytes()
		if len(rc.record[i]) < ulid.EncodedSize {
			panic("record is too short")
		}
		rc.id[i] = rc.record[i][:ulid.EncodedSize]
	} else if err := rc.scanner[i].Err(); err != nil && err != io.EOF {
		return err
	}
	return nil
}

type readCloser struct {
	io.Reader
	io.Closer
}

func newMultiReadCloser(rc ...io.ReadCloser) io.ReadCloser {
	var (
		r = make([]io.Reader, len(rc))
		c = make([]io.Closer, len(rc))
	)
	for i := range rc {
		r[i] = rc[i]
		c[i] = rc[i]
	}
	return readCloser{
		Reader: io.MultiReader(r...),
		Closer: multiCloser(c),
	}
}

// multiCloser closes all underlying io.Closers.
// If an error is encountered, closings continue.
type multiCloser []io.Closer

func (c multiCloser) Close() error {
	var errs []error
	for _, closer := range c {
		if closer == nil {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return multiCloseError(errs)
	}
	return nil
}

type multiCloseError []error

func (e multiCloseError) Error() string {
	a := make([]string, len(e))
	for i, err := range e {
		a[i] = err.Error()
	}
	return strings.Join(a, "; ")
}

// Like bufio.ScanLines, but retain the \n.
func scanLinesPreserveNewline(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[0 : i+1], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

type recordFilter func([]byte) bool

func max(a, b string) string {
	if a > b {
		return a
	}
	return b
}
