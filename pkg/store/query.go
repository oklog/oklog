package store

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio"
	"github.com/pkg/errors"

	"github.com/oklog/prototype/pkg/fs"
	"github.com/oklog/ulid"
)

// QueryResult contains statistics about, and matching records for, a query.
type QueryResult struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Q     string `json:"q"`
	Regex bool   `json:"regex"`

	NodesQueried    int    `json:"nodes_queried"`
	SegmentsQueried int    `json:"segments_queried"`
	MaxDataSetSize  int64  `json:"max_data_set_size"`
	ErrorCount      int    `json:"error_count,omitempty"`
	Duration        string `json:"duration"`

	Records io.ReadCloser // TODO(pb): audit to ensure closing is valid throughout
}

// EncodeTo encodes the QueryResult to the HTTP response writer.
func (qr *QueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderFrom, qr.From)
	w.Header().Set(httpHeaderTo, qr.To)
	w.Header().Set(httpHeaderQ, qr.Q)
	w.Header().Set(httpHeaderRegex, fmt.Sprint(qr.Regex))

	w.Header().Set(httpHeaderNodesQueried, strconv.Itoa(qr.NodesQueried))
	w.Header().Set(httpHeaderSegmentsQueried, strconv.Itoa(qr.SegmentsQueried))
	w.Header().Set(httpHeaderMaxDataSetSize, strconv.FormatInt(qr.MaxDataSetSize, 10))
	w.Header().Set(httpHeaderErrorCount, strconv.Itoa(qr.ErrorCount))
	w.Header().Set(httpHeaderDuration, qr.Duration)

	if qr.Records != nil {
		io.Copy(w, qr.Records) // TODO(pb): CopyBuffer
		qr.Records.Close()
	}
}

// DecodeFrom decodes the QueryResult from the HTTP response.
func (qr *QueryResult) DecodeFrom(resp *http.Response) {
	qr.From = resp.Header.Get(httpHeaderFrom)
	qr.To = resp.Header.Get(httpHeaderTo)
	qr.Q = resp.Header.Get(httpHeaderQ)
	qr.Regex, _ = strconv.ParseBool(resp.Header.Get(httpHeaderRegex))
	qr.NodesQueried, _ = strconv.Atoi(resp.Header.Get(httpHeaderNodesQueried))
	qr.SegmentsQueried, _ = strconv.Atoi(resp.Header.Get(httpHeaderSegmentsQueried))
	qr.MaxDataSetSize, _ = strconv.ParseInt(resp.Header.Get(httpHeaderMaxDataSetSize), 10, 64)
	qr.ErrorCount, _ = strconv.Atoi(resp.Header.Get(httpHeaderErrorCount))
	qr.Duration = resp.Header.Get(httpHeaderDuration)
	qr.Records = resp.Body
}

// Merge the other QueryResult into this one.
func (qr *QueryResult) Merge(other QueryResult) error {
	qr.NodesQueried += other.NodesQueried
	qr.SegmentsQueried += other.SegmentsQueried
	if other.MaxDataSetSize > qr.MaxDataSetSize {
		qr.MaxDataSetSize = other.MaxDataSetSize
	}
	qr.ErrorCount += other.ErrorCount

	var (
		buf bytes.Buffer
		err error
	)
	switch {
	case qr.Records != nil && other.Records == nil:
		defer qr.Records.Close()
		_, _, _, err = mergeRecords(&buf, qr.Records)
	case qr.Records == nil && other.Records != nil:
		defer other.Records.Close()
		_, _, _, err = mergeRecords(&buf, other.Records)
	case qr.Records != nil && other.Records != nil:
		defer qr.Records.Close()
		defer other.Records.Close()
		_, _, _, err = mergeRecords(&buf, qr.Records, other.Records)
	}
	qr.Records = ioutil.NopCloser(&buf)
	return err
}

const (
	httpHeaderFrom            = "X-Oklog-From"
	httpHeaderTo              = "X-Oklog-To"
	httpHeaderQ               = "X-Oklog-Q"
	httpHeaderRegex           = "X-Oklog-Regex"
	httpHeaderNodesQueried    = "X-Oklog-Nodes-Queried"
	httpHeaderSegmentsQueried = "X-Oklog-Segments-Queried"
	httpHeaderMaxDataSetSize  = "X-Oklog-Max-Data-Set-Size"
	httpHeaderErrorCount      = "X-Oklog-Error-Count"
	httpHeaderDuration        = "X-Oklog-Duration"
)

type recordFilter func([]byte) bool

// newQueryReader converts a batch of segment files to a single io.Reader.
// Records are yielded in time order, oldest first, hopefully efficiently!
// Only records passing the recordFilter are yielded.
func newQueryReader(fs fs.Filesystem, segments []string, pass recordFilter) (r io.Reader, sz int64, err error) {
	// Batch the segments, and construct a reader for each batch.
	var readers []io.Reader
	for _, batch := range batchSegments(segments) {
		switch len(batch) {
		case 0:
			continue // weird

		case 1:
			// A batch of one can be read straight thru.
			f, err := fs.Open(batch[0])
			if err != nil {
				return nil, sz, err // TODO(pb): don't leak FDs
			}
			readers = append(readers, newConcurrentFilteringReader(f, pass))
			sz += f.Size()

		default:
			// A batch of N requires a K-way merge.
			readers, batchsz, err := makeConcurrentFilteringReaders(fs, batch, pass)
			if err != nil {
				return nil, sz, err // TODO(pb): don't leak FDs
			}
			mr, err := newMergeReader(readers)
			if err != nil {
				return nil, sz, err // TODO(pb): don't leak FDs
			}
			readers = append(readers, mr)
			sz += batchsz
		}
	}

	// The MultiReader drains each reader in sequence.
	return io.MultiReader(readers...), sz, nil
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

func makeConcurrentFilteringReaders(fs fs.Filesystem, segments []string, pass recordFilter) (readers []io.Reader, sz int64, err error) {
	readers = make([]io.Reader, len(segments))
	for i := range segments {
		f, err := fs.Open(segments[i])
		if err != nil {
			return nil, sz, err // TODO(pb): don't leak FDs
		}
		readers[i] = newConcurrentFilteringReader(f, pass)
		sz += f.Size()
	}
	return readers, sz, nil
}

func newConcurrentFilteringReader(src io.Reader, pass recordFilter) io.Reader {
	r, w := nio.Pipe(buffer.New(1024 * 1024))
	//r, w := io.Pipe()
	go func() {
		br := bufio.NewReader(src)
		for {
			line, err := br.ReadSlice('\n')
			if err != nil {
				w.CloseWithError(err)
				return
			}
			if !pass(line) {
				continue
			}
			if n, err := w.Write(line); err != nil {
				w.CloseWithError(err)
				return
			} else if n < len(line) {
				w.CloseWithError(io.ErrShortWrite)
				return
			}
		}
	}()
	return r
}

// mergeReader performs a K-way merge from multiple readers.
// TODO(pb): the readers need to be closed; wire that thru
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
	const (
		scanBufferSize   = 64 * 1024      // 64KB
		scanMaxTokenSize = scanBufferSize // if equal, no allocs
	)
	for i := 0; i < len(readers); i++ {
		r.scanner[i] = bufio.NewScanner(readers[i])
		r.scanner[i].Buffer(make([]byte, scanBufferSize), scanMaxTokenSize)
		if err := r.advance(i); err != nil {
			return nil, err
		}
	}

	// Ready to read.
	return r, nil
}

func (r *mergeReader) Read(p []byte) (int, error) {
	// Pick the source with the smallest ID.
	// TODO(pb): could be improved with an e.g. tournament tree
	smallest := -1 // index
	for i := range r.id {
		if !r.ok[i] {
			continue // already drained
		}
		if smallest < 0 || bytes.Compare(r.id[i], r.id[smallest]) < 0 {
			smallest = i
		}
	}
	if smallest < 0 {
		return 0, io.EOF // everything is drained
	}

	// Copy the record over.
	src := append(r.record[smallest], '\n')
	n := copy(p, src)
	if n < len(src) {
		panic("short read!") // TODO(pb): obviously needs fixing
	}

	// Advance the chosen source.
	if err := r.advance(smallest); err != nil {
		return n, errors.Wrapf(err, "advancing reader %d", smallest)
	}

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
