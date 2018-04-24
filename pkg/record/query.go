package record

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio"
	"github.com/oklog/oklog/pkg/event"
	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// QueryParams defines all dimensions of a query.
// StatsOnly is implicit by the HTTP method.
type QueryParams struct {
	Topic string     `json:"topic"`
	From  ULIDOrTime `json:"from"`
	To    ULIDOrTime `json:"to"`
	Q     string     `json:"q"`
	Regex bool       `json:"regex"`
}

// DecodeFrom populates a QueryParams from a URL.
func (qp *QueryParams) DecodeFrom(u *url.URL, rb RangeBehavior) error {
	if err := qp.From.Parse(u.Query().Get("from")); err != nil && rb == RangeRequired {
		return errors.Wrap(err, "parsing 'from'")
	}
	if err := qp.To.Parse(u.Query().Get("to")); err != nil && rb == RangeRequired {
		return errors.Wrap(err, "parsing 'to'")
	}
	qp.Topic = u.Query().Get("topic")
	qp.Q = u.Query().Get("q")
	_, qp.Regex = u.Query()["regex"]

	if qp.Regex {
		if _, err := regexp.Compile(qp.Q); err != nil {
			return errors.Wrap(err, "compiling regex")
		}
	}

	return nil
}

type RangeBehavior int

const (
	RangeRequired RangeBehavior = iota
	RangeNotRequired
)

// ULIDOrTime is how we interpret the From and To query params.
// Users may specify a valid ULID, or an RFC3339Nano timestamp.
// We prefer them in that order, and cross-populate the fields.
type ULIDOrTime struct {
	ulid.ULID
	time.Time
}

// Parse a string, likely taken from a query param.
func (ut *ULIDOrTime) Parse(s string) error {
	if id, err := ulid.Parse(s); err == nil {
		ut.ULID = id
		var (
			msec = id.Time()
			sec  = int64(msec / 1000)
			nsec = int64(msec%1000) * 1000000
		)
		ut.Time = time.Unix(sec, nsec).UTC()
		return nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		ut.ULID.SetEntropy(nil)
		// Pass t.UTC to mirror ulid.Now, which does the same.
		// (We use ulid.Now when generating ULIDs in the ingester.)
		ut.ULID.SetTime(ulid.Timestamp(t.UTC()))
		ut.Time = t
		return nil
	}
	return errors.Errorf("%s: can't parse as ULID or RFC3339 time", s)
}

// QueryResult contains statistics about, and matching records for, a query.
type QueryResult struct {
	Params QueryParams `json:"query"`

	NodesQueried    int    `json:"nodes_queried"`
	SegmentsQueried int    `json:"segments_queried"`
	MaxDataSetSize  int64  `json:"max_data_set_size"`
	ErrorCount      int    `json:"error_count,omitempty"`
	Duration        string `json:"duration"`

	Records io.ReadCloser // TODO(pb): audit to ensure closing is valid throughout
}

// EncodeTo encodes the QueryResult to the HTTP response writer.
// It also closes the records ReadCloser.
func (qr *QueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderFrom, qr.Params.From.Format(time.RFC3339))
	w.Header().Set(httpHeaderTo, qr.Params.To.Format(time.RFC3339))
	w.Header().Set(httpHeaderQ, qr.Params.Q)
	w.Header().Set(httpHeaderRegex, fmt.Sprint(qr.Params.Regex))

	w.Header().Set(httpHeaderNodesQueried, strconv.Itoa(qr.NodesQueried))
	w.Header().Set(httpHeaderSegmentsQueried, strconv.Itoa(qr.SegmentsQueried))
	w.Header().Set(httpHeaderMaxDataSetSize, strconv.FormatInt(qr.MaxDataSetSize, 10))
	w.Header().Set(httpHeaderErrorCount, strconv.Itoa(qr.ErrorCount))
	w.Header().Set(httpHeaderDuration, qr.Duration)

	if qr.ErrorCount > 0 {
		w.WriteHeader(http.StatusPartialContent)
	}

	if qr.Records != nil {
		// CopyBuffer can be useful for complex query pipelines.
		// TODO(pb): validate the 1MB buffer size with profiling
		buf := make([]byte, 1024*1024)
		io.CopyBuffer(w, qr.Records, buf)
		qr.Records.Close()
	}
}

// DecodeFrom decodes the QueryResult from the HTTP response.
func (qr *QueryResult) DecodeFrom(resp *http.Response) error {
	var err error
	if err = qr.Params.From.Parse(resp.Header.Get(httpHeaderFrom)); err != nil {
		return errors.Wrap(err, "from")
	}
	if err = qr.Params.To.Parse(resp.Header.Get(httpHeaderTo)); err != nil {
		return errors.Wrap(err, "to")
	}
	qr.Params.Q = resp.Header.Get(httpHeaderQ)
	if qr.Params.Regex, err = strconv.ParseBool(resp.Header.Get(httpHeaderRegex)); err != nil {
		return errors.Wrap(err, "regex")
	}
	if qr.NodesQueried, err = strconv.Atoi(resp.Header.Get(httpHeaderNodesQueried)); err != nil {
		return errors.Wrap(err, "nodes queried")
	}
	if qr.SegmentsQueried, err = strconv.Atoi(resp.Header.Get(httpHeaderSegmentsQueried)); err != nil {
		return errors.Wrap(err, "segments queried")
	}
	if qr.MaxDataSetSize, err = strconv.ParseInt(resp.Header.Get(httpHeaderMaxDataSetSize), 10, 64); err != nil {
		return errors.Wrap(err, "max data set size")
	}
	if qr.ErrorCount, err = strconv.Atoi(resp.Header.Get(httpHeaderErrorCount)); err != nil {
		return errors.Wrap(err, "error count")
	}
	qr.Duration = resp.Header.Get(httpHeaderDuration)
	qr.Records = resp.Body
	return nil
}

// Merge the other QueryResult into this one.
func (qr *QueryResult) Merge(other QueryResult) error {
	// Union the simple integer types.
	qr.NodesQueried += other.NodesQueried
	qr.SegmentsQueried += other.SegmentsQueried
	if other.MaxDataSetSize > qr.MaxDataSetSize {
		qr.MaxDataSetSize = other.MaxDataSetSize
	}
	qr.ErrorCount += other.ErrorCount

	// Merge the record readers.
	// Both mergeRecords and multiCloser can handle nils.
	var buf bytes.Buffer
	_, _, _, err := MergeRecords(&buf, qr.Records, other.Records)
	multiCloser{qr.Records, other.Records}.Close()
	qr.Records = ioutil.NopCloser(&buf)

	// Done.
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

// ErrShortRead is returned when a read is unexpectedly shortened.
var ErrShortRead = errors.New("short read")

// MergeRecords takes a set of io.Readers that contain ULID-prefixed records in
// individually-sorted order, and writes the globally-sorted output to the
// io.Writer.
func MergeRecords(w io.Writer, readers ...io.Reader) (low, high ulid.ULID, n int64, err error) {
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

// QuerySegment models a segment file on disk.
type QuerySegment struct {
	Path   string // ULID-ULID.extension
	Size   int64
	Reader ReadCloser
}

// NewQueryReadCloser converts a batch of segments to a single io.ReadCloser.
// Records are yielded in time order, oldest first, hopefully efficiently!
// Only records passing the recordFilter are yielded.
// The sz of the segment files can be used as a proxy for read effort.
func NewQueryReadCloser(fs fs.Filesystem, segments []QuerySegment, pass Filter, bufsz int64, reporter event.Reporter) (rc io.ReadCloser, sz int64, err error) {
	// We will build successive ReadClosers for each batch.
	var rcs []io.ReadCloser

	// Don't leak FDs on error.
	defer func() {
		if err != nil {
			for i, rc := range rcs {
				if cerr := rc.Close(); cerr != nil {
					reporter.ReportEvent(event.Event{
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
			sz += batch[0].Size
			rcs = append(rcs, newConcurrentFilteringReadCloser(batch[0].Reader, pass, bufsz))

		default:
			// A batch of N requires a K-way merge.
			cfrcs, batchsz, err := makeConcurrentFilteringReadClosers(fs, batch, pass, bufsz)
			if err != nil {
				return nil, sz, err
			}
			mrc, err := NewMergeReadCloser(cfrcs)
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
func batchSegments(segments []QuerySegment) [][]QuerySegment {
	// First, parse ranges from filename.
	// TODO(pb): handle weird filenames better, somehow
	type lexrange struct{ a, b string }
	ranges := make([]lexrange, len(segments))
	for i := range segments {
		f := strings.SplitN(basename(segments[i].Path), "-", 2)
		ranges[i] = lexrange{f[0], f[1]}
	}

	// Now, walk the segments.
	var (
		result = [][]QuerySegment{} // non-nil
		group  []QuerySegment       // that we're currently building
		b      string               // end ULID of the group
	)
	for i := range segments {
		switch {
		case len(group) <= 0:
			// If the group is empty, it gets the segment.
			group = []QuerySegment{segments[i]}
			b = ranges[i].b

		case ranges[i].a > b:
			// If the current segment doesn't overlap with the group,
			// the group is closed and we start a new group.
			result = append(result, group)
			group = []QuerySegment{segments[i]}
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

func makeConcurrentFilteringReadClosers(fs fs.Filesystem, segments []QuerySegment, pass Filter, bufsz int64) (rcs []io.ReadCloser, sz int64, err error) {
	rcs = make([]io.ReadCloser, len(segments))
	for i := range segments {
		sz += segments[i].Size
		rcs[i] = newConcurrentFilteringReadCloser(segments[i].Reader, pass, bufsz)
	}
	return rcs, sz, nil
}

func newConcurrentFilteringReadCloser(src ReadCloser, pass Filter, bufsz int64) io.ReadCloser {
	r, w := nio.Pipe(buffer.New(bufsz))
	go func() {
		defer src.Close() // close the fs.File when we're done reading

		// TODO(pb): need to benchmark

		for {
			line, err := src.Read()
			if err == io.EOF {
				w.Close()
				break
			}
			if err != nil {
				w.CloseWithError(err)
				break
			}
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
	}()
	return r
}

// mergeReadCloser performs a K-way merge from multiple readers.
type mergeReadCloser struct {
	close   []io.Closer
	scanner []*bufio.Scanner
	ok      []bool
	record  [][]byte
	id      [][]byte
}

func NewMergeReadCloser(rcs []io.ReadCloser) (io.ReadCloser, error) {
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

func basename(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(path)
	return base[:len(base)-len(ext)]
}

func max(a, b string) string {
	if a > b {
		return a
	}
	return b
}
