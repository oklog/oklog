package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"

	"github.com/oklog/oklog/pkg/fs"
)

// QueryParams defines all dimensions of a query.
type QueryParams struct {
	From  time.Time `json:"from"`
	To    time.Time `json:"to"`
	Q     string    `json:"q"`
	Regex bool      `json:"regex"`
}

// DecodeFrom populates a QueryParams from a URL.
func (qp *QueryParams) DecodeFrom(u *url.URL) error {
	from, err := time.Parse(time.RFC3339Nano, u.Query().Get("from"))
	if err != nil {
		return errors.Wrap(err, "parsing 'from'")
	}
	to, err := time.Parse(time.RFC3339Nano, u.Query().Get("to"))
	if err != nil {
		return errors.Wrap(err, "parsing 'to'")
	}
	qp.From = from
	qp.To = to
	qp.Q = u.Query().Get("q")
	_, qp.Regex = u.Query()["regex"]
	return nil
}

// EncodeTo encodes the QueryParams to the url.Values.
func (qp *QueryParams) EncodeTo(u *url.URL) {
	values := url.Values{}
	values.Set("from", qp.From.Format(time.RFC3339Nano))
	values.Set("to", qp.To.Format(time.RFC3339Nano))
	values.Set("q", qp.Q)
	if qp.Regex {
		values.Set("regex", "true")
	}
	u.RawQuery = values.Encode()
}

// UnmarshalJSON implements json.Unmarshaler.
// It parses the times as RFC3339Nano timestamps.
func (qp *QueryParams) UnmarshalJSON(data []byte) error {
	var intermediary struct {
		From  string `json:"from"`
		To    string `json:"to"`
		Q     string `json:"q"`
		Regex bool   `json:"regex"`
	}
	if err := json.Unmarshal(data, &intermediary); err != nil {
		return err
	}
	from, err := time.Parse(time.RFC3339Nano, intermediary.From)
	if err != nil {
		return errors.Wrap(err, "parsing 'from'")
	}
	to, err := time.Parse(time.RFC3339Nano, intermediary.To)
	if err != nil {
		return errors.Wrap(err, "parsing 'to'")
	}
	qp.From = from
	qp.To = to
	qp.Q = intermediary.Q
	qp.Regex = intermediary.Regex
	return nil
}

// MarshalJSON implements json.Marshaler.
// It marshals the times as RFC3339Nano timestamps.
func (qp *QueryParams) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		From  string `json:"from"`
		To    string `json:"to"`
		Q     string `json:"q"`
		Regex bool   `json:"regex"`
	}{
		From:  qp.From.Format(time.RFC3339Nano),
		To:    qp.To.Format(time.RFC3339Nano),
		Q:     qp.Q,
		Regex: qp.Regex,
	})
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
	if qr.Params.From, err = time.Parse(time.RFC3339Nano, resp.Header.Get(httpHeaderFrom)); err != nil {
		return errors.Wrap(err, "from")
	}
	if qr.Params.To, err = time.Parse(time.RFC3339Nano, resp.Header.Get(httpHeaderTo)); err != nil {
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
	_, _, _, err := mergeRecords(&buf, qr.Records, other.Records)
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

type recordFilter func([]byte) bool

// newQueryReadCloser converts a batch of segments to a single io.ReadCloser.
// Records are yielded in time order, oldest first, hopefully efficiently!
// Only records passing the recordFilter are yielded.
// The sz of the segment files can be used as a proxy for read effort.
func newQueryReadCloser(fs fs.Filesystem, segments []string, pass recordFilter, bufsz int64) (rc io.ReadCloser, sz int64, err error) {
	// We will build successive ReadClosers for each batch.
	var rcs []io.ReadCloser

	// Don't leak FDs on error.
	defer func() {
		if err != nil {
			for _, rc := range rcs {
				rc.Close()
			}
		}
	}()

	// Batch the segments, and construct a ReadCloser for each batch.
	for _, batch := range batchSegments(segments) {
		switch len(batch) {
		case 0:
			continue // weird

		case 1:
			// A batch of one can be read straight thru.
			f, err := fs.Open(batch[0])
			if err != nil {
				return nil, sz, err
			}
			rcs = append(rcs, newConcurrentFilteringReadCloser(f, pass, bufsz))
			sz += f.Size()

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
func batchSegments(segments []string) [][]string {
	// First, parse ranges from filename.
	// TODO(pb): handle weird filenames better, somehow
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

func makeConcurrentFilteringReadClosers(fs fs.Filesystem, segments []string, pass recordFilter, bufsz int64) (rcs []io.ReadCloser, sz int64, err error) {
	// Don't leak FDs on error.
	defer func() {
		if err != nil {
			for _, rc := range rcs {
				rc.Close()
			}
			rcs = nil
		}
	}()

	for _, segment := range segments {
		f, err := fs.Open(segment)
		if err != nil {
			return rcs, sz, err
		}
		rcs = append(rcs, newConcurrentFilteringReadCloser(f, pass, bufsz))
		sz += f.Size()
	}

	return rcs, sz, nil
}

func newConcurrentFilteringReadCloser(src io.ReadCloser, pass recordFilter, bufsz int64) io.ReadCloser {
	r, w := nio.Pipe(buffer.New(bufsz))
	go func() {
		defer src.Close() // close the fs.File when we're done reading

		// ReadSlice will abort with ErrBufferFull if a single line exceeds the
		// Reader's buffer. We pass bufsz as a quick fix here. An actual, robust
		// solution would use a dynamic buffer but avoid allocating. Perhaps
		// Scanner/Bytes would suffice, but we'd need to confirm with a
		// benchmark.
		br := bufio.NewReaderSize(src, int(bufsz))

		for {
			line, err := br.ReadSlice('\n')
			if err != nil {
				w.CloseWithError(err)
				return
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
	src := append(rc.record[smallest], '\n')
	n := copy(p, src)
	if n < len(src) {
		panic("short read!") // TODO(pb): obviously needs fixing
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
