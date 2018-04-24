package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/go-kit/kit/log"
	"github.com/google/codesearch/index"
	csre "github.com/google/codesearch/regexp"
	"github.com/google/codesearch/sparse"
	"github.com/oklog/oklog/pkg/event"
	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/oklog/pkg/record"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

const (
	extActive  = ".active"
	extFlushed = ".flushed"
	extReading = ".reading" // compacting or trashing
	extTrashed = ".trashed"
	extIndex   = ".index"

	ulidTimeSize = 10 // bytes
)

var (
	ulidMaxEntropy = []byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	}
)

type Indexer struct {
	reporter  event.Reporter
	fsys      fs.Filesystem
	path      string
	watchPath string
}

func NewIndex(fsys fs.Filesystem, path, watchPath string, reporter event.Reporter) (*Indexer, error) {
	if err := fsys.MkdirAll(path); err != nil {
		return nil, err
	}
	if reporter == nil {
		reporter = event.LogReporter{Logger: log.NewNopLogger()}
	}
	return &Indexer{
		reporter:  reporter,
		fsys:      fsys,
		path:      path,
		watchPath: watchPath,
	}, nil
}

type indexSegmentReader struct {
	path    string
	segment *os.File
	iter    roaring.IntIterable
	index   *Reader
}

func (r *indexSegmentReader) Read() ([]byte, error) {
	if !r.iter.HasNext() {
		return nil, io.EOF
	}
	off, length, err := r.index.Record(r.iter.Next())
	if err != nil {
		return nil, err
	}
	b := make([]byte, length)
	if _, err := r.segment.ReadAt(b, int64(off)); err != nil {
		return nil, err
	}
	return b, nil
}

func (r *indexSegmentReader) Close() error {
	r.index.Close()
	return r.segment.Close()
}

// queryMatchingSegments returns a sorted slice of all segment files that could
// possibly have records in the provided time range. The caller is responsible
// for closing the segments.
func (ix *Indexer) queryMatchingSegments(from, to ulid.ULID, iq *index.Query) (segments []record.QuerySegment) {
	ix.fsys.Walk(ix.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		// We should query .reading segments, too.
		// Better to get duplicates than miss records.
		if ext := filepath.Ext(path); ext != extIndex {
			return nil // skip
		}
		low, high, err := parseFilename(path)
		if err != nil {
			ix.reporter.ReportEvent(event.Event{
				Op: "queryMatchingSegments", File: path, Warning: err,
				Msg: fmt.Sprintf("will remove apparently-bad data file of size %d", info.Size()),
			})
			if err := ix.fsys.Remove(path); err != nil {
				ix.reporter.ReportEvent(event.Event{
					Op: "queryMatchingSegments", File: path, Warning: err,
					Msg: "tried to remove apparently-bad data file, which failed",
				})
			}
			return nil // weird; skip
		}
		if !overlap(from, to, low, high) {
			return nil
		}
		switch err {
		case nil:
			ir, err := NewReader(path)
			if err != nil {
				return err
			}
			bm, err := ir.Query(iq)
			if err != nil {
				return err
			}

			sf, err := os.Open(modifyExtension(path, extFlushed))
			if err != nil {
				return err
			}
			info, err := sf.Stat()
			if err != nil {
				return err
			}
			segments = append(segments, record.QuerySegment{
				Path: modifyExtension(path, extFlushed),
				Reader: &indexSegmentReader{
					path:    modifyExtension(path, extFlushed),
					segment: sf,
					iter:    bm.Iterator(),
					index:   ir,
				},
				Size: info.Size(),
			})
		case os.ErrNotExist:
			ix.reporter.ReportEvent(event.Event{
				Op: "queryMatchingSegments", File: path, Warning: err,
				Msg: "this can happen due to e.g. compaction",
			})
		default:
			ix.reporter.ReportEvent(event.Event{
				Op: "queryMatchingSegments", File: path, Error: err,
			})
		}
		return nil
	})
	sort.Slice(segments, func(i, j int) bool {
		a := strings.SplitN(basename(segments[i].Path), "-", 2)[0]
		b := strings.SplitN(basename(segments[j].Path), "-", 2)[0]
		return a < b
	})
	return segments
}

// Query written and closed segments.
func (ix *Indexer) Query(qp record.QueryParams, statsOnly bool) (record.QueryResult, error) {
	// TODO(fabxc): enforce regex queries for now. We need to escape the regex
	// for trigram queries or build our own trigram query for equal matches.
	qp.Regex = true

	re, err := csre.Compile(qp.Q)
	if err != nil {
		return record.QueryResult{}, errors.Wrap(err, "compile regex")
	}
	var (
		begin    = time.Now()
		segments = ix.queryMatchingSegments(qp.From.ULID, qp.To.ULID, index.RegexpQuery(re.Syntax))
		pass     = record.FilterBoundedPlain(qp.From.ULID, qp.To.ULID, []byte(qp.Q))
	)
	if qp.Regex {
		pass = record.FilterBoundedRegex(qp.From.ULID, qp.To.ULID, regexp.MustCompile(qp.Q))
	}

	// Time range should be inclusive, so we need a max value here.
	if err := qp.To.ULID.SetEntropy(ulidMaxEntropy); err != nil {
		panic(err)
	}

	// Build the lazy reader.
	rc, sz, err := record.NewQueryReadCloser(ix.fsys, segments, pass, 1024*1024, ix.reporter)
	if err != nil {
		return record.QueryResult{}, errors.Wrap(err, "constructing the lazy reader")
	}
	if statsOnly {
		rc = ioutil.NopCloser(bytes.NewReader(nil))
	}

	return record.QueryResult{
		Params: qp,

		NodesQueried:    1,
		SegmentsQueried: len(segments),
		MaxDataSetSize:  sz,
		ErrorCount:      0,
		Duration:        time.Since(begin).String(),

		Records: rc,
	}, nil
}

// indices returns a set of indices that currently exist.
func (ix *Indexer) indices() (map[string]struct{}, error) {
	cur, err := ix.fsys.ReadDir(ix.path)
	if err != nil {
		return nil, err
	}
	res := make(map[string]struct{}, len(cur)/2)

	for _, fi := range cur {
		if filepath.Ext(fi.Name()) != extIndex {
			continue
		}
		res[fi.Name()] = struct{}{}
	}
	return res, nil
}

// Sync adds and deletes indices based on which segments are found in the watch directory.
func (ix *Indexer) Sync() error {
	indices, err := ix.indices()
	if err != nil {
		return err
	}
	fis, err := ix.fsys.ReadDir(ix.watchPath)
	if err != nil {
		return err
	}
	add := map[string]struct{}{}
	del := map[string]struct{}{}
	all := map[string]struct{}{}

	for _, fi := range fis {
		if filepath.Ext(fi.Name()) != extFlushed {
			continue
		}
		if _, ok := indices[modifyExtension(fi.Name(), extIndex)]; !ok {
			add[fi.Name()] = struct{}{}
		}
		all[fi.Name()] = struct{}{}
	}
	for s := range indices {
		if _, ok := all[modifyExtension(s, extFlushed)]; !ok {
			del[s] = struct{}{}
		}
	}

	// All new segments must be indexed before deleting old ones. Otherwise records
	// may get temporarily unavailable when the store compacts them.
	w := NewWriter()
	for s := range add {
		if err := ix.index(w, s); err != nil {
			ix.reporter.ReportEvent(event.Event{
				Op: "Sync", File: s, Warning: err,
				Msg: "index new segment",
			})
		}
	}
	for s := range del {
		if err := ix.drop(s); err != nil {
			ix.reporter.ReportEvent(event.Event{
				Op: "Sync", File: s, Warning: err,
				Msg: "drop index for remove segment",
			})
		}
	}
	return nil
}

func (ix *Indexer) index(w *Writer, s string) error {
	var (
		from = filepath.Join(ix.watchPath, s)
		to   = filepath.Join(ix.path, s)
	)
	// We hard link the segment before indexing it. This prevents losing access
	// when the store drops it.
	if err := ix.fsys.Link(from, to); err != nil {
		return err
	}

	f, err := os.Open(to)
	if err != nil {
		return err
	}
	defer f.Close()

	r := record.NewReader(f)

	out, err := os.Create(modifyExtension(to, extIndex))
	if err != nil {
		return err
	}
	defer out.Close()
	w.Reset(out)

	offset := uint32(0)
	for i := 0; ; i++ {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if i > math.MaxUint32 {
			return errors.New("too many records")
		}
		// Index the record without the ULID.
		w.Add(offset, uint32(len(rec)), rec[ulid.EncodedSize+1:])
		offset += uint32(len(rec))
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}

func (ix *Indexer) drop(s string) error {
	fmt.Println("drop", s)
	// Delete hardlink of segment and its index file.
	if err := ix.fsys.Remove(filepath.Join(ix.path, s)); err != nil {
		return err
	}
	return ix.fsys.Remove(filepath.Join(ix.path, modifyExtension(s, extFlushed)))
}

type offset struct {
	key uint32
	off uint32
	len uint32
}

type toc struct {
	records     uint32
	numRecords  uint32
	trigrams    uint32
	numTrigrams uint32
}

type Writer struct {
	file         *os.File
	buf          *bufio.Writer
	n            uint32
	toc          toc
	trigram      *sparse.Set // trigrams for the current file
	recOffs      []offset
	postingsOffs []offset
	postings     map[uint32]*roaring.Bitmap
}

func NewWriter() *Writer {
	return &Writer{
		trigram:  sparse.NewSet(1 << 24),
		postings: map[uint32]*roaring.Bitmap{},
		buf:      bufio.NewWriterSize(nil, 1024*1024),
	}
}

// Reset the writer to a new file.
func (w *Writer) Reset(f *os.File) error {
	for k := range w.postings {
		delete(w.postings, k)
	}
	w.recOffs = w.recOffs[:0]
	w.postingsOffs = w.postingsOffs[:0]
	w.trigram.Reset()
	w.file = f
	w.buf.Reset(f)
	return nil
}

func (w *Writer) write(b []byte) error {
	n, err := w.buf.Write(b)
	w.n += uint32(n)
	return err
}

// Add a record at the offset and length to the index.
func (w *Writer) Add(off, length uint32, b []byte) {
	w.trigram.Reset()
	tv := uint32(0)

	for i, c := range b {
		// Shift trigram 1 byte to the left and add current character.
		tv = (tv << 8) & (1<<24 - 1)
		tv |= uint32(c)

		if i >= 3 {
			w.trigram.Add(tv)
		}
	}

	id := uint32(len(w.recOffs))
	w.recOffs = append(w.recOffs, offset{off: off, len: length})

	// add record to hash table with id.
	for _, t := range w.trigram.Dense() {
		bm, ok := w.postings[t]
		if !ok {
			bm = roaring.New()
			w.postings[t] = bm
		}
		bm.Add(id)
	}
}

// Flush all writer state to the index file. Must be called exactly once after
// all entries were added.
func (w *Writer) Flush() error {
	if err := w.writeRecordOffsets(); err != nil {
		return err
	}
	if err := w.writePostings(); err != nil {
		return err
	}
	if err := w.writePostingsOffests(); err != nil {
		return err
	}
	if err := w.writeTrailer(); err != nil {
		return err
	}
	return w.buf.Flush()
}

func (w *Writer) writePostings() error {
	// Sort trigrams first to make output deterministic.
	type entry struct {
		t uint32
		p *roaring.Bitmap
	}
	var entries []entry

	for t, p := range w.postings {
		entries = append(entries, entry{t: t, p: p})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].t < entries[j].t
	})

	for _, e := range entries {
		e.p.RunOptimize()
		b, err := e.p.MarshalBinary()
		if err != nil {
			return err
		}
		w.postingsOffs = append(w.postingsOffs, offset{key: e.t, off: w.n, len: uint32(len(b))})
		if err := w.write(b); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeRecordOffsets() error {
	var buf [4]byte
	w.toc.records = w.n

	for _, o := range w.recOffs {
		binary.BigEndian.PutUint32(buf[:], o.off)
		if err := w.write(buf[:]); err != nil {
			return err
		}
	}
	// Write one final offset. It doesn't actually point to a record but the end of the
	// segment. This way we can infer the length of each record from the index.
	last := w.recOffs[len(w.recOffs)-1]
	binary.BigEndian.PutUint32(buf[:], last.off+last.len)
	return w.write(buf[:])
}

func (w *Writer) writePostingsOffests() error {
	var buf [postingEntrySize]byte
	w.toc.trigrams = w.n

	for _, e := range w.postingsOffs {
		binary.BigEndian.PutUint32(buf[:4], e.key)
		binary.BigEndian.PutUint32(buf[4:8], e.off)
		binary.BigEndian.PutUint32(buf[8:], uint32(e.len))
		if err := w.write(buf[:]); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeTrailer() error {
	var buf [16]byte
	binary.BigEndian.PutUint32(buf[:4], w.toc.records)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(w.recOffs)))
	binary.BigEndian.PutUint32(buf[8:12], w.toc.trigrams)
	binary.BigEndian.PutUint32(buf[12:], uint32(len(w.postings)))
	return w.write(buf[:])
}

// Reader provides read access to an index file.
type Reader struct {
	file *os.File
	toc  toc
}

// NewReader returns a new index reader against the given file.
func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &Reader{file: f}
	return r, r.initTOC()
}

// Close the readers underlying resources.
func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) initTOC() error {
	if _, err := r.file.Seek(-16, os.SEEK_END); err != nil {
		return err
	}
	var b [16]byte
	if _, err := r.file.Read(b[:]); err != nil {
		return err
	}
	r.toc.records = binary.BigEndian.Uint32(b[:4])
	r.toc.numRecords = binary.BigEndian.Uint32(b[4:8])
	r.toc.trigrams = binary.BigEndian.Uint32(b[8:12])
	r.toc.numTrigrams = binary.BigEndian.Uint32(b[12:])
	return nil
}

// Query returns a list of records that match the given trigram query.
func (r *Reader) Query(q *index.Query) (*roaring.Bitmap, error) {
	return r.query(q, nil)
}

func (r *Reader) query(q *index.Query, restrict *roaring.Bitmap) (*roaring.Bitmap, error) {
	var (
		list *roaring.Bitmap
		err  error
	)
	switch q.Op {
	case index.QNone:
		// nothing
	case index.QAll:
		if restrict != nil {
			return restrict, nil
		}
		list = roaring.New()
		for i := uint32(0); i < r.toc.numRecords; i++ {
			list.Add(i)
		}
		return list, nil
	case index.QAnd:
		var ands []*roaring.Bitmap
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])

			p, err := r.Posting(tri)
			if err == ErrNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			ands = append(ands, p)
		}
		if restrict != nil {
			ands = append(ands, restrict)
		}
		if len(ands) > 0 {
			list = roaring.FastAnd(ands...)
		}
		for _, sub := range q.Sub {
			if list == nil {
				list = restrict
			}
			list, err = r.query(sub, list)
			if err != nil {
				return nil, err
			}
		}
	case index.QOr:
		var ors []*roaring.Bitmap
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])

			p, err := r.Posting(tri)
			if err == ErrNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			if restrict != nil {
				p.And(restrict)
			}
			ors = append(ors, p)
		}
		if len(ors) > 0 {
			list = roaring.FastOr(ors...)
		}
		for _, sub := range q.Sub {
			list1, err := r.query(sub, restrict)
			if err != nil {
				return nil, err
			}
			list.Or(list1)
		}
	}
	return list, nil
}

var ErrNotFound = errors.New("not found")

const postingEntrySize = 12

// postingOffset returns the offset and length for the postings list of the input trigram.
// It returns ErrNotFound if no list exists.
func (r *Reader) postingOffset(tri uint32) (uint32, int, error) {
	buf := make([]byte, r.toc.numTrigrams*postingEntrySize)
	if _, err := r.file.ReadAt(buf, int64(r.toc.trigrams)); err != nil {
		return 0, 0, err
	}
	i := sort.Search(int(r.toc.numTrigrams), func(i int) bool {
		i *= postingEntrySize
		t := binary.BigEndian.Uint32(buf[i:])
		return t >= tri
	})
	if i >= int(r.toc.numTrigrams) {
		return 0, 0, ErrNotFound
	}
	i *= postingEntrySize
	t := binary.BigEndian.Uint32(buf[i:])
	if t != tri {
		return 0, 0, ErrNotFound
	}
	o := binary.BigEndian.Uint32(buf[i+4:])
	l := int(binary.BigEndian.Uint32(buf[i+8:]))
	return o, l, nil
}

// Posting returns the postings list at the given offset and length.
func (r *Reader) Posting(tri uint32) (*roaring.Bitmap, error) {
	o, l, err := r.postingOffset(tri)
	if err != nil {
		return nil, err
	}
	b := make([]byte, l)
	if _, err := r.file.ReadAt(b, int64(o)); err != nil {
		return nil, err
	}
	rb := roaring.New()
	if _, err := rb.FromBuffer(b); err != nil {
		return nil, err
	}
	return rb, nil
}

// Record returns the record offset for the given ID.
func (r *Reader) Record(id uint32) (uint32, uint32, error) {
	var buf [8]byte
	off := r.toc.records + id*4
	if off >= r.toc.records+r.toc.numRecords*4 {
		return 0, 0, ErrNotFound
	}
	// Read offset of searched record and the next one.
	if _, err := r.file.ReadAt(buf[:], int64(off)); err != nil {
		return 0, 0, err
	}
	rec := binary.BigEndian.Uint32(buf[:4])
	next := binary.BigEndian.Uint32(buf[4:])
	// Infer record length from next offset,
	return rec, next - rec, nil
}

func basename(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(path)
	return base[:len(base)-len(ext)]
}

func modifyExtension(filename, newExt string) string {
	return filename[:len(filename)-len(filepath.Ext(filename))] + newExt
}

func parseFilename(filename string) (a, b ulid.ULID, err error) {
	fields := strings.SplitN(basename(filename), "-", 2)
	if len(fields) != 2 {
		return a, b, segmentParseError{filename, fmt.Errorf("invalid filename, not enough fields")}
	}
	a, err = ulid.Parse(fields[0])
	if err != nil {
		return a, b, segmentParseError{filename, fmt.Errorf("failed to parse first ULID: %v", err)}
	}
	b, err = ulid.Parse(fields[1])
	if err != nil {
		return a, b, segmentParseError{filename, fmt.Errorf("failed to parse second ULID: %v", err)}
	}
	return a, b, nil
}

type segmentParseError struct {
	Filename string
	Err      error
}

func (e segmentParseError) Error() string {
	return fmt.Sprintf("%s: %v", e.Filename, e.Err)
}

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
