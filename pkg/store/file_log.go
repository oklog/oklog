package store

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"

	"github.com/oklog/prototype/pkg/fs"
	"github.com/oklog/ulid"
)

const (
	extActive  = ".active"
	extFlushed = ".flushed"
	extReading = ".reading" // compacting or trashing
	extTrashed = ".trashed"
)

// NewFileLog returns a Log backed by the filesystem at path root.
// Note that we don't own segment files! They may disappear.
func NewFileLog(fs fs.Filesystem, root string, segmentTargetSize int64) (Log, error) {
	if err := fs.MkdirAll(root); err != nil {
		return nil, err
	}
	return &fileLog{
		fs:                fs,
		root:              root,
		segmentTargetSize: segmentTargetSize,
	}, nil
}

type fileLog struct {
	fs                fs.Filesystem
	root              string
	segmentTargetSize int64
}

func (log *fileLog) Create() (WriteSegment, error) {
	filename := filepath.Join(log.root, fmt.Sprintf("%s%s", uuid.New(), extActive))
	f, err := log.fs.Create(filename)
	if err != nil {
		return nil, err
	}
	return &fileWriteSegment{log.fs, f}, nil
}

func (log *fileLog) Query(engine QueryEngine, from, to time.Time, q string, statsOnly bool) (QueryResult, error) {
	var (
		fromULID = ulid.MustNew(ulid.Timestamp(from), nil)
		toULID   = ulid.MustNew(ulid.Timestamp(to), nil)
		segments = log.queryMatchingSegments(fromULID, toULID)
	)
	switch engine {
	case QueryEngineNaïve:
		return log.queryNaïve(segments, fromULID, toULID, q, statsOnly)
	case QueryEngineRipgrep:
		return log.queryRipgrep(segments, fromULID, toULID, q, statsOnly)
	case QueryEngineLazy:
		return log.queryLazy(segments, fromULID, toULID, q, statsOnly)
	default:
		return QueryResult{}, errors.Errorf("unsupported engine %s", engine)
	}
}

func (log *fileLog) Overlapping() ([]ReadSegment, error) {
	// We make a simple n-squared algorithm for now.
	// First, collect all flushed segments.
	segments := map[string][]string{}
	filepath.Walk(log.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		if filepath.Ext(path) != extFlushed {
			return nil // skip
		}
		segments[path] = nil
		return nil
	})

	// Then, for each segment, compare against all other segments.
	// Record all segments which overlap.
	for path := range segments {
		fields := strings.SplitN(basename(path), "-", 2)
		if len(fields) != 2 {
			continue // weird; skip
		}
		a := ulid.MustParse(fields[0])
		b := ulid.MustParse(fields[1])
		for compare := range segments {
			if path == compare {
				continue // we will overlap with ourselves, natch
			}
			fields = strings.SplitN(basename(compare), "-", 2)
			if len(fields) != 2 {
				continue // weird; skip
			}
			c := ulid.MustParse(fields[0])
			d := ulid.MustParse(fields[1])
			if overlap(a, b, c, d) {
				segments[path] = append(segments[path], compare)
			}
		}
	}

	// Next, figure out which segment has the most overlap.
	// It and all of its overlapping friends become our candidates.
	var candidates []string
	for path, overlaps := range segments {
		if len(overlaps)+1 > len(candidates) {
			candidates = append(overlaps, path)
		}
	}

	// The most overlap may still not be enough.
	const minimumOverlap = 3 // TODO(pb): parameterize
	if len(candidates) < minimumOverlap {
		return nil, ErrNoSegmentsAvailable
	}

	// Our candidates are good enough.
	// Create ReadSegments.
	readSegments := make([]ReadSegment, len(candidates))
	for i, path := range candidates {
		readSegment, err := newFileReadSegment(log.fs, path)
		if err != nil {
			return nil, err
		}
		readSegments[i] = readSegment
	}
	return readSegments, nil
}

func (log *fileLog) Sequential() ([]ReadSegment, error) {
	// First we need to build an index of all of the segments in time order.
	// For this we only need the first ULID in the segment.
	var segmentInfos []segmentInfo
	filepath.Walk(log.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		if filepath.Ext(path) != extFlushed {
			return nil // skip
		}
		fields := strings.SplitN(basename(path), "-", 2)
		if len(fields) != 2 {
			return nil // weird; skip
		}
		segmentInfos = append(segmentInfos, segmentInfo{fields[0], path, info.Size()})
		return nil
	})
	sort.Slice(segmentInfos, func(i, j int) bool { return segmentInfos[i].lowID < segmentInfos[j].lowID })

	// We'll walk all the segments and try to get at least 2 that are
	// small enough to compact together to the given target size.
	const minimumSegments = 2
	candidates := chooseFirstSequential(segmentInfos, minimumSegments, log.segmentTargetSize)
	if len(candidates) < minimumSegments {
		return nil, ErrNoSegmentsAvailable // no problem
	}

	// Our candidates are good enough.
	// Create ReadSegments.
	readSegments := make([]ReadSegment, len(candidates))
	for i, path := range candidates {
		readSegment, err := newFileReadSegment(log.fs, path)
		if err != nil {
			return nil, err
		}
		readSegments[i] = readSegment
	}
	return readSegments, nil
}

func (log *fileLog) Trashable(oldestRecord time.Time) ([]ReadSegment, error) {
	oldestID := ulid.MustNew(ulid.Timestamp(oldestRecord), nil)

	// Get the segments we'll trash.
	var candidates []string
	filepath.Walk(log.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		if filepath.Ext(path) != extFlushed {
			return nil // skip
		}
		fields := strings.SplitN(basename(path), "-", 2)
		if len(fields) != 2 {
			return nil // weird; skip
		}
		high := ulid.MustParse(fields[1])
		if bytes.Compare(high[:], oldestID[:]) < 0 {
			candidates = append(candidates, path)
		}
		return nil
	})
	if len(candidates) <= 0 {
		return nil, ErrNoSegmentsAvailable
	}

	// We have some candidates. Create and return ReadSegments.
	readSegments := make([]ReadSegment, len(candidates))
	for i, path := range candidates {
		readSegment, err := newFileReadSegment(log.fs, path)
		if err != nil {
			return nil, err
		}
		readSegments[i] = readSegment
	}
	return readSegments, nil
}

func (log *fileLog) Purgeable(oldestModTime time.Time) ([]TrashSegment, error) {
	// Get the segments we'll remove from the trash.
	var candidates []string
	filepath.Walk(log.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		if filepath.Ext(path) != extTrashed {
			return nil // skip
		}
		if info.ModTime().Before(oldestModTime) {
			candidates = append(candidates, path)
		}
		return nil
	})
	if len(candidates) <= 0 {
		return nil, ErrNoSegmentsAvailable
	}

	// We have some candidates. Create and return TrashSegments.
	trashSegments := make([]TrashSegment, len(candidates))
	for i, path := range candidates {
		f, err := log.fs.Open(path)
		if err != nil {
			return nil, errors.Wrap(err, "opening candidate segment for read")
		}
		trashSegments[i] = fileTrashSegment{log.fs, f}
	}
	return trashSegments, nil
}

func (log *fileLog) Stats() (LogStats, error) {
	var stats LogStats
	filepath.Walk(log.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // recurse
		}
		switch filepath.Ext(path) {
		case extActive:
			stats.ActiveSegments++
			stats.ActiveBytes += info.Size()
		case extFlushed:
			stats.FlushedSegments++
			stats.FlushedBytes += info.Size()
		case extReading:
			stats.ReadingSegments++
			stats.ReadingBytes += info.Size()
		case extTrashed:
			stats.TrashedSegments++
			stats.TrashedBytes += info.Size()
		}
		return nil
	})
	return stats, nil
}

// queryNaïve does a blind K-way merge of matching segments into memory,
// then per-record filtering for time boundaries and the query expression.
// The returned Records io.Reader is just an in-memory buffer.
func (log *fileLog) queryNaïve(segments []string, from, to ulid.ULID, q string, statsOnly bool) (QueryResult, error) {
	re, err := regexp.Compile(q)
	if err != nil {
		return QueryResult{}, err
	}

	// Merge matching segments in a global time order.
	var readers []io.Reader
	for _, segment := range segments {
		f, err := log.fs.Open(segment)
		if err != nil {
			continue
		}
		defer f.Close()
		readers = append(readers, f)
	}
	var records bytes.Buffer
	if _, _, _, err = mergeRecords(&records, readers...); err != nil {
		return QueryResult{}, err
	}

	// Filter matching records.
	var (
		s              = bufio.NewScanner(&records)
		filtered       = bytes.Buffer{}
		recordsQueried = 0
		recordsMatched = 0
		fromBytes      = []byte(from.String())
		toBytes        = []byte(to.String())
	)
	for s.Scan() {
		recordsQueried++
		b := s.Bytes()
		if len(b) < ulid.EncodedSize {
			continue // weird
		}
		id := b[:ulid.EncodedSize]
		if bytes.Compare(id, fromBytes) < 0 {
			continue
		}
		if bytes.Compare(id, toBytes) > 0 {
			continue
		}
		if re.Match(b) {
			recordsMatched++
			filtered.Write(append(b, '\n'))
		}
	}
	if statsOnly {
		records.Reset()
	} else {
		records = filtered
	}

	// Return.
	return QueryResult{
		Engine:          string(QueryEngineNaïve),
		From:            from.String(),
		To:              to.String(),
		Q:               q,
		NodesQueried:    1,
		SegmentsQueried: len(segments),
		RecordsQueried:  recordsQueried,
		RecordsMatched:  recordsMatched,
		Records:         ioutil.NopCloser(&records),
	}, nil
}

// queryRipgrep just shells out to ripgrep -- or cat, if no q is specified.
// It doesn't accurately filter on time bounds, or ensure correct order.
// It's here to serve as a "target" benchmark for other implementations.
func (log *fileLog) queryRipgrep(segments []string, from, to ulid.ULID, q string, statsOnly bool) (QueryResult, error) {
	var pipeline string
	if q == "" {
		pipeline = fmt.Sprintf("cat %s", strings.Join(segments, " "))
	} else {
		pipeline = fmt.Sprintf("rg -e %q %s", q, strings.Join(segments, " "))
	}

	// TODO(pb): need to filter out too-old and too-recent records

	cmd := exec.Command("sh", "-c", pipeline)
	var records bytes.Buffer
	cmd.Stdout = &records
	if err := cmd.Run(); err != nil {
		return QueryResult{}, errors.Wrapf(err, "%s", pipeline)
	}

	// TODO(pb): statsOnly requires a different code path altogether

	return QueryResult{
		Engine:          string(QueryEngineRipgrep),
		From:            from.String(),
		To:              to.String(),
		Q:               q,
		NodesQueried:    1,
		SegmentsQueried: len(segments),
		RecordsQueried:  -1,
		RecordsMatched:  -1,
		Records:         ioutil.NopCloser(&records),
	}, nil
}

// queryLazy is like naïve, but via lazy-evaluated (streaming) readers.
// We build up the chain of readers and return a result quite quickly.
// Costs are deferred to whoever reads the QueryResult Records.
func (log *fileLog) queryLazy(segments []string, from, to ulid.ULID, q string, statsOnly bool) (QueryResult, error) {
	// Build the query matcher.
	matchQuery := func([]byte) bool { return true }
	if q != "" {
		re, err := regexp.Compile(q)
		if err != nil {
			return QueryResult{}, err
		}
		matchQuery = func(b []byte) bool { return re.Match(b[ulid.EncodedSize+1:]) }
	}

	// Build the record filter.
	fromBytes, toBytes := from[:], to[:]
	pass := func(b []byte) bool {
		if len(b) < ulid.EncodedSize {
			println("### short")
			return false
		}
		if !(bytes.Compare(b[:ulid.EncodedSize], fromBytes) >= 0) {
			println("### old")
			return false
		}
		if !(bytes.Compare(b[:ulid.EncodedSize], toBytes) <= 0) {
			println("### new")
			return false
		}
		if !matchQuery(b) {
			println("### nomatch")
			return false
		}
		return true
		//return len(b) > ulid.EncodedSize &&
		//	bytes.Compare(b[:ulid.EncodedSize], fromBytes) >= 0 &&
		//	bytes.Compare(b[:ulid.EncodedSize], toBytes) <= 0 &&
		//	matchQuery(b)
	}

	// Build the lazy reader.
	r, err := newQueryReader(log.fs, segments, pass)
	if err != nil {
		return QueryResult{}, err
	}

	// TODO(pb): statsOnly requires a different code path altogether

	return QueryResult{
		Engine:          string(QueryEngineLazy),
		From:            from.String(),
		To:              to.String(),
		Q:               q,
		NodesQueried:    1,
		SegmentsQueried: len(segments),
		RecordsQueried:  -1,
		RecordsMatched:  -1,
		Records:         ioutil.NopCloser(r),
	}, nil
}

// queryMatchingSegments returns a sorted slice of all segment files
// that could possibly have records in the provided time range.
func (log *fileLog) queryMatchingSegments(from, to ulid.ULID) (segments []string) {
	filepath.Walk(log.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		// We should query .reading segments, too.
		// Better to get duplicates than miss records.
		if ext := filepath.Ext(path); !(ext == extFlushed || ext == extReading) {
			return nil // skip
		}
		fields := strings.SplitN(basename(path), "-", 2)
		if len(fields) != 2 {
			return nil // weird; skip
		}
		low := ulid.MustParse(fields[0])
		high := ulid.MustParse(fields[1])
		if overlap(from, to, low, high) {
			segments = append(segments, path)
		}
		return nil
	})
	sort.Slice(segments, func(i, j int) bool {
		a := strings.SplitN(basename(segments[i]), "-", 2)[0]
		b := strings.SplitN(basename(segments[j]), "-", 2)[0]
		return a < b
	})
	return segments
}

type fileWriteSegment struct {
	fs fs.Filesystem
	f  fs.File
}

func (w fileWriteSegment) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

// Close the segment and make it available for query.
func (w fileWriteSegment) Close(low, high ulid.ULID) error {
	if err := w.f.Close(); err != nil {
		return err
	}
	oldname := w.f.Name()
	oldpath := filepath.Dir(oldname)
	newname := filepath.Join(oldpath, fmt.Sprintf("%s-%s%s", low.String(), high.String(), extFlushed))
	if w.fs.Exists(newname) {
		return errors.Errorf("file %s already exists", newname)
	}
	return w.fs.Rename(oldname, newname)
}

// Delete the segment.
func (w fileWriteSegment) Delete() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	return w.fs.Remove(w.f.Name())
}

type fileReadSegment struct {
	fs fs.Filesystem
	f  fs.File
}

func newFileReadSegment(fs fs.Filesystem, path string) (fileReadSegment, error) {
	if filepath.Ext(path) != extFlushed {
		return fileReadSegment{}, errors.Errorf("newFileReadSegment from non-flushed file %s", path)
	}
	oldpath := path
	newpath := modifyExtension(oldpath, extReading)
	if err := fs.Rename(oldpath, newpath); err != nil {
		return fileReadSegment{}, err
	}
	f, err := fs.Open(newpath)
	if err != nil {
		return fileReadSegment{}, err
	}
	return fileReadSegment{fs, f}, nil
}

func (r fileReadSegment) Read(p []byte) (int, error) {
	return r.f.Read(p)
}

func (r fileReadSegment) Reset() error {
	if err := r.f.Close(); err != nil {
		return err
	}
	oldpath := r.f.Name()
	newpath := modifyExtension(oldpath, extFlushed)
	return r.fs.Rename(oldpath, newpath)
}

func (r fileReadSegment) Trash() error {
	if err := r.f.Close(); err != nil {
		return err
	}
	oldpath := r.f.Name()
	newpath := modifyExtension(oldpath, extTrashed)
	if err := r.fs.Rename(oldpath, newpath); err != nil {
		return err
	}
	return r.fs.Chtimes(newpath, time.Now(), time.Now())
}

func (r fileReadSegment) Purge() error {
	if err := r.f.Close(); err != nil {
		return err
	}
	return r.fs.Remove(r.f.Name())
}

type fileTrashSegment struct {
	fs fs.Filesystem
	f  fs.File
}

func (t fileTrashSegment) Purge() error {
	if err := t.f.Close(); err != nil {
		return err
	}
	return t.fs.Remove(t.f.Name())
}

// chooseFirstSequential segments that are small enough to compact together to
// less than the target size. Don't bother returning anything if you can't find
// at least minimum.
func chooseFirstSequential(segmentInfos []segmentInfo, minimum int, targetSize int64) []string {
	var (
		candidates    []string
		candidateSize int64
	)
	for _, si := range segmentInfos {
		if (candidateSize + si.size) <= targetSize {
			// We can take this segment. Merge.
			candidates = append(candidates, si.path)
			candidateSize += si.size
		} else if len(candidates) >= minimum {
			// We can't take this segment, but we have enough already. Break.
			break
		} else if si.size <= targetSize {
			// We can't *take* this segment, but we can *start* with it. Reset.
			candidates = []string{si.path}
			candidateSize = si.size
		} else {
			// We can't take or start with this segment. Clear.
			candidates = []string{}
			candidateSize = 0
		}
	}
	if len(candidates) < minimum {
		candidates = []string{} // oh well
	}
	return candidates
}

type segmentInfo struct {
	lowID string
	path  string
	size  int64
}

func basename(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(path)
	return base[:len(base)-len(ext)]
}

func modifyExtension(filename, newExt string) string {
	return filename[:len(filename)-len(filepath.Ext(filename))] + newExt
}
