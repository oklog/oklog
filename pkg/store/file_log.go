package store

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"

	"github.com/oklog/oklog/pkg/fs"
)

const (
	extActive  = ".active"
	extFlushed = ".flushed"
	extReading = ".reading" // compacting or trashing
	extTrashed = ".trashed"

	ulidTimeSize = 10 // bytes

	lockFile = "LOCK"
)

var (
	ulidMaxEntropy = []byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	}
)

type fileLog struct {
	root              string
	filesys           fs.Filesystem
	releaser          fs.Releaser // for the LOCK
	segmentTargetSize int64
	segmentBufferSize int64
	compression       string
	reporter          EventReporter
}

// NewFileLog returns a Log backed by the filesystem at path root.
// Note that we don't own segment files! They may disappear.
func NewFileLog(filesys fs.Filesystem, root string, segmentTargetSize, segmentBufferSize int64, compression string, reporter EventReporter) (Log, error) {
	if reporter == nil {
		reporter = LogReporter{log.NewNopLogger()}
	}
	if err := filesys.MkdirAll(root); err != nil {
		return nil, errors.Wrapf(err, "creating path %s", root)
	}
	lock := filepath.Join(root, lockFile)
	r, existed, err := filesys.Lock(lock)
	if err != nil {
		return nil, errors.Wrapf(err, "locking %s", lock)
	}
	if existed {
		// The previous owner crashed, but we still got the lock.
		// So this is like Prometheus "crash recovery" mode.
		// But we don't have anything special we need to do.
	}
	if err := recoverSegments(filesys, root); err != nil {
		return nil, errors.Wrap(err, "during recovery")
	}
	return &fileLog{
		root:              root,
		filesys:           filesys,
		releaser:          r,
		segmentTargetSize: segmentTargetSize,
		segmentBufferSize: segmentBufferSize,
		compression:       compression,
		reporter:          reporter,
	}, nil
}

func (fl *fileLog) Create() (WriteSegment, error) {
	filename := filepath.Join(fl.root, fmt.Sprintf("%s%s%s", uuid.New(), fl.ext(extActive), compressionToExt[fl.compression]))
	f, err := fl.filesys.Create(filename)
	if err != nil {
		return nil, err
	}

	cf, err := newCompressedWriter(fl.compression, f)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &fileWriteSegment{fl.filesys, f, cf}, nil
}

func (fl *fileLog) Query(qp QueryParams, statsOnly bool) (QueryResult, error) {
	var (
		begin    = time.Now()
		segments = fl.queryMatchingSegments(qp.From.ULID, qp.To.ULID)
		pass     = recordFilterBoundedPlain(qp.From.ULID, qp.To.ULID, []byte(qp.Q))
	)
	if qp.Regex {
		pass = recordFilterBoundedRegex(qp.From.ULID, qp.To.ULID, regexp.MustCompile(qp.Q))
	}

	// Time range should be inclusive, so we need a max value here.
	if err := qp.To.ULID.SetEntropy(ulidMaxEntropy); err != nil {
		panic(err)
	}

	// Build the lazy reader.
	rc, sz, err := newQueryReadCloser(fl.filesys, segments, pass, fl.segmentBufferSize, fl.reporter)
	if err != nil {
		return QueryResult{}, errors.Wrap(err, "constructing the lazy reader")
	}
	if statsOnly {
		rc = ioutil.NopCloser(bytes.NewReader(nil))
	}

	return QueryResult{
		Params: qp,

		NodesQueried:    1,
		SegmentsQueried: len(segments),
		MaxDataSetSize:  sz,
		ErrorCount:      0,
		Duration:        time.Since(begin).String(),

		Records: rc,
	}, nil
}

func (fl *fileLog) Overlapping() ([]ReadSegment, error) {
	// We make a simple n-squared algorithm for now.
	// First, collect all flushed segments.
	segments := map[string][]string{}
	fl.filesys.Walk(fl.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		if firstExt(path) != extFlushed {
			return nil // skip
		}
		if _, _, err := parseFilename(path); err != nil {
			fl.reporter.ReportEvent(Event{
				Op: "Overlapping", File: path, Warning: err,
				Msg: fmt.Sprintf("will remove apparently-bad data file of size %d", info.Size()),
			})
			// TODO(pb): re-parse and recover this file, async
			if err := fl.filesys.Remove(path); err != nil {
				fl.reporter.ReportEvent(Event{
					Op: "Overlapping", File: path, Warning: err,
					Msg: "tried to remove apparently-bad data file, which failed",
				})
			}
			return nil // weird; ignore
		}
		segments[path] = nil
		return nil
	})

	// Then, for each segment, compare against all other segments.
	// Record all segments which overlap.
	for path := range segments {
		a, b, err := parseFilename(path) // TODO(pb): maybe eliminate double work
		if err != nil {
			panic(fmt.Errorf("failed to parse a filename that must have successfully parsed previously: %v", err))
		}
		for compare := range segments {
			if path == compare {
				continue // we will overlap with ourselves, natch
			}
			c, d, err := parseFilename(compare)
			if err != nil {
				panic(fmt.Errorf("failed to parse a filename that must have successfully parsed previously: %v", err))
			}
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
		readSegment, err := newFileReadSegment(fl.filesys, path)
		if err != nil {
			return nil, err
		}
		readSegments[i] = readSegment
	}
	return readSegments, nil
}

func (fl *fileLog) Sequential() ([]ReadSegment, error) {
	// First we need to build an index of all of the segments in time order.
	// For this we only need the first ULID in the segment.
	var segmentInfos []segmentInfo
	fl.filesys.Walk(fl.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		if firstExt(path) != extFlushed {
			return nil // skip
		}
		a, _, err := parseFilename(path)
		if err != nil {
			fl.reporter.ReportEvent(Event{
				Op: "Sequential", File: path, Warning: err,
				Msg: fmt.Sprintf("will remove apparently-bad data file of size %d", info.Size()),
			})
			// TODO(pb): re-parse and recover this file, async
			if err := fl.filesys.Remove(path); err != nil {
				fl.reporter.ReportEvent(Event{
					Op: "Sequential", File: path, Warning: err,
					Msg: "tried to remove apparently-bad data file, which failed",
				})
			}
			return nil // weird; skip
		}
		segmentInfos = append(segmentInfos, segmentInfo{a.String(), path, info.Size()})
		return nil
	})
	sort.Slice(segmentInfos, func(i, j int) bool { return segmentInfos[i].lowID < segmentInfos[j].lowID })

	// We'll walk all the segments and try to get at least 2 that are
	// small enough to compact together to the given target size.
	const minimumSegments = 2
	candidates := chooseFirstSequential(segmentInfos, minimumSegments, fl.segmentTargetSize)
	if len(candidates) < minimumSegments {
		return nil, ErrNoSegmentsAvailable // no problem
	}

	// Our candidates are good enough.
	// Create ReadSegments.
	readSegments := make([]ReadSegment, len(candidates))
	for i, path := range candidates {
		readSegment, err := newFileReadSegment(fl.filesys, path)
		if err != nil {
			return nil, err
		}
		readSegments[i] = readSegment
	}
	return readSegments, nil
}

func (fl *fileLog) Trashable(oldestRecord time.Time) ([]ReadSegment, error) {
	oldestID := ulid.MustNew(ulid.Timestamp(oldestRecord), nil)

	// Get the segments we'll trash.
	var candidates []string
	fl.filesys.Walk(fl.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		if firstExt(path) != extFlushed {
			return nil // skip
		}
		_, high, err := parseFilename(path)
		if err != nil {
			fl.reporter.ReportEvent(Event{
				Op: "Trashable", File: path, Warning: err,
				Msg: fmt.Sprintf("will remove apparently-bad data file of size %d", info.Size()),
			})
			// TODO(pb): re-parse and recover this file, async
			if err := fl.filesys.Remove(path); err != nil {
				fl.reporter.ReportEvent(Event{
					Op: "Trashable", File: path, Warning: err,
					Msg: "tried to remove apparently-bad data file, which failed",
				})
			}
			return nil // weird; skip
		}
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
		readSegment, err := newFileReadSegment(fl.filesys, path)
		if err != nil {
			return nil, err
		}
		readSegments[i] = readSegment
	}
	return readSegments, nil
}

func (fl *fileLog) Purgeable(oldestModTime time.Time) ([]TrashSegment, error) {
	// Get the segments we'll remove from the trash.
	var candidates []string
	fl.filesys.Walk(fl.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		if firstExt(path) != extTrashed {
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
		f, err := fl.filesys.Open(path)
		if err != nil {
			return nil, errors.Wrap(err, "opening candidate segment for read")
		}
		trashSegments[i] = fileTrashSegment{fl.filesys, f}
	}
	return trashSegments, nil
}

func (fl *fileLog) Stats() (LogStats, error) {
	var stats LogStats
	fl.filesys.Walk(fl.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // recurse
		}
		switch firstExt(path) {
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

func (fl *fileLog) Close() error {
	return fl.releaser.Release()
}

func (fl *fileLog) ext(ext string) string {
	return ext + compressionToExt[fl.compression]
}

func recoverSegments(filesys fs.Filesystem, root string) error {
	var toRename, toReprocess []string
	filesys.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // recurse
		}
		switch firstExt(path) {
		case extActive:
			toReprocess = append(toReprocess, path)
		case extReading:
			toRename = append(toRename, path)
		}
		return nil
	})

	for _, path := range toReprocess {
		// mergeRecords has the side effect of extracting the low and high ULIDs
		// from a segment file. We use it for that side effect. This is a little
		// bit inefficient; that's OK.
		f, err := filesys.Open(path)
		if err != nil {
			return err
		}

		ce := secondExt(path)
		cf, err := newCompressedReader(extToCompression[ce], f)
		if err != nil {
			return err
		}

		lo, hi, _, err := mergeRecords(ioutil.Discard, cf)
		cf.Close() // ignore error, for now
		if err != nil {
			return err
		}
		var (
			oldname = path
			oldpath = filepath.Dir(oldname)
			newname = filepath.Join(oldpath, fmt.Sprintf("%s-%s%s%s", lo, hi, extFlushed, ce))
		)
		if err := filesys.Rename(oldname, newname); err != nil {
			return err
		}
	}

	// It's possible this will create duplicate records.
	// We rely on repair and compaction to remove them.
	for _, path := range toRename {
		var (
			oldname = path
			newname = modifyExtension(oldname, extFlushed)
		)
		if err := filesys.Rename(oldname, newname); err != nil {
			return err
		}
	}

	return nil
}

func recordFilterPlain(q []byte) recordFilter {
	return func(b []byte) bool {
		return len(b) > ulid.EncodedSize && bytes.Contains(b[ulid.EncodedSize+1:], q)
	}
}

func recordFilterRegex(q *regexp.Regexp) recordFilter {
	return func(b []byte) bool {
		return len(b) > ulid.EncodedSize && q.Match(b[ulid.EncodedSize+1:])
	}
}

func recordFilterBoundedPlain(from, to ulid.ULID, q []byte) recordFilter {
	fromBytes, _ := from.MarshalText()
	fromBytes = fromBytes[:ulidTimeSize]
	toBytes, _ := to.MarshalText()
	toBytes = toBytes[:ulidTimeSize]
	return func(b []byte) bool {
		return len(b) > ulid.EncodedSize &&
			bytes.Compare(b[:ulidTimeSize], fromBytes) >= 0 &&
			bytes.Compare(b[:ulidTimeSize], toBytes) <= 0 &&
			bytes.Contains(b[ulid.EncodedSize+1:], q)
	}
}

func recordFilterBoundedRegex(from, to ulid.ULID, q *regexp.Regexp) recordFilter {
	fromBytes, _ := from.MarshalText()
	fromBytes = fromBytes[:ulidTimeSize]
	toBytes, _ := to.MarshalText()
	toBytes = toBytes[:ulidTimeSize]
	return func(b []byte) bool {
		return len(b) > ulid.EncodedSize &&
			bytes.Compare(b[:ulidTimeSize], fromBytes) >= 0 &&
			bytes.Compare(b[:ulidTimeSize], toBytes) <= 0 &&
			q.Match(b[ulid.EncodedSize+1:])
	}
}

// queryMatchingSegments returns a sorted slice of all segment files that could
// possibly have records in the provided time range. The caller is responsible
// for closing the segments.
func (fl *fileLog) queryMatchingSegments(from, to ulid.ULID) (segments []readSegment) {
	fl.filesys.Walk(fl.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // descend
		}
		// We should query .reading segments, too.
		// Better to get duplicates than miss records.
		if ext := firstExt(path); !(ext == extFlushed || ext == extReading) {
			return nil // skip
		}
		low, high, err := parseFilename(path)
		if err != nil {
			fl.reporter.ReportEvent(Event{
				Op: "queryMatchingSegments", File: path, Warning: err,
				Msg: fmt.Sprintf("will remove apparently-bad data file of size %d", info.Size()),
			})
			// TODO(pb): re-parse and recover this file, async
			if err := fl.filesys.Remove(path); err != nil {
				fl.reporter.ReportEvent(Event{
					Op: "queryMatchingSegments", File: path, Warning: err,
					Msg: "tried to remove apparently-bad data file, which failed",
				})
			}
			return nil // weird; skip
		}
		if !overlap(from, to, low, high) {
			return nil
		}
		file, err := fl.filesys.Open(path)
		if err == os.ErrNotExist {
			fl.reporter.ReportEvent(Event{
				Op: "queryMatchingSegments", File: path, Warning: err,
				Msg: "this can happen due to e.g. compaction",
			})
			return nil
		}
		if err != nil {
			fl.reporter.ReportEvent(Event{
				Op: "queryMatchingSegments", File: path, Error: err,
			})
			return nil
		}
		cfile, err := newCompressedReader(extToCompression[secondExt(path)], file)
		if err != nil {
			fl.reporter.ReportEvent(Event{
				Op: "queryMatchingSegments", File: path, Error: err,
			})
			return nil
		}

		segments = append(segments, readSegment{path, cfile, info.Size()})
		return nil
	})
	sort.Slice(segments, func(i, j int) bool {
		a := strings.SplitN(basename(segments[i].path), "-", 2)[0]
		b := strings.SplitN(basename(segments[j].path), "-", 2)[0]
		return a < b
	})
	return segments
}

type fileWriteSegment struct {
	fs fs.Filesystem
	f  fs.File
	cf compressedWriter
}

func (w fileWriteSegment) Write(p []byte) (int, error) {
	return w.cf.Write(p)
}

// Close the segment and make it available for query.
func (w fileWriteSegment) Close(low, high ulid.ULID) error {
	if err := w.cf.Close(); err != nil {
		return err
	}
	oldname := w.f.Name()
	_, ce := exts(oldname)
	oldpath := filepath.Dir(oldname)
	newname := filepath.Join(oldpath, fmt.Sprintf("%s-%s%s%s", low.String(), high.String(), extFlushed, ce))
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

func (w fileWriteSegment) Size() int64 {
	return w.cf.size()
}

type fileReadSegment struct {
	fs fs.Filesystem
	f  fs.File
	cf  compressedReader
}

func newFileReadSegment(fs fs.Filesystem, path string) (fileReadSegment, error) {
	se, ce := exts(path)

	if se != extFlushed {
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

	cf, err := newCompressedReader(extToCompression[ce], f)
	if err != nil {
		f.Close()
		return fileReadSegment{}, err
	}

	return fileReadSegment{fs, f, cf}, nil
}

func (r fileReadSegment) Read(p []byte) (int, error) {
	return r.cf.Read(p)
}

func (r fileReadSegment) Reset() error {
	if err := r.cf.Close(); err != nil {
		return err
	}
	oldpath := r.f.Name()
	newpath := modifyExtension(oldpath, extFlushed)
	return r.fs.Rename(oldpath, newpath)
}

func (r fileReadSegment) Trash() error {
	if err := r.cf.Close(); err != nil {
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
	if err := r.cf.Close(); err != nil {
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
	ext1, ext2 := exts(path)
	return base[:len(base)-len(ext1) - len(ext2)]
}

func modifyExtension(filename, newExt string) string {
	se, ce := exts(filename)
	return filename[:len(filename)-len(se)-len(ce)] + newExt + ce
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
