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

// NewFileLog returns a Log backed by the filesystem at path root.
// Note that we don't own segment files! They may disappear.
func NewFileLog(filesys fs.Filesystem, root string, segmentTargetSize, segmentBufferSize int64) (Log, error) {
	if err := filesys.MkdirAll(root); err != nil {
		return nil, err
	}
	lock := filepath.Join(root, lockFile)
	r, existed, err := filesys.Lock(lock)
	if err != nil {
		return nil, errors.Wrapf(err, "locking %s", lock)
	}
	if existed {
		return nil, errors.Errorf("%s already exists; another process is running, or the file is stale", lock)
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
	}, nil
}

type fileLog struct {
	root              string
	filesys           fs.Filesystem
	releaser          fs.Releaser
	segmentTargetSize int64
	segmentBufferSize int64
}

func (log *fileLog) Create() (WriteSegment, error) {
	filename := filepath.Join(log.root, fmt.Sprintf("%s%s", uuid.New(), extActive))
	f, err := log.filesys.Create(filename)
	if err != nil {
		return nil, err
	}
	return &fileWriteSegment{log.filesys, f}, nil
}

func (log *fileLog) Query(from, to time.Time, q string, regex, statsOnly bool) (QueryResult, error) {
	var (
		fromULID = ulid.MustNew(ulid.Timestamp(from), nil)
		toULID   = ulid.MustNew(ulid.Timestamp(to), nil)
		segments = log.queryMatchingSegments(fromULID, toULID)
		pass     = recordFilterPlain(fromULID, toULID, []byte(q))
	)

	// Time range should be inclusive, so we need a max value here.
	if err := toULID.SetEntropy(ulidMaxEntropy); err != nil {
		panic(err)
	}

	if regex {
		re, err := regexp.Compile(q)
		if err != nil {
			return QueryResult{}, err
		}
		pass = recordFilterRegex(fromULID, toULID, re)
	}

	// Build the lazy reader.
	rc, sz, err := newQueryReadCloser(log.filesys, segments, pass, log.segmentBufferSize)
	if err != nil {
		return QueryResult{}, err
	}
	if statsOnly {
		rc = ioutil.NopCloser(bytes.NewReader(nil))
	}

	return QueryResult{
		From:  from.String(),
		To:    to.String(),
		Q:     q,
		Regex: regex,

		NodesQueried:    1,
		SegmentsQueried: len(segments),
		MaxDataSetSize:  sz,
		ErrorCount:      0,

		Records: rc,
	}, nil
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
		readSegment, err := newFileReadSegment(log.filesys, path)
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
		readSegment, err := newFileReadSegment(log.filesys, path)
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
		readSegment, err := newFileReadSegment(log.filesys, path)
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
		f, err := log.filesys.Open(path)
		if err != nil {
			return nil, errors.Wrap(err, "opening candidate segment for read")
		}
		trashSegments[i] = fileTrashSegment{log.filesys, f}
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

func (log *fileLog) Close() error {
	return log.releaser.Release()
}

func recoverSegments(filesys fs.Filesystem, root string) error {
	var toRename []string
	filesys.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // recurse
		}
		switch filepath.Ext(path) {
		case extReading, extActive:
			toRename = append(toRename, path)
		}
		return nil
	})
	for _, path := range toRename {
		// It's possible this will create duplicate records.
		// We rely on repair and compaction to remove them.
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

func recordFilterPlain(from, to ulid.ULID, q []byte) recordFilter {
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

func recordFilterRegex(from, to ulid.ULID, q *regexp.Regexp) recordFilter {
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
