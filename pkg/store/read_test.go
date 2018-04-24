package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/oklog/pkg/record"
	"github.com/oklog/ulid"
)

func TestMergeRecordsToLog(t *testing.T) {
	t.Parallel()

	records := []string{
		0:  fmt.Sprintf("%s A\n", ulid.MustNew(100, nil).String()),
		1:  fmt.Sprintf("%s B\n", ulid.MustNew(101, nil).String()),
		2:  fmt.Sprintf("%s C\n", ulid.MustNew(102, nil).String()),
		3:  fmt.Sprintf("%s D\n", ulid.MustNew(103, nil).String()),
		4:  fmt.Sprintf("%s E\n", ulid.MustNew(104, nil).String()),
		5:  fmt.Sprintf("%s F\n", ulid.MustNew(105, nil).String()),
		6:  fmt.Sprintf("%s G\n", ulid.MustNew(106, nil).String()),
		7:  fmt.Sprintf("%s H\n", ulid.MustNew(107, nil).String()),
		8:  fmt.Sprintf("%s I\n", ulid.MustNew(108, nil).String()),
		9:  fmt.Sprintf("%s J\n", ulid.MustNew(109, nil).String()),
		10: fmt.Sprintf("%s K\n", ulid.MustNew(110, nil).String()),
		11: fmt.Sprintf("%s L\n", ulid.MustNew(111, nil).String()),
	}
	var (
		dst               = &mockLog{&bytes.Buffer{}}
		segmentTargetSize = int64(100000)
		readers           = []io.Reader{
			strings.NewReader(records[0] + records[3] + records[10]),
			strings.NewReader(records[2] + records[4] + records[5] + records[11]),
			strings.NewReader(records[1] + records[6] + records[7] + records[8]),
			strings.NewReader(records[9]),
		}
	)

	n, err := mergeRecordsToLog(dst, segmentTargetSize, readers...)
	if err != nil {
		t.Error(err)
	}

	var totalSize int
	for _, s := range records {
		totalSize += len(s)
	}
	if want, have := int64(totalSize), n; want != have {
		t.Errorf("n: want %d, have %d", want, have)
	}

	if want, have := strings.Join(records, ""), dst.Buffer.String(); want != have {
		t.Errorf("Result: want %q, have %q", want, have)
	}
}

func BenchmarkMergeRecordsToLog(b *testing.B) {
	// Need large number of big (1k) records to get the performance problems
	// with bytes.FieldsFunc to really show themselves. If you drop these
	// numbers too low, you don't see much of a difference.
	const (
		readerCount       = 10
		recordCount       = 10000
		recordSize        = 1024
		segmentTargetSize = recordCount * recordSize * (readerCount / 5)
		segmentBufferSize = 1024
		charset           = "0123456789ABCDEFGHJKMNPQRSTVWXYZ "
	)

	dst, err := NewFileLog(fs.NewNopFilesystem(), "/", segmentTargetSize, segmentBufferSize, nil)
	if err != nil {
		b.Fatal(err)
	}

	startStamp := ulid.Now()
	rand.Seed(12345678)
	populate := func(w io.Writer) {
		stamp := startStamp
		for i := 0; i < recordCount; i++ {
			stamp += uint64(rand.Int63n(10))
			id := ulid.MustNew(stamp, nil) // ascending records plz
			record := make([]rune, recordSize-1-ulid.EncodedSize-1)
			for j := range record {
				record[j] = rune(charset[rand.Intn(len(charset))])
			}
			fmt.Fprintf(w, "%s %s\n", id, string(record))
		}
	}

	readers := make([]io.Reader, readerCount)
	for i := 0; i < readerCount; i++ {
		var buf bytes.Buffer
		populate(&buf)
		readers[i] = &buf
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n, err := mergeRecordsToLog(dst, segmentTargetSize, readers...); err != nil {
			b.Errorf("n=%d err=%v", n, err)
		}
	}
}

type mockLog struct {
	*bytes.Buffer
}

func (log *mockLog) Create() (WriteSegment, error) {
	return &mockWriteSegment{log.Buffer}, nil
}

func (log *mockLog) Query(qp record.QueryParams, statsOnly bool) (record.QueryResult, error) {
	return record.QueryResult{}, errors.New("not implemented")
}

func (log *mockLog) Oldest() (ReadSegment, error) {
	return nil, errors.New("not implemented")
}

func (log *mockLog) Overlapping() ([]ReadSegment, error) {
	return nil, errors.New("not implemented")
}

func (log *mockLog) Sequential() ([]ReadSegment, error) {
	return nil, errors.New("not implemented")
}

func (log *mockLog) Trashable(oldestRecord time.Time) ([]ReadSegment, error) {
	return nil, errors.New("not implemented")
}

func (log *mockLog) Purgeable(oldestModTime time.Time) ([]TrashSegment, error) {
	return nil, errors.New("not implemented")
}

func (log *mockLog) Stats() (LogStats, error) {
	return LogStats{}, errors.New("not implemented")
}

func (log *mockLog) Close() error { return nil }

type mockWriteSegment struct{ *bytes.Buffer }

func (mockWriteSegment) Close(ulid.ULID, ulid.ULID) error { return nil }
func (mockWriteSegment) Delete() error                    { return nil }
