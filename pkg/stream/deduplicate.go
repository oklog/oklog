package stream

import (
	"time"

	"github.com/google/btree"
	"github.com/oklog/ulid"
)

// Deduplicate and order records within the given time window.
// A smaller window may cause duplicate or out-of-order messages.
// A larger window will cause higher end-to-end latency.
// The ticker is used every window / 10 to flush the buffer.
// The returned chan is closed when the in chan is closed.
func Deduplicate(in <-chan []byte, window time.Duration, ticker func(time.Duration) *time.Ticker) <-chan []byte {
	out := make(chan []byte, 1024) // TODO(pb): validate buffer size
	go func() {
		var (
			d = dedupe{
				BTree:         btree.New(2),
				ulidRecordmap: map[WrapULID][]byte{},
			}
			tk = ticker(window / 10)
		)
		defer tk.Stop()
		defer close(out)
		for {
			select {
			case record, ok := <-in:
				if !ok {
					return
				}
				d.insert(record)

			case now := <-tk.C:
				d.remove(now.Add(-window), out)
			}
		}
	}()
	return out
}

type dedupe struct {
	*btree.BTree
	ulidRecordmap map[WrapULID][]byte // map[ulid]record && no race
}

func (d dedupe) insert(record []byte) {
	if len(record) <= ulid.EncodedSize {
		panic("invalid record")
	}
	var node ulid.ULID
	node.UnmarshalText(record[:ulid.EncodedSize])
	d.BTree.ReplaceOrInsert(WrapULID{node})
	d.ulidRecordmap[WrapULID{node}] = record
}

func (d dedupe) remove(olderThan time.Time, dst chan<- []byte) {
	var (
		pivot  = ulid.MustNew(ulid.Timestamp(olderThan), nil)
		toEmit []WrapULID
	)
	d.BTree.AscendLessThan(WrapULID{pivot}, func(i btree.Item) bool {
		// Unfortunately, can't mutate the tree during iteration.
		toEmit = append(toEmit, i.(WrapULID))
		return true
	})
	for _, node := range toEmit {
		dst <- d.ulidRecordmap[node]
		d.BTree.Delete(node)
	}
}

type WrapULID struct {
	ulid.ULID
}

func (w WrapULID) Less(other btree.Item) bool {
	if w.Compare(other.(WrapULID).ULID) < 0 {
		return true
	}
	return false
}
