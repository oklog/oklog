package stream

import (
	"bytes"
	"fmt"
	"time"

	"github.com/google/btree"
	"github.com/oklog/ulid"
)

// Deduplicate and order records within the given time window.
// A smaller window may cause duplicate or out-of-order messages.
// A larger window will cause higher end-to-end latency.
// The ticker is used every window / 10 to flush the buffer.
// The function returns when the `in` chan is closed.
func Deduplicate(in <-chan []byte, window time.Duration, ticker func(time.Duration) *time.Ticker, out chan<- []byte) {
	var (
		d  = dedupe{BTree: btree.New(2)}
		tk = ticker(window / 10)
	)
	defer tk.Stop()
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
}

type dedupe struct{ *btree.BTree }

func (d dedupe) insert(record []byte) {
	d.BTree.ReplaceOrInsert(item(record))
}

func (d dedupe) remove(olderThan time.Time, dst chan<- []byte) {
	var (
		pivot, _ = ulid.MustNew(ulid.Timestamp(olderThan), nil).MarshalText()
		toEmit   [][]byte
	)
	d.BTree.AscendLessThan(item(pivot), func(i btree.Item) bool {
		// Unfortunately, can't mutate the tree during iteration.
		toEmit = append(toEmit, i.(item))
		return true
	})
	for _, record := range toEmit {
		dst <- record
		d.BTree.Delete(item(record))
	}
}

type item []byte

func (i item) Less(other btree.Item) bool {
	otherItem := other.(item)
	if len(i) < ulid.EncodedSize || len(otherItem) < ulid.EncodedSize {
		panic(fmt.Sprintf("invalid record given to deduplicator: %q v %q", string(i), string(otherItem)))
	}

	// We rely on the BTree itself to deduplicate. From the docs:
	// "If !a.Less(b) && !b.Less(a), we treat this to mean a == b."
	// So make sure to return false when bytes.Compare == 0.
	return bytes.Compare(i[:ulid.EncodedSize], otherItem[:ulid.EncodedSize]) < 0
}
