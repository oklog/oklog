package main

import (
	"sync"
)

// sliceBuffer is a fixed-length ring buffer. It can be used by a forwarder, to 'drop' messages instead of applying backpressure. See Issue #15
type sliceBuffer struct {
	max int

	buf   []string
	first int         // the index of the first item in the buffer
	last  int         // the index of the last item in the buffer
	ch    chan string // the channel is used to block on the Get method
	mutex sync.RWMutex
}

// Put forwards right away
func (b *sliceBuffer) Put(record string) {
	select {
	case b.ch <- record:
	default:
		if b.Len() >= b.max {
			// Drop record
			// Note: another alternative would have been to move the 'first' value before putting this record onto the buffer, but it's easier and marginally cheaper to just drop the incoming record.
			return
		}
		b.mutex.Lock()
		b.buf = append(b.buf, record)
		b.buf[b.last] = record
		if b.last >= b.max {
			b.last = 0
		} else {
			b.last++
		}
		b.mutex.Unlock()
	}
}

// Get blocks when no data is available
func (b *sliceBuffer) Get() string {
	var record string
	if b.Len() < 1 {
		//just block until available
		record = <-b.ch
		return record
	}
	b.mutex.Lock()
	record = b.buf[b.first]
	if b.first >= b.max {
		b.first = 0
	} else {
		b.first++
	}
	b.mutex.Unlock()
	return record
}

func (b *sliceBuffer) Len() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	if b.last >= b.first {
		return b.last - b.first
	}
	return b.max - b.first + b.last + 1
}

func newSliceBuffer(bufSize int) *sliceBuffer {
	b := &sliceBuffer{
		max:   bufSize,
		buf:   make([]string, bufSize),
		mutex: sync.RWMutex{},
		ch:    make(chan string),
	}
	return b
}
