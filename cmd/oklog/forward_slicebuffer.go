package main

import (
	"sync"
)

// sliceBuffer is a fixed-length buffer. It can be used by a forwarder, to 'drop' messages instead of applying backpressure. See Issue #15
type sliceBuffer struct {
	//onward io.Writer
	//prefix string
	max int

	buf   []string
	first int
	last  int
	ch    chan string
	mutex sync.RWMutex
}

// Put forwards right away when data is needed
func (b *sliceBuffer) Put(record string) {
	select {
	case b.ch <- record:
	default:
		if int(b.Len()) >= b.max {
			// Drop record
			// Note that I originally tried to Shift off data off the beginning, and put this record onto the buffer.
			// But it's cheaper just to discard this record instead..
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
	b.mutex.RLock()
	if len(b.buf) < 1 {
		b.mutex.RUnlock()
		//just block until available
		record = <-b.ch
		return record
	}
	b.mutex.RUnlock()
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
	} else {
		return b.max - b.first + b.last + 1
	}
	return len(b.buf)
}

func newSliceBuffer(bufSize int) *sliceBuffer {
	ch := make(chan string)
	b := &sliceBuffer{
		max:   bufSize,
		buf:   make([]string, bufSize),
		mutex: sync.RWMutex{},
		ch:    ch,
	}
	return b
}
