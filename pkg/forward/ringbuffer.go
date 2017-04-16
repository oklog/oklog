package forward

import (
	"fmt"
	"sync"
)

// RingBuffer is a fixed-length ring buffer. It can be used by a forwarder, to 'drop' messages instead of applying backpressure. See Issue #15
type RingBuffer struct {
	maxSize int

	buf   []string
	first int          // the index of the first item in the buffer
	len   int          // the current length of the buffer
	ch    chan string  // the channel is used to block on the Get method whenever data is unavailable
	mutex sync.RWMutex // synchronizes changes to buf, first, last
}

func (b *RingBuffer) diagnostics(t string) {
	fmt.Printf("%s slice:%+v, slicelen:%d, first:%d, last:%d, len:%d \n", t, b.buf, len(b.buf), b.first, b.last(), b.len)
}

// Put() processes the record without blocking.
// It's behaviour varies depending on the state of the buffer and any blocking Get() invocations
// It either sends the record over the channel, adds it to the buffer, or drops the record if the buffer is full.
func (b *RingBuffer) Put(record string) {
	select {
	case b.ch <- record: // no need to even add to the buffer if something is blocking on the channel
	default:
		b.mutex.Lock()
		defer b.mutex.Unlock()
		b.diagnostics(">PUT:before")
		b.buf[b.last()] = record
		if b.len >= b.maxSize {
			b.inc()
		} else {
			b.len++
		}
		b.diagnostics(">PUT:after")
	}
}

func (b *RingBuffer) inc() {
	if b.first >= b.maxSize-1 {
		b.first = 0
	} else {
		b.first++
	}
}

func (b *RingBuffer) last() int {
	r := b.len + b.first
	if r >= b.maxSize {
		r -= b.maxSize
	}
	return r
}

// Get() blocks until data is available
func (b *RingBuffer) Get() string {
	var record string
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.len < 1 {
		b.diagnostics("GET>:no-data")
		//just block until available
		record = <-b.ch
		return record
	}
	b.diagnostics("GET>:before")
	record = b.buf[b.first]
	b.inc()
	b.len--
	b.diagnostics("GET>:after")
	return record
}

// Len() is just a synchronized version of len()
func (b *RingBuffer) Len() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.len
}

func NewRingBuffer(bufSize int) *RingBuffer {
	if bufSize < 1 {
		panic("buffer size should be greater than zero")
	}
	b := &RingBuffer{
		maxSize: bufSize,
		buf:     make([]string, bufSize),
		mutex:   sync.RWMutex{},
		ch:      make(chan string),
		first:   0,
		len:     0,
	}
	return b
}
