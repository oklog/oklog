package main

import (
	"bufio"
	"container/ring"
	"io"
	"sync"

	"github.com/pkg/errors"
)

// ringBuffer is a fixed-length ring buffer. It can be used by a forwarder, to 'drop' messages instead of applying backpressure. See Issue #15
type ringBuffer struct {
	//onward io.Writer
	//prefix string
	max int64

	w *ring.Ring
	r *ring.Ring

	mutex     sync.Mutex
	remaining int64
	err       error
}

func (rb *ringBuffer) Consume(r io.Reader) {

	bs := bufio.NewScanner(r)
	ok := bs.Scan()
	for ok {
		record := bs.Text()
		rb.Put(record)
		ok = bs.Scan()
	}
	if !ok {
		rb.err = errors.Errorf("Cannot read from input")
	}
}

func (rb *ringBuffer) Scan() bool {
	return rb.err == nil
}

func (rb *ringBuffer) Text() string {
	return rb.Get()
}

func (rb *ringBuffer) Err() error {
	return rb.err
}

func (rb *ringBuffer) Remaining() int64 {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()
	return rb.remaining
}

func (rb *ringBuffer) inc(by int64) {
	rb.mutex.Lock()
	if rb.remaining+by > rb.max {
		rb.remaining = rb.max
	} else {
		rb.remaining += by
	}
	rb.mutex.Unlock()
}

func (rb *ringBuffer) Put(record string) (int, error) {
	rb.w.Value = record
	rb.w = rb.w.Next()
	rb.inc(1)
	return len(record) + 1, rb.err
}

// blocks. Do we need a Get with timeout?
func (rb *ringBuffer) Get() string {
	remaining := int64(0)
	for remaining < 1 {
		remaining = rb.Remaining()
	}
	v, _ := rb.r.Value.(string)
	rb.r = rb.r.Next()
	rb.inc(-1)
	return v
}

func newRingBuffer(ringBufSize int) *ringBuffer {
	r := ring.New(ringBufSize)
	rb := &ringBuffer{
		w:   r,
		r:   r,
		max: int64(ringBufSize),
	}
	return rb
}
