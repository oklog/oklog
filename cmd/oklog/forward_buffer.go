package main

import (
	"container/ring"
	"sync"
)

type textScanner interface {
	Scan() bool
	Text() string
	Err() error
}

// ringBuffer is a fixed-length ring buffer. It can be used by a forwarder, to 'drop' messages instead of applying backpressure. See Issue #15
type ringBuffer struct {
	//onward io.Writer
	//prefix string
	max int64

	w *ring.Ring
	r *ring.Ring

	mutex     sync.Mutex
	remaining int64
	done      bool
	err       error
}

func (bf *ringBuffer) Scan() bool {
	return false
}

func (bf *ringBuffer) Text() string {
	return bf.Get()
}

func (bf *ringBuffer) Err() error {
	return nil
}

func (bf *ringBuffer) status() (bool, int64) {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()
	return bf.done, bf.remaining
}

func (bf *ringBuffer) inc(by int64) {
	bf.mutex.Lock()
	if bf.remaining+by > bf.max {
		bf.remaining = bf.max
	} else {
		bf.remaining += by
	}
	bf.mutex.Unlock()
}

func (bf *ringBuffer) Put(record string) (int, error) {
	bf.w.Value = record
	bf.w = bf.w.Next()
	bf.inc(1)
	return len(record) + 1, bf.err
}

func (bf *ringBuffer) Get() string {
	remaining := int64(0)
	for remaining < 1 {
		_, remaining = bf.status()
	}
	//TODO
	v, _ := bf.r.Value.(string)
	bf.r = bf.r.Next()
	bf.inc(-1)
	return v
}

func (bf *ringBuffer) stop() error {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()
	bf.done = true
	return bf.err
}

func newRingBuffer(ringBufSize int) *ringBuffer {
	r := ring.New(ringBufSize)
	bf := &ringBuffer{
		w:   r,
		r:   r,
		max: int64(ringBufSize),
	}
	return bf
}
