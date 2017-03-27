package main

import (
	"container/ring"
	"fmt"
	"io"
	"sync"
)

// bufferedForwarder is a fixed-length ring buffer. It can be used by a forwarder, to 'drop' messages instead of applying backpressure. See Issue #15
type bufferedForwarder struct {
	onward io.Writer
	prefix string
	max    int64

	w *ring.Ring
	r *ring.Ring

	mutex     sync.Mutex
	remaining int64
	done      bool
	err       error
}

func (bf *bufferedForwarder) status() (bool, int64) {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()
	return bf.done, bf.remaining
}

func (bf *bufferedForwarder) inc(by int64) {
	bf.mutex.Lock()
	if bf.remaining+by > bf.max {
		bf.remaining = bf.max
	} else {
		bf.remaining += by
	}
	bf.mutex.Unlock()
}

func (bf *bufferedForwarder) add(record string) (int, error) {
	bf.w.Value = record
	bf.w = bf.w.Next()
	bf.inc(1)
	return len(record) + 1, bf.err
}

func (bf *bufferedForwarder) start() {
	for {
		done, _ := bf.process()
		if done {
			return
		}
	}
}

func (bf *bufferedForwarder) process() (bool, bool) {
	done, remaining := bf.status()
	if done {
		return true, false
	}
	if remaining > 0 {
		v, ok := bf.r.Value.(string)
		if ok {
			bf.forward(v)
		}
		bf.r = bf.r.Next()
		bf.inc(-1)
		return false, true
	}
	return false, false
}

func (bf *bufferedForwarder) stop() error {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()
	bf.done = true
	return bf.err
}

func (bf *bufferedForwarder) forward(record string) {
	_, err := fmt.Fprintf(bf.onward, "%s%s\n", bf.prefix, record)
	if err != nil {
		bf.err = err
	}
}

func newBufferedForwarder(ringBufSize int, conn io.Writer, prefix string) *bufferedForwarder {
	r := ring.New(ringBufSize)
	bf := &bufferedForwarder{
		w:      r,
		r:      r,
		max:    int64(ringBufSize),
		onward: conn,
		prefix: prefix,
	}
	return bf
}
