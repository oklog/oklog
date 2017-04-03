package main

import (
	"bufio"
	"io"
	"sync"

	"github.com/pkg/errors"
)

// bounded buffer should store messages whenever a consumer falls behind
type boundedBuffer interface {
	Put(string)  // Put should not block
	Get() string // Get should block until data is available
}

// bufferedScanner composes a buffer to make it behave akin to a Scanner
// bufferedScanner does not need to be thread-safe (but the composed buffer does)
type bufferedScanner struct {
	buf   boundedBuffer
	val   string //temporary place to store a val after a Scan()
	err   error
	mutex sync.RWMutex
}

func (b *bufferedScanner) Consume(r io.Reader) {
	bs := bufio.NewScanner(r)
	ok := bs.Scan()
	for ok {
		record := bs.Text()
		b.buf.Put(record)
		ok = bs.Scan()
	}
	if !ok {
		b.mutex.Lock()
		if bs.Err() != nil {
			b.err = errors.Wrapf(bs.Err(), "Error reading from input")
		} else {
			b.err = errors.Errorf("Error reading from input")
		}
		b.mutex.Unlock()
	}
}

func (b *bufferedScanner) Scan() bool {
	b.val = b.buf.Get()
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.err == nil
}

func (b *bufferedScanner) Text() string {
	return b.val
}

func (b *bufferedScanner) Err() error {
	return b.err
}
