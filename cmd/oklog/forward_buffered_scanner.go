package main

import (
	"bufio"
	"io"
	"sync"

	"github.com/pkg/errors"
)

// boundedBuffer should store a buffer of messages whenever the consumer falls behind the producer
type boundedBuffer interface {
	Put(string)  // Put should not block
	Get() string // Get should block until data is available
}

// BufferedScanner composes a boundedBuffer to make it behave akin to a Scanner
// BufferedScanner's Scan()/Text() is not synchronised (but the composed buffer is)
type BufferedScanner struct {
	buf   boundedBuffer
	val   string //temporary place to store a val after a Scan(). Not synchronised (Use case does not require it)
	err   error
	mutex sync.RWMutex //synchronises access to err
}

func (b *BufferedScanner) Consume(r io.Reader) {
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

func (b *BufferedScanner) Scan() bool {
	b.val = b.buf.Get()
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.err == nil
}

func (b *BufferedScanner) Text() string {
	return b.val
}

func (b *BufferedScanner) Err() error {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.err
}
