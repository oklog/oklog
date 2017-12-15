package forward

import (
	"bufio"
	"io"
	"sync"

	"github.com/pkg/errors"
)

// BoundedBuffer should store a buffer of messages whenever the consumer falls behind the producer
type BoundedBuffer interface {
	Put(string)  // Put should not block
	Get() string // Get should block until data is available
}

// bufferedScanner composes a boundedBuffer to make it behave akin to a Scanner
// bufferedScanner's Scan()/Text() is not synchronised (but the composed buffer is)
type bufferedScanner struct {
	Buf   BoundedBuffer
	err   error
	mutex sync.RWMutex //synchronises access to err
}

func NewBufferedScanner(b BoundedBuffer) *bufferedScanner {
	return &bufferedScanner{
		Buf: b,
	}
}

func (b *bufferedScanner) Consume(r io.Reader) {
	bs := bufio.NewScanner(r)
	ok := bs.Scan()
	for ok {
		record := bs.Text()
		b.Buf.Put(record)
		ok = bs.Scan()
	}
	if !ok {
		b.mutex.Lock()
		b.err = errors.Errorf("Error reading from input")
		b.mutex.Unlock()
	}
}

func (b *bufferedScanner) Next() (bool, string, error) {
	val := b.Buf.Get()
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.err != nil, val, b.err
}
