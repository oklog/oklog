package forward

// RingBuffer is a fixed-length ring buffer. It can be used by a forwarder, to 'drop' messages instead of applying backpressure. See Issue #15
type ringBuffer struct {
	maxSize int
	ch      chan string // a buffered channel is used to buffer records
}

// Put() processes the record without blocking.
func (b *ringBuffer) Put(record string) {
	for {
		select {
		case b.ch <- record:
			return
		default:
			// when buffer full, drop oldest record
			<-b.ch
		}
	}

}

// Get() blocks until data is available
func (b *ringBuffer) Get() string {
	return <-b.ch
}

func NewRingBuffer(bufSize int) *ringBuffer {
	if bufSize < 0 {
		panic("buffer size should not be less than zero")
	}
	b := &ringBuffer{
		maxSize: bufSize,
		ch:      make(chan string, bufSize),
	}
	return b
}
