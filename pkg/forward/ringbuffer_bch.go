package forward

// RingBufferBCH is a fixed-length ring buffer. It can be used by a forwarder, to 'drop' messages instead of applying backpressure. See Issue #15
type RingBufferBCH struct {
	maxSize int
	ch      chan string // a buffered channel is used to buffer records
}

// Put() processes the record without blocking.
func (b *RingBufferBCH) Put(record string) {
	for {
		select {
		case b.ch <- record:
			return
		default:
			<-b.ch
		}
	}

}

// Get() blocks until data is available
func (b *RingBufferBCH) Get() string {
	return <-b.ch
}

func NewRingBufferBCH(bufSize int) *RingBufferBCH {
	if bufSize < 1 {
		panic("buffer size should be greater than zero")
	}
	b := &RingBufferBCH{
		maxSize: bufSize,
		ch:      make(chan string, bufSize),
	}
	return b
}
