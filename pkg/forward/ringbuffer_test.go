package forward

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkRingBufferPutGet(b *testing.B) {
	bufferSize := b.N / 2
	if bufferSize < 1 {
		bufferSize = 1
	}
	buf := NewRingBuffer(bufferSize)
	for i := 0; i < b.N; i++ {
		buf.Put(fmt.Sprintf("%02d", i))
		buf.Get()
	}
}

func BenchmarkRingBufferPut10Get5(b *testing.B) {
	bufferSize := 5
	if bufferSize < 1 {
		bufferSize = 1
	}
	putsPerIteration := bufferSize * 2
	getsPerIteration := putsPerIteration
	if getsPerIteration > bufferSize {
		getsPerIteration = bufferSize
	}

	buf := NewRingBuffer(bufferSize)
	for i := 0; i < b.N; i++ {
		for j := 0; j < putsPerIteration; j++ {
			buf.Put(fmt.Sprintf("%02d", i))
		}
		for j := 0; j < getsPerIteration; j++ {
			buf.Get()
		}
	}
}

func TestRingBuffer(t *testing.T) {
	var b BoundedBuffer
	bufferSize := 5
	b = NewRingBuffer(bufferSize)
	testBuffer(t, b, bufferSize)
}

// verifies that most recent messages are kept in the buffer
func TestRingBufferMostRecent(t *testing.T) {
	var tests = []struct {
		Name          string
		Size          int
		Input         []string
		FirstExpected string
	}{
		{"Basic", 4, []string{"0", "1", "2", "3"}, "0"},
		{"AroundCorner", 3, []string{"0", "1", "2", "3"}, "1"},
		{"AroundCornerTwice", 3, []string{"0", "1", "2", "3", "4", "5", "6"}, "4"},
	}
	for _, test := range tests {
		b := NewRingBuffer(test.Size)
		for _, message := range test.Input {
			b.Put(message)
		}
		res := b.Get()
		if res != test.FirstExpected {
			t.Errorf("[%s] Error: %s does not match expected %s", test.Name, res, test.FirstExpected)
		} else {
			t.Logf("[%s] OK", test.Name)
		}
	}
}

func TestRingBufferBlocksWhenEmpty(t *testing.T) {
	bufferSize := 3
	buf := NewRingBuffer(bufferSize)

	buf.Put("1")

	c := make(chan struct{})
	f := func() {
		buf.Get()
		c <- struct{}{}
	}
	//non-empty - should not block
	go f()
	blocked := false
	select {
	case <-c:
		blocked = false
	case <-time.After(10 * time.Millisecond):
		blocked = true
	}
	if blocked {
		t.Errorf("Should not have blocked")
	}
	//empty - should block
	go f()
	select {
	case <-c:
		blocked = false
	case <-time.After(10 * time.Millisecond):
		blocked = true
	}
	if !blocked {
		t.Errorf("Should have blocked")
	}

}

func TestRingBufferLen(t *testing.T) {
	cap := 10
	b := NewRingBuffer(cap)
	if b.Len() != 0 {
		t.Errorf("Incorrect length %d != %d", b.Len(), 0)
	}
	c := 5
	for i := 0; i < c; i++ {
		b.Put(fmt.Sprintf("entry %d", i))
	}
	if b.Len() != c {
		t.Errorf("Incorrect length %d != %d", b.Len(), c)
	}
	//drain
	for i := 0; i < c; i++ {
		b.Get()
	}
	if b.Len() != 0 {
		t.Errorf("Incorrect length %d != %d", b.Len(), 0)
	}

	for i := 0; i < c; i++ {
		b.Put(fmt.Sprintf("entry %d", i))
	}
	//test over capacity
	for i := 0; i < c; i++ {
		b.Put(fmt.Sprintf("entry %d", i))
	}

	if b.Len() != 10 {
		t.Errorf("Incorrect length %d (expected: %d, first: %d, last: %d, cap: %d)", b.Len(), 10, b.first, b.last(), cap)
	}

}
