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

func testBuffer(t testing.TB, b BoundedBuffer, bufferSize int) {
	msgCount := 10
	b.Put(fmt.Sprintf("%02d", 0))
	b.Get()
	for i := 0; i < msgCount; i++ {
		b.Put(fmt.Sprintf("%02d", i))
	}
	for i := 0; i < bufferSize; i++ {
		b.Get()
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
