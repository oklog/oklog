package forward

import (
	"fmt"
	"io"
	"testing"
)

func TestBufferedScanner(t *testing.T) {
	sb := NewRingBuffer(4)
	bs := BufferedScanner{
		Buf: sb,
	}
	exp := []string{"hi", "ho", "yi", "yo"}
	pr, pw := io.Pipe()
	go bs.Consume(pr)
	for _, e := range exp {
		pw.Write([]byte(e + "\n"))
	}

	for i, e := range exp {
		if i >= sb.maxSize-1 {
			break
		}
		ok := bs.Scan()
		if !ok {
			t.Errorf("Buffer scan should return a value")
		}
		fmt.Println("Received", bs.Text())
		if bs.Text() != e {
			t.Errorf("Buffer should return a specific value [%s] but received [%s]", e, bs.Text())
		}
	}
}
