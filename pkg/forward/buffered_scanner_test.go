package forward

import (
	"io"
	"testing"
)

func TestBufferedScanner(t *testing.T) {
	sb := NewRingBuffer(4)
	bs := bufferedScanner{
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
		_, text, err := bs.Next()
		if err != nil {
			t.Errorf("Buffer should not return error. %v", err)
		}
		t.Log("Received", text)
		if text != e {
			t.Errorf("Buffer should return a specific value [%s] but received [%s]", e, text)
		}
	}
}
