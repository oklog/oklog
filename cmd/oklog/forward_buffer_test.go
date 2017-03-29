package main

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBufferedForwarderRemaining(t *testing.T) {
	//onwardWriter := bytes.NewBufferString("")
	msgCount := 10
	bufferSize := 5
	bf := newRingBuffer(5)
	bf.Put(fmt.Sprintf("%02d", 0))
	if bf.Remaining() != 1 {
		t.Errorf("remaining should be 1")
	}
	bf.Get()
	if bf.Remaining() != 0 {
		t.Errorf("remaining should be 0")
	}
	for i := 0; i < msgCount; i++ {
		bf.Put(fmt.Sprintf("%02d", i))
	}
	//verifies that remainder matches bufferSize rather than msgCount
	if bf.Remaining() != int64(bufferSize) {
		t.Errorf("remaining should be %d", bufferSize)
	}

}

func TestBufferedForwarder(t *testing.T) {
	onwardWriter := bytes.NewBufferString("")
	msgCount := 10
	bufferSize := 5
	prefix := "pfx "
	bf := newRingBuffer(5)
	for i := 0; i < msgCount; i++ {
		bf.Put(fmt.Sprintf("%03d", i))
	}
	for i := 0; i < msgCount; i++ {
		_ = bf.Get()
		if bf.Remaining() < 1 {
			break
		}
	}

	s := onwardWriter.String()
	if len(s) != (3+len(prefix)+1)*bufferSize {
		//	t.Errorf("Expected to receive 5 messages")
	}
	t.Logf("%s", s)
	t.Logf("%d", len(s))
}
