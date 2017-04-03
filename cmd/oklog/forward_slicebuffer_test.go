package main

import (
	"fmt"
	"testing"
)

func TestSliceBuffer(t *testing.T) {
	testSliceBuffer(t)
}

func BenchmarkSliceBuffer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testSliceBuffer(b)
	}
}

func testSliceBuffer(t testing.TB) {
	var b buffer
	bufferSize := 5
	b = newSliceBuffer(bufferSize)
	testBuffer(t, b, bufferSize)
}

func testBuffer(t testing.TB, b buffer, bufferSize int) {
	msgCount := 10
	b.Put(fmt.Sprintf("%02d", 0))
	b.Get()
	for i := 0; i < msgCount; i++ {
		b.Put(fmt.Sprintf("%02d", i))
	}
	for i := 0; i < bufferSize; i++ {
		t.Logf(b.Get())
	}
}

func TestSliceBufferLen(t *testing.T) {
	cap := 10
	b := newSliceBuffer(cap)
	if b.Len() != 0 {
		t.Errorf("Incorrect length %d != %d", b.Len(), 0)
	}
	c := 5
	for i := 0; i < c; i++ {
		b.Put(fmt.Sprintf("entry %d", i))
	}
	t.Logf("length %d (first: %d, last: %d, cap: %d)", b.Len(), b.first, b.last, cap)
	if b.Len() != c {
		t.Errorf("Incorrect length %d != %d", b.Len(), c)
	}
	//drain
	for i := 0; i < c; i++ {
		b.Get()
	}
	t.Logf("length %d (first: %d, last: %d, cap: %d)", b.Len(), b.first, b.last, cap)
	if b.Len() != 0 {
		t.Errorf("Incorrect length %d != %d", b.Len(), 0)
	}

	for i := 0; i < c; i++ {
		b.Put(fmt.Sprintf("entry %d", i))
	}
	t.Logf("length %d (first: %d, last: %d, cap: %d)", b.Len(), b.first, b.last, cap)
	//test over capacity
	for i := 0; i < c; i++ {
		b.Put(fmt.Sprintf("entry %d", i))
	}

	t.Logf("length %d (first: %d, last: %d, cap: %d)", b.Len(), b.first, b.last, cap)
	if b.Len() != 10 {
		t.Errorf("Incorrect length %d (expected: %d, first: %d, last: %d, cap: %d)", b.Len(), 10, b.first, b.last, cap)
	}

}
