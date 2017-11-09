package main

import (
	"bufio"
	"io"
	"testing"
	"time"
)

func TestScan(t *testing.T) {

	interval := 10 * time.Millisecond
	{
		done := make(chan bool, 1)

		r, w := io.Pipe()
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 4), 4) // avoid too big buffer size
		s := newScanner(sc, 0, false)

		go func() {
			defer func() { done <- true }()
			w.Write([]byte("foo\n"))
			start := time.Now()
			w.Write([]byte("bar\n"))
			w.Write([]byte("bar\n"))
			w.Write([]byte("bar\n"))
			elapsed := time.Since(start)
			if elapsed <= interval {
				t.Errorf("elapsed(%v) should gt sleep interval(%v)", elapsed, interval)
			}
		}()

		{
			v, err := s.Next()
			if err != nil {
				t.Errorf("err(%v) should be nil", err)
			}
			if v != "foo" {
				t.Errorf("(%v) != (%v)", v, "foo")
			}
		}

		time.Sleep(interval)

		{
			s.Next()
			s.Next()
			s.Next()
		}

		w.Close()
		r.Close()
		<-done
	}

	{
		done := make(chan bool, 1)

		r, w := io.Pipe()

		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 4), 4) // avoid too big buffer size
		s := newScanner(sc, 1, true)

		go func() {
			defer func() { done <- true }()
			w.Write([]byte("foo\n"))
			start := time.Now()
			w.Write([]byte("bar\n"))
			w.Write([]byte("bar\n"))
			w.Write([]byte("bar\n"))
			elapsed := time.Since(start)
			if elapsed > interval {
				t.Errorf("elapsed(%v) should lt sleep interval(%v)", elapsed, interval)
			}
			w.CloseWithError(io.EOF)
		}()

		{
			v, err := s.Next()
			if err != nil {
				t.Errorf("err(%v) should be nil", err)
			}
			if v != "foo" {
				t.Errorf("(%v) != (%v)", v, "foo")
			}
		}

		time.Sleep(interval)
		{
			v, err := s.Next()
			if err != nil {
				t.Errorf("err(%v) should be nil", err)
			}
			if v != "bar" {
				t.Errorf("(%v) != (%v)", v, "bar")
			}

			if _, err := s.Next(); err != io.EOF {
				t.Errorf("err(%v) should be %v", err, io.EOF)
			}
		}

		r.Close()
		<-done
	}
}
