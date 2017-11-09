package main

import (
	"bufio"
	"io"
)

func newScanner(r *bufio.Scanner, queueSize int, drop bool) *scanner {
	s := &scanner{
		s:    r,
		ch:   make(chan scanItem, queueSize),
		drop: drop,
	}
	go s.loop()
	return s
}

type scanItem struct {
	Val string
	Err error
}

type scanner struct {
	s    *bufio.Scanner
	ch   chan scanItem
	drop bool
}

func (s *scanner) Next() (string, error) {
	item := <-s.ch
	return item.Val, item.Err
}

func (s *scanner) loop() {
	for s.s.Scan() {
		item := scanItem{s.s.Text(), nil}
		select {
		case s.ch <- item:
		default:
			if !s.drop {
				s.ch <- item
			}
		}
	}
	err := s.s.Err()
	if err == nil {
		err = io.EOF
	}
	s.ch <- scanItem{"", err}
}
