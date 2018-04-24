package record

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"regexp"

	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/ulid"
)

// ErrIllegalTopicName is returned if a topic's character sequence is invalid.
var ErrIllegalTopicName = errors.New("illegal topic name")

// Reader emits records.
// It returns io.EOF if the underlying record source has no more data.
type Reader interface {
	Read() (record []byte, err error)
}

// ReaderFunc is a function implementing a Reader.
type ReaderFunc func() ([]byte, error)

func (f ReaderFunc) Read() ([]byte, error) {
	return f()
}

// ReadCloser is a Reader that can be closed.
type ReadCloser interface {
	Reader
	io.Closer
}

// RecordReaderFactory returns a new RecordReader backed by the given reader.
type ReaderFactory func(io.Reader) Reader

// NewReader returns a new Reader over an io.Reader.
func NewReader(r io.Reader) Reader {
	br := bufio.NewReader(r)

	return ReaderFunc(func() ([]byte, error) {
		return br.ReadBytes('\n')
	})
}

// NewReaderSize returns a new Reader of an io.Reader with the given
// buffer size.
func NewReaderSize(r io.Reader, sz int) Reader {
	br := bufio.NewReaderSize(r, sz)

	return ReaderFunc(func() ([]byte, error) {
		return br.ReadBytes('\n')
	})
}

type recordReadCloser struct {
	c io.Closer
	r Reader
}

func NopCloser(r Reader) ReadCloser {
	return recordReadCloser{r: r}
}

func (rc recordReadCloser) Close() error {
	if rc.c == nil {
		return nil
	}
	return rc.c.Close()
}

func (rc recordReadCloser) Read() ([]byte, error) {
	return rc.r.Read()
}

func NewFileReader(fsys fs.Filesystem, filename string) (ReadCloser, error) {
	f, err := fsys.Open(filename)
	if err != nil {
		return nil, err
	}
	return &recordReadCloser{c: f, r: NewReader(f)}, nil
}

func NewFileReaderSize(fsys fs.Filesystem, filename string, sz int) (ReadCloser, error) {
	f, err := fsys.Open(filename)
	if err != nil {
		return nil, err
	}
	return &recordReadCloser{c: f, r: NewReaderSize(f, sz)}, nil
}

// NewDynamicReader returns a record reader that expects each input record from r
// to start with a space-delimited topic identifier.
func NewDynamicReader(r io.Reader) Reader {
	br := bufio.NewReader(r)

	return ReaderFunc(func() ([]byte, error) {
		l, err := br.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		// Validate that a correct topic prefix exists.
		if i := bytes.IndexByte(l, ' '); i < 0 || !IsValidTopic(l[:i]) {
			return nil, ErrIllegalTopicName
		}
		return l, nil
	})
}

// StaticReaderFactory returns a RecordReaderFactory that prefixes all records
// with the given bytes as topic.
// Callers must ensure the topic identifier is valid.
func StaticReaderFactory(topic []byte) ReaderFactory {
	topic = append(topic, ' ')

	return func(r io.Reader) Reader {
		br := bufio.NewReader(r)

		return ReaderFunc(func() ([]byte, error) {
			l, err := br.ReadBytes('\n')
			if err != nil {
				return nil, err
			}
			return append(topic, l...), nil
		})
	}
}

// IsValidTopic ensures that the a topic only contains allowed characters. It implements
// the regex [a-zA-Z0-9][a-zA-Z0-9_-]*
// It takes a byte slice to avoid memory allocations at the caller.
func IsValidTopic(b []byte) bool {
	for i, c := range b {
		if i > 0 && (c == '_' || c == '-') {
			continue
		}
		if ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') {
			continue
		}
		return false
	}
	return len(b) > 0
}

type Filter func([]byte) bool

const ulidTimeSize = 10 // bytes

func FilterPlain(q []byte) Filter {
	return func(b []byte) bool {
		return len(b) > ulid.EncodedSize && bytes.Contains(b[ulid.EncodedSize+1:], q)
	}
}

func FilterRegex(q *regexp.Regexp) Filter {
	return func(b []byte) bool {
		return len(b) > ulid.EncodedSize && q.Match(b[ulid.EncodedSize+1:])
	}
}

func FilterBoundedPlain(from, to ulid.ULID, q []byte) Filter {
	fromBytes, _ := from.MarshalText()
	fromBytes = fromBytes[:ulidTimeSize]
	toBytes, _ := to.MarshalText()
	toBytes = toBytes[:ulidTimeSize]
	return func(b []byte) bool {
		return len(b) > ulid.EncodedSize &&
			bytes.Compare(b[:ulidTimeSize], fromBytes) >= 0 &&
			bytes.Compare(b[:ulidTimeSize], toBytes) <= 0 &&
			bytes.Contains(b[ulid.EncodedSize+1:], q)
	}
}

func FilterBoundedRegex(from, to ulid.ULID, q *regexp.Regexp) Filter {
	fromBytes, _ := from.MarshalText()
	fromBytes = fromBytes[:ulidTimeSize]
	toBytes, _ := to.MarshalText()
	toBytes = toBytes[:ulidTimeSize]
	return func(b []byte) bool {
		return len(b) > ulid.EncodedSize &&
			bytes.Compare(b[:ulidTimeSize], fromBytes) >= 0 &&
			bytes.Compare(b[:ulidTimeSize], toBytes) <= 0 &&
			q.Match(b[ulid.EncodedSize+1:])
	}
}
