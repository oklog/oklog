package ioext

import "io"

// OffsetReader returns a io.Reader that reads from the given io.ReaderAt
// starting at the given offset.
func OffsetReader(r io.ReaderAt, off int64) io.Reader {
	return &offsetReader{r, off}
}

type offsetReader struct {
	r   io.ReaderAt
	off int64
}

func (r *offsetReader) Read(p []byte) (int, error) {
	n, err := r.r.ReadAt(p, r.off)
	r.off += int64(n)
	return n, err
}
