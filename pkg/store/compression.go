package store

import (
	"io"
	"strings"

	"compress/gzip"
	"github.com/valyala/gozstd"
)

const (
	compressionNone = ""
	compressionGzip = "gzip"
	compressionZstd = "zstd"

	extCompressionNone = ""
	extCompressionGzip = ".gz"
	extCompressionZstd = ".zst"
)

var compressionToExt map[string]string = map[string]string{
	compressionNone: extCompressionNone,
	compressionGzip: extCompressionGzip,
	compressionZstd: extCompressionZstd,
}

var extToCompression map[string]string = map[string]string{
	extCompressionNone: compressionNone,
	extCompressionGzip: compressionGzip,
	extCompressionZstd: compressionZstd,
}

var compressionExtensionsList []string = []string{
	extCompressionGzip,
	extCompressionZstd,
}

func IsCompressionValid(compression string) bool {
	for k, _ := range compressionToExt {
		if k == compression {
			return true
		}
	}

	return false
}

func isCompressionExt(ext string) bool {
	for _, e := range compressionExtensionsList {
		if e == ext {
			return true
		}
	}

	return false
}

// filepath.Ext adopted for filenames with one or two extensions
func exts(path string) (string, string) {
	items := strings.Split(path, ".")
	if len(items) > 2 {
		return "." + items[len(items)-2], "." + items[len(items)-1]
	} else if len(items) == 2 {
		return "." + items[len(items)-1], ""
	} else {
		return "", ""
	}
}

func firstExt(path string) string {
	e, _ := exts(path)
	return e
}

func secondExt(path string) string {
	_, e := exts(path)
	return e
}

type newCompressedReaderFunc func(io.ReadCloser) (compressedReader, error)
type newCompressedWriterFunc func(io.WriteCloser) (compressedWriter, error)

type compressedReader interface {
	io.ReadCloser
}

type compressedWriter interface {
	io.WriteCloser

	/* total number of bytes written */
	size() int64
}

type compressor struct {
	newReader newCompressedReaderFunc
	newWriter newCompressedWriterFunc
}

var compressors map[string]*compressor = map[string]*compressor{
	compressionNone: &compressor{newCopyReader, newCopyWriter},
	compressionGzip: &compressor{newGzipReader, newGzipWriter},
	compressionZstd: &compressor{newZstdReader, newZstdWriter},
}

func getCompressor(compression string) *compressor {
	c, ok := compressors[compression]
	if !ok {
		return &compressor{newCopyReader, newCopyWriter}
	}

	return c
}

func newCompressedReader(compression string, src io.ReadCloser) (compressedReader, error) {
	return getCompressor(compression).newReader(src)
}

func newCompressedWriter(compression string, dst io.WriteCloser) (compressedWriter, error) {
	return getCompressor(compression).newWriter(dst)
}

type copyReader struct {
	src io.ReadCloser
}

func newCopyReader(src io.ReadCloser) (compressedReader, error) {
	return &copyReader{
		src: src,
	}, nil
}

func (r *copyReader) Read(p []byte) (int, error) {
	return r.src.Read(p)
}

func (r *copyReader) Close() error {
	return r.src.Close()
}

type copyWriter struct {
	dst io.WriteCloser
	sz  int64
}

func newCopyWriter(dst io.WriteCloser) (compressedWriter, error) {
	return &copyWriter{
		dst: dst,
	}, nil
}

func (w *copyWriter) Write(p []byte) (int, error) {
	n, err := w.dst.Write(p)
	w.sz += int64(n)
	return n, err
}

func (w *copyWriter) size() int64 {
	return w.sz
}

func (w *copyWriter) Close() error {
	return w.dst.Close()
}

type gzipReader struct {
	src  io.ReadCloser
	csrc *gzip.Reader
}

func newGzipReader(src io.ReadCloser) (compressedReader, error) {
	r, err := gzip.NewReader(src)
	if err != nil {
		return nil, err
	}

	return &gzipReader{
		src:  src,
		csrc: r,
	}, nil
}

func (r *gzipReader) Read(p []byte) (int, error) {
	return r.csrc.Read(p)
}

func (r *gzipReader) Close() error {
	return r.src.Close()
}

type gzipWriter struct {
	dst  io.WriteCloser
	cw   *countingWriter
	cdst *gzip.Writer
}

func newGzipWriter(dst io.WriteCloser) (compressedWriter, error) {
	cw := &countingWriter{}

	return &gzipWriter{
		dst:  dst,
		cw:   cw,
		cdst: gzip.NewWriter(io.MultiWriter(dst, cw)),
	}, nil
}

func (w *gzipWriter) Write(p []byte) (int, error) {
	return w.cdst.Write(p)
}

func (w *gzipWriter) size() int64 {
	return w.cw.n
}

func (w *gzipWriter) Close() error {
	if err := w.cdst.Close(); err != nil {
		return err
	}

	return w.dst.Close()
}

type zstdReader struct {
	src  io.ReadCloser
	csrc *gozstd.Reader
}

func newZstdReader(src io.ReadCloser) (compressedReader, error) {
	return &zstdReader{
		src:  src,
		csrc: gozstd.NewReader(src),
	}, nil
}

func (r *zstdReader) Read(p []byte) (int, error) {
	return r.csrc.Read(p)
}

func (r *zstdReader) Close() error {
	return r.src.Close()
}

type zstdWriter struct {
	dst  io.WriteCloser
	cw   *countingWriter
	cdst *gozstd.Writer
}

func newZstdWriter(dst io.WriteCloser) (compressedWriter, error) {
	cw := &countingWriter{}

	return &zstdWriter{
		dst:  dst,
		cw:   cw,
		cdst: gozstd.NewWriter(io.MultiWriter(dst, cw)),
	}, nil
}

func (w *zstdWriter) Write(p []byte) (int, error) {
	return w.cdst.Write(p)
}

func (w *zstdWriter) size() int64 {
	return w.cw.n
}

func (w *zstdWriter) Close() error {
	if err := w.cdst.Close(); err != nil {
		return err
	}

	w.cdst.Release()

	return w.dst.Close()
}
