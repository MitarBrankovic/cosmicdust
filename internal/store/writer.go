package store

import (
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// countingWriter counts bytes and feeds a checksum on the way to the file: the
// CRC the control plane deduplicates on with the name.
type countingWriter struct {
	w   io.Writer
	n   int64
	crc uint32
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	c.crc = crc32.Update(c.crc, castagnoli, p[:n])
	return n, err
}

// row is what both tables have in common for the writer.
type row interface {
	LogRow | EventRow
}

func tsOf[T row](r *T) time.Time {
	switch v := any(r).(type) {
	case *LogRow:
		return v.TS
	case *EventRow:
		return v.TS
	}
	return time.Time{}
}

func rawSizeOf[T row](r *T) int64 {
	switch v := any(r).(type) {
	case *LogRow:
		return v.rawSize()
	case *EventRow:
		return v.rawSize()
	}
	return 0
}

// partWriter writes one Parquet file in pieces and commits it atomically:
// memory is bounded by a row group, not by the file.
type partWriter[T row] struct {
	dir, path, tmp string
	f              *os.File
	cw             *countingWriter
	pw             *parquet.GenericWriter[T]
	pool           *writerPool[T]
	rows, raw      int64
	min, max       time.Time
}

// writerPool keeps Parquet writers from one file to the next. A new writer per
// file allocated its column buffers anew: 78 MB in two minutes at 1,000
// lines/s. A conversion may write a few files at once.
type writerPool[T row] struct {
	free []*parquet.GenericWriter[T]
}

func (p *writerPool[T]) get(out io.Writer, opts []parquet.WriterOption) *parquet.GenericWriter[T] {
	if n := len(p.free); n > 0 {
		w := p.free[n-1]
		p.free = p.free[:n-1]
		w.Reset(out)
		return w
	}
	return parquet.NewGenericWriter[T](out, opts...)
}

func (p *writerPool[T]) put(w *parquet.GenericWriter[T]) {
	if len(p.free) < maxOpenParts {
		p.free = append(p.free, w)
	}
}

// eventPageBytes is the page size of the events table.
const eventPageBytes = 64 << 10

// poolOf is the store's pool of writers for table rows T.
func poolOf[T row](s *Store) *writerPool[T] {
	if p, ok := any(&s.logWriters).(*writerPool[T]); ok {
		return p
	}
	return any(&s.eventWriters).(*writerPool[T])
}

// newPartWriter starts a file, named name once committed.
func newPartWriter[T row](s *Store, dir, name string) (*partWriter[T], error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	cw := &countingWriter{w: f}
	pool := poolOf[T](s)
	page := s.opts.PageBytes
	if _, events := any((*T)(nil)).(*EventRow); events {
		// A few events per file: pages of the size logs need kept ~3 MB of
		// column buffers for them.
		page = min(page, eventPageBytes)
	}
	pw := pool.get(cw, []parquet.WriterOption{
		parquet.Compression(s.codec),
		parquet.MaxRowsPerRowGroup(s.opts.RowGroupRows),
		parquet.PageBufferSize(page),
		parquet.CreatedBy("cosmicdust", "", ""),
	})
	return &partWriter[T]{dir: dir, path: path, tmp: path + ".tmp", f: f, cw: cw, pw: pw, pool: pool}, nil
}

func (w *partWriter[T]) write(rows []T) error {
	for i := range rows {
		ts := tsOf(&rows[i]).UTC()
		if w.rows == 0 || ts.Before(w.min) {
			w.min = ts
		}
		if w.rows == 0 || ts.After(w.max) {
			w.max = ts
		}
		w.raw += rawSizeOf(&rows[i])
		w.rows++
	}
	_, err := w.pw.Write(rows)
	return err
}

// commit finishes the file: footer, fsync, rename into place, fsync of the
// directory. Only then does it exist under its name.
func (w *partWriter[T]) commit() (manifestFile, error) {
	err := w.pw.Close()
	w.pool.put(w.pw)
	w.pw = nil
	if err != nil {
		w.abort()
		return manifestFile{}, err
	}
	if err := w.f.Sync(); err != nil {
		w.abort()
		return manifestFile{}, err
	}
	if err := w.f.Close(); err != nil {
		_ = os.Remove(w.tmp)
		return manifestFile{}, err
	}
	if err := os.Rename(w.tmp, w.path); err != nil {
		_ = os.Remove(w.tmp)
		return manifestFile{}, err
	}
	if err := syncDir(w.dir); err != nil {
		return manifestFile{}, err
	}
	return manifestFile{
		Name:      filepath.Base(w.path),
		Rows:      w.rows,
		BytesRaw:  w.raw,
		Bytes:     w.cw.n,
		CRC32C:    w.cw.crc,
		MinTS:     w.min,
		MaxTS:     w.max,
		CreatedAt: time.Now().UTC(),
	}, nil
}

// abort drops the file. The writer goes back to the pool: Reset clears what
// it held.
func (w *partWriter[T]) abort() {
	if w.pw != nil {
		w.pool.put(w.pw)
		w.pw = nil
	}
	_ = w.f.Close()
	_ = os.Remove(w.tmp)
}

// rowReader streams one Parquet file, a chunk at a time.
type rowReader[T row] struct {
	f   *os.File
	r   *parquet.GenericReader[T]
	buf []T
	pos int
	err error
}

func openRows[T row](path string, chunk int) (*rowReader[T], error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &rowReader[T]{f: f, r: parquet.NewGenericReader[T](f), buf: make([]T, 0, chunk)}, nil
}

// next returns the next row, or nil at the end or on an error (err).
func (r *rowReader[T]) next() *T {
	if r.pos >= len(r.buf) {
		if r.err != nil {
			return nil
		}
		n, err := r.r.Read(r.buf[:cap(r.buf)])
		r.buf, r.pos = r.buf[:n], 0
		if err != nil && !errors.Is(err, io.EOF) {
			r.err = err
		}
		if n == 0 {
			if r.err == nil && err != nil {
				r.err = io.EOF
			}
			return nil
		}
		if errors.Is(err, io.EOF) {
			r.err = io.EOF // this chunk is the last
		}
	}
	p := &r.buf[r.pos]
	r.pos++
	return p
}

func (r *rowReader[T]) close() {
	_ = r.r.Close()
	_ = r.f.Close()
}

// failed is the reader's error, io.EOF being none.
func (r *rowReader[T]) failed() error {
	if errors.Is(r.err, io.EOF) {
		return nil
	}
	return r.err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
