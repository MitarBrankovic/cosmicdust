package store

import (
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/format"
)

// zstdCodec is Parquet's zstd with one encoder that lives as long as the store.
//
// parquet-go keeps its encoders in a pool the garbage collector empties: with
// a collection every few seconds and a conversion every ten, every conversion
// made new encoders, each with a new history window - 56 MB of allocation in
// two minutes at 1,000 lines/s, more than the lines themselves. One
// writer goroutine needs one encoder. It is built for low memory with a window
// of one page (1 MiB), at the fastest level, which the compression
// measurements chose.
type zstdCodec struct {
	mu  sync.Mutex
	enc *zstd.Encoder
	dec *zstd.Decoder
}

func newZstdCodec(window int) (*zstdCodec, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithLowerEncoderMem(true),
		zstd.WithWindowSize(window),
		zstd.WithZeroFrames(true),
		zstd.WithEncoderCRC(false),
	)
	if err != nil {
		return nil, err
	}
	return &zstdCodec{enc: enc}, nil
}

func (c *zstdCodec) String() string { return "ZSTD" }

func (c *zstdCodec) CompressionCodec() format.CompressionCodec { return format.Zstd }

func (c *zstdCodec) Encode(dst, src []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enc.EncodeAll(src, dst[:0]), nil
}

func (c *zstdCodec) Decode(dst, src []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dec == nil {
		d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true))
		if err != nil {
			return nil, err
		}
		c.dec = d
	}
	return c.dec.DecodeAll(src, dst[:0])
}

var _ compress.Codec = (*zstdCodec)(nil)
