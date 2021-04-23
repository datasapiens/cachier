package compression

import (
	"bytes"
	"io"
	"sync"

	"github.com/DataDog/zstd"
	lz4 "github.com/cloudflare/golz4"
	"github.com/klauspost/compress/s2"
)

// NoCompressionService uses no compression
var NoCompressionService *noCompression = &noCompression{
	id: 0,
}

// ZstdCompressionService uses  zstd compression method
// github.com/DataDog/zstd
var ZstdCompressionService *zstdCompression = &zstdCompression{
	id:               1,
	compressionLevel: 3,
}

// S2CompressionService uses s2/snappy compression method
// github.com/klauspost/compress/s2
var S2CompressionService *s2Compression = &s2Compression{
	id: 2,
	readerPool: &sync.Pool{
		New: func() interface{} {
			return s2.NewReader(nil)
		}},
	writterPool: &sync.Pool{
		New: func() interface{} {
			return s2.NewWriter(nil)
		}},
}

// Lz4CompressionService uses lz4 compression
// github.com/cloudflare/golz4
var Lz4CompressionService *lz4Compression = &lz4Compression{
	id: 3,
}

type noCompression struct {
	id byte
}

// Compress returns src without any changes.
func (c noCompression) Compress(src []byte) ([]byte, error) {
	return src, nil
}

// Decompress returns src without any changes.
func (c noCompression) Decompress(src []byte, dstSize int) ([]byte, error) {
	return src, nil
}

// GetID returns compression identifier.
func (c noCompression) GetID() byte {
	return c.id
}

type zstdCompression struct {
	id               byte
	compressionLevel int
}

// Compress compresses src  using zstd method
func (c zstdCompression) Compress(src []byte) ([]byte, error) {

	output, err := zstd.CompressLevel(nil, src, c.compressionLevel)
	if err != nil {
		return nil, err
	}

	return output, nil
}

// Decompress decompresses src  using zstd method
func (c zstdCompression) Decompress(src []byte, dstSize int) ([]byte, error) {
	dst := make([]byte, 0, dstSize)
	output, err := zstd.Decompress(dst, src)
	if err != nil {
		return nil, err
	}
	return output, nil
}

// GetID returns compression identifier.
func (c zstdCompression) GetID() byte {
	return c.id
}

// SetCompressionLevel allows to set compression level
func (c *zstdCompression) SetCompressionLevel(level int) *zstdCompression {
	c.compressionLevel = level
	return c
}

type s2Compression struct {
	id          byte
	writterPool *sync.Pool
	readerPool  *sync.Pool
}

// Compress compresses src  using s2 method
func (c s2Compression) Compress(src []byte) ([]byte, error) {
	enc := c.writterPool.Get().(*s2.Writer)
	defer c.writterPool.Put(enc)
	var out bytes.Buffer
	enc.Reset(&out)
	err := enc.EncodeBuffer(src)
	if err != nil {
		enc.Close()
		return nil, err
	}
	// Blocks until compression is done.
	err = enc.Close()
	if err != nil {
		return nil, err
	}
	return out.Bytes(), err
}

// Decompress decompresses src  using s2 method
func (c s2Compression) Decompress(src []byte, dstSize int) ([]byte, error) {
	dec := c.readerPool.Get().(*s2.Reader)
	defer c.readerPool.Put(dec)
	r := bytes.NewReader(src)
	dec.Reset(r)
	var out bytes.Buffer
	_, err := io.Copy(&out, dec)

	if err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

// GetID returns compression identifier.
func (c s2Compression) GetID() byte {
	return c.id
}

type lz4Compression struct {
	id byte
}

// Compress compresses src  using lz4 method poreted from C
func (c lz4Compression) Compress(src []byte) ([]byte, error) {
	output := make([]byte, lz4.CompressBound(src))
	outSize, err := lz4.Compress(src, output)
	if err != nil {
		return nil, err
	}

	return output[:outSize], nil
}

// Decompress decompresses src  using lz4 method
func (c lz4Compression) Decompress(src []byte, dstSize int) ([]byte, error) {
	dst := make([]byte, dstSize)
	err := lz4.Uncompress(src, dst)
	if err != nil {
		return nil, err
	}

	return dst, nil
}

// GetID returns compression identifier.
func (c lz4Compression) GetID() byte {
	return c.id
}
