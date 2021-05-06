package compression

import (
	"bytes"
	"io"
	"sync"

	"github.com/DataDog/zstd"
	lz4 "github.com/cloudflare/golz4"
	"github.com/klauspost/compress/s2"
)

// IDs of build in compression providers
const (
	ProviderIDZstd = 1
	ProviderIDS2   = 2
	ProviderIDLz4  = 3
)

var providerNameToID = map[string]byte{
	"zstd": ProviderIDZstd,
	"s2":   ProviderIDS2,
	"lz4":  ProviderIDLz4,
}

func GetProviderID(name string) (byte, error) {
	providerID, ok := providerNameToID[name]
	if !ok {
		return 0, ErrProviderNotFound
	}
	return providerID, nil
}

func getBuildInProviders() map[byte]Provider {

	noCompression := NewNoCompressionService()
	zstdCompression := NewZstdCompressionService()
	lz4Compression := NewLz4CompressionService()
	s2Compression := NewS2CompressionService()

	providers := map[byte]Provider{
		noCompression.GetID():   noCompression,
		zstdCompression.GetID(): zstdCompression,
		lz4Compression.GetID():  lz4Compression,
		s2Compression.GetID():   s2Compression,
	}

	return providers
}

// NewNoCompressionService creates new instance of compression provider which is not using the compression
func NewNoCompressionService() Provider {
	return &noCompression{
		id: 0,
	}
}

// NewZstdCompressionService creates new instance of compression provider which uses github.com/DataDog/zstd compression method
func NewZstdCompressionService() Provider {
	return &zstdCompression{
		id:               ProviderIDZstd,
		compressionLevel: 3,
	}
}

// NewS2CompressionService creates new instance of compression provider which uses github.com/klauspost/compress/s2 compression method
func NewS2CompressionService() Provider {
	return &s2Compression{
		id: ProviderIDS2,
		readerPool: &sync.Pool{
			New: func() interface{} {
				return s2.NewReader(nil)
			}},
		writterPool: &sync.Pool{
			New: func() interface{} {
				return s2.NewWriter(nil)
			}},
	}
}

// NewS2CompressionService creates new instance of compression provider which uses github.com/klauspost/compress/s2 compression method
func NewLz4CompressionService() Provider {
	return &lz4Compression{
		id: ProviderIDLz4,
	}
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

func (c *noCompression) Configure(params CompressionParams) error {
	return nil
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

// GetID returns compression identifier.
func (c *zstdCompression) Configure(params CompressionParams) error {
	if params == nil {
		return ErrCompressionParamNil
	}

	level, err := params.GetInt(CompressionParamLevel)
	if err != nil {
		return err
	}

	c.compressionLevel = level
	return nil
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

func (c *s2Compression) Configure(params CompressionParams) error {
	return nil
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

func (c *lz4Compression) Configure(params CompressionParams) error {
	return nil
}
