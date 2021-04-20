package compression

import (
	"bytes"
	"encoding/binary"
	"io"

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
	return append(src, c.id), nil
}

// Decompress returns src without any changes.
func (c noCompression) Decompress(src []byte) ([]byte, error) {
	return src[:len(src)-methodIDLengthInByte], nil
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

	metadata, err := getMetadata(c.id, len(src))
	if err != nil {
		return nil, err
	}

	return append(output, metadata...), nil
}

// Decompress decompresses src  using zstd method
func (c zstdCompression) Decompress(src []byte) ([]byte, error) {
	input, dstSize, err := extractMetadata(src)
	if err != nil {
		return nil, err
	}
	dst := make([]byte, 0, dstSize)
	output, err := zstd.Decompress(dst, input)
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
func (c *zstdCompression) SetCompressionLevel(level int) *zstdCompression {
	c.compressionLevel = level
	return c
}

type s2Compression struct {
	id byte
}

// Compress compresses src  using lz4 method poreted from C
func (c s2Compression) Compress(src []byte) ([]byte, error) {

	var out bytes.Buffer
	enc := s2.NewWriter(&out)
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
	metadata, err := getMetadata(c.id, len(src))
	if err != nil {
		return nil, err
	}
	_, err = out.Write(metadata)
	return out.Bytes(), err
}

// Decompress decompresses src  using lz4 method
func (c s2Compression) Decompress(src []byte) ([]byte, error) {
	input, _, err := extractMetadata(src)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(input)
	dec := s2.NewReader(r)
	var out bytes.Buffer
	_, err = io.Copy(&out, dec)

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

	metadata, err := getMetadata(c.id, len(src))
	if err != nil {
		return nil, err
	}

	return append(output[:outSize], metadata...), nil
}

// Decompress decompresses src  using lz4 method
func (c lz4Compression) Decompress(src []byte) ([]byte, error) {
	input, dstSize, err := extractMetadata(src)
	if err != nil {
		return nil, err
	}
	dst := make([]byte, dstSize)
	err = lz4.Uncompress(input, dst)
	if err != nil {
		return nil, err
	}

	return dst, nil
}

// GetID returns compression identifier.
func (c lz4Compression) GetID() byte {
	return c.id
}

func getMetadata(methodIdentifier byte, inputLenght int) ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, 0, metadataSizeInByte))
	err := binary.Write(buff, byteOrder, uint64(inputLenght))
	if err != nil {
		return nil, err
	}
	err = buff.WriteByte(methodIdentifier)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func extractMetadata(input []byte) ([]byte, int, error) {
	if len(input) < metadataSizeInByte {
		return nil, 0, ErrMissingMetadata
	}
	output := input[:len(input)-metadataSizeInByte]
	dstSize := byteOrder.Uint64(input[len(input)-metadataSizeInByte : len(input)-methodIDLengthInByte])
	return output, int(dstSize), nil
}
