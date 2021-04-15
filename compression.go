package cachier

import (
	"github.com/DataDog/zstd"
	"github.com/pierrec/lz4/v4"
)

const lz4UnknownFrameDescriptor = "Unknown frame descriptor"
const lz4IncorrectSize = "Src size is incorrect"

// NoCompressionService uses no compression
var NoCompressionService *noCompression = &noCompression{}

// ZstdCompressionService uses  lz4 method
var ZstdCompressionService *zstdCompression = &zstdCompression{}

// Lz4CompressionService uses  lz4 method
var Lz4CompressionService *lz4Compression = &lz4Compression{}

type noCompression struct {
}

// Compress returns src without any changes.
func (c noCompression) Compress(src []byte) ([]byte, error) {
	return src, nil
}

// Decompress returns src without any changes.
func (c noCompression) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

type zstdCompression struct {
}

// Compress compresses src  using zstd method
func (zs zstdCompression) Compress(src []byte) ([]byte, error) {
	output, err := zstd.Compress(nil, src)
	if err != nil {
		return nil, err
	}

	if len(output) >= len(src) {
		return src, nil
	}

	return output, nil
}

// Decompress decompresses src  using zstd method
func (zs zstdCompression) Decompress(src []byte) ([]byte, error) {
	output, err := zstd.Decompress(nil, src)
	if err != nil {
		if err.Error() == lz4UnknownFrameDescriptor || err.Error() == lz4IncorrectSize {
			return src, nil
		}

		return nil, err
	}
	return output, nil
}

type lz4Compression struct {
}

// Compress compresses src  using lz4 method
func (lz lz4Compression) Compress(src []byte) ([]byte, error) {
	buf := make([]byte, 2*len(src))
	var compressor lz4.Compressor
	n, err := compressor.CompressBlock(src, buf)
	if err != nil {
		return nil, err
	}
	if n == 0 || n >= len(src) {
		return src, nil
	}
	return buf[:n], nil
}

// Decompress decompresses src  using lz4 method
func (lz lz4Compression) Decompress(src []byte) ([]byte, error) {
	buf := make([]byte, 10*len(src))
	n, err := lz4.UncompressBlock(src, buf)
	if err != nil {
		// Try to use bigger buffer
		buf = make([]byte, 100*len(src))
		n, err = lz4.UncompressBlock(src, buf)
		if err != nil {
			return src, nil
		}
	}

	return buf[:n], nil
}
