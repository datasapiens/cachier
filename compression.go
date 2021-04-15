package cachier

import (
	"github.com/DataDog/zstd"
	"github.com/pierrec/lz4/v4"
)

const Lz4UnknownFrameDescriptor = "Unknown frame descriptor"
const Lz4IncorrectSize = "Src size is incorrect"

var NoCompressionService *noCompression = &noCompression{}
var ZstdCompressionService *zstdCompression = &zstdCompression{}
var Lz4CompressionService *lz4Compression = &lz4Compression{}

type noCompression struct {
}

func (c noCompression) Compress(src []byte) ([]byte, error) {
	return src, nil
}
func (c noCompression) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

type zstdCompression struct {
}

func (c zstdCompression) Compress(src []byte) ([]byte, error) {
	output, err := zstd.Compress(nil, src)
	if err != nil {
		return nil, err
	}

	if len(output) >= len(src) {
		return src, nil
	}

	return output, nil
}
func (c zstdCompression) Decompress(src []byte) ([]byte, error) {
	output, err := zstd.Decompress(nil, src)
	if err != nil {
		if err.Error() == Lz4UnknownFrameDescriptor || err.Error() == Lz4IncorrectSize {
			return src, nil
		}

		return nil, err
	}
	return output, nil
}

type lz4Compression struct {
}

func (c lz4Compression) Compress(src []byte) ([]byte, error) {
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
func (c lz4Compression) Decompress(src []byte) ([]byte, error) {
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
