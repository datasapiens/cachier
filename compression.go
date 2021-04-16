package cachier

import (
	"github.com/DataDog/zstd"
	clz4 "github.com/cloudflare/golz4"
	"github.com/pierrec/lz4/v4"
)

const lz4UnknownFrameDescriptor = "Unknown frame descriptor"
const lz4IncorrectSize = "Src size is incorrect"
const minInputSizeForCompressionInBytes = 1024

// NoCompressionService uses no compression
var NoCompressionService *noCompression = &noCompression{}

// ZstdCompressionServic	github.com/bkaradzic/go-lz4 v1.0.0e uses  lz4 method
var ZstdCompressionService *zstdCompression = &zstdCompression{
	minInputSize: minInputSizeForCompressionInBytes,
}

// Lz4CompressionService uses  lz4 method
var Lz4CompressionService *lz4Compression = &lz4Compression{
	minInputSize: minInputSizeForCompressionInBytes,
}

// DO NOT USE IT.
// CLz4CompressionService uses  lz4 method ported from C
// compression is 3 times slower than in pure golang implementation.
// Trying to decompress regular data (not compressed one) causes inifinity loop
var cLz4CompressionService *cLz4Compression = &cLz4Compression{
	minInputSize: minInputSizeForCompressionInBytes,
}

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
	minInputSize int
}

// Compress compresses src  using zstd method
func (zs zstdCompression) Compress(src []byte) ([]byte, error) {

	if len(src) < zs.minInputSize {
		return src, nil
	}
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
	minInputSize int
}

// Compress compresses src  using lz4 method
func (lz lz4Compression) Compress(src []byte) ([]byte, error) {
	if len(src) < lz.minInputSize {
		return src, nil
	}

	buf := make([]byte, len(src))
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
	buf := make([]byte, 20*len(src))
	n, err := lz4.UncompressBlock(src, buf)
	if err != nil {
		// Try to use maxium required buffer size
		buf = make([]byte, 255*len(src))
		n, err = lz4.UncompressBlock(src, buf)
		if err != nil {
			return src, nil
		}
	}

	return buf[:n], nil
}

type cLz4Compression struct {
	minInputSize int
}

// Compress compresses src  using lz4 method poreted from C
func (clz cLz4Compression) Compress(src []byte) ([]byte, error) {
	if len(src) < clz.minInputSize {
		return src, nil
	}

	output := make([]byte, clz4.CompressBound(src))
	outSize, err := clz4.Compress(src, output)
	if err != nil {
		return nil, err
	}
	if outSize >= len(src) {
		return src, nil
	}
	return output, nil
}

// Decompress decompresses src  using lz4 method
func (clz cLz4Compression) Decompress(src []byte) ([]byte, error) {
	output := make([]byte, 20*len(src))
	err := clz4.Uncompress(src, output)
	if err != nil {
		return src, nil
	}

	return output, nil
}

// This commented out block contains other way to compress and decompress using
// the github.com/pierrec/lz4 implementation. Hovewer the reasults are worse than using blocks
// Compress compresses src  using lz4 method
// func (lz lz4Compression) Compress(src []byte) ([]byte, error) {
// 	var out bytes.Buffer
// 	r := bytes.NewReader(src)
// 	zw := lz4.NewWriter(&out)
// 	_, err := io.Copy(zw, r)
// 	if err != nil {
// 		fmt.Println(err)
// 		return nil, err
// 	}
// 	err = zw.Close()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return out.Bytes(), nil
// }

// // Decompress decompresses src  using lz4 method
// func (lz lz4Compression) Decompress(src []byte) ([]byte, error) {
// 	var out bytes.Buffer
// 	r := bytes.NewReader(src)
// 	zr := lz4.NewReader(r)
// 	_, err := io.Copy(&out, zr)
// 	if err != nil {
// 		fmt.Println(err)
// 		return nil, err
// 	}

// 	return out.Bytes(), nil
// }
