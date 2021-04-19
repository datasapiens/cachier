package cachier

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const textBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randTextBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = textBytes[rand.Intn(len(textBytes))]
	}
	return b
}

func TestNoCompression(t *testing.T) {
	input := randTextBytes(1024)
	output, err := NoCompressionService.Compress(input)
	require.Nil(t, err)
	assert.Equal(t, input, output)
}

func TestZstdCompression(t *testing.T) {
	input := randTextBytes(4096)
	compressedOutput, err := ZstdCompressionService.Compress(input)
	require.Nil(t, err)
	assert.True(t, len(compressedOutput) <= len(input))
	decompressedOutput, err := ZstdCompressionService.Decompress(compressedOutput)
	require.Nil(t, err)
	assert.Equal(t, input, decompressedOutput)
}

func TestZstdCompressionToShortString(t *testing.T) {
	s := "hello world"
	input := []byte(strings.Repeat(s, 1))
	compressedOutput, err := ZstdCompressionService.Compress(input)
	require.Nil(t, err)
	assert.True(t, len(compressedOutput) <= len(input))
	decompressedOutput, err := ZstdCompressionService.Decompress(compressedOutput)
	require.Nil(t, err)
	assert.Equal(t, input, decompressedOutput)
}

func TestZstdCompressionNotCompressedString(t *testing.T) {
	s := "hello world"
	input := []byte(strings.Repeat(s, 2))
	compressedOutput, err := ZstdCompressionService.Compress(input)
	require.Nil(t, err)
	assert.True(t, len(compressedOutput) <= len(input))
	decompressedOutput, err := ZstdCompressionService.Decompress(compressedOutput)
	require.Nil(t, err)
	assert.Equal(t, input, decompressedOutput)
}

func TestLz4CompressionRandomBytes(t *testing.T) {
	input := randTextBytes(4096)
	compressedOutput, err := Lz4CompressionService.Compress(input)
	require.Nil(t, err)
	assert.True(t, len(compressedOutput) <= len(input))
	decompressedOutput, err := Lz4CompressionService.Decompress(compressedOutput)
	require.Nil(t, err)
	assert.Equal(t, input, decompressedOutput)
}

func TestLz4CompressionShortString(t *testing.T) {
	s := "hello world"
	input := []byte(s)
	compressedOutput, err := Lz4CompressionService.Compress(input)
	require.Nil(t, err)
	assert.True(t, len(compressedOutput) <= len(input))
	decompressedOutput, err := Lz4CompressionService.Decompress(compressedOutput)
	require.Nil(t, err)
	assert.Equal(t, input, decompressedOutput)
}

func TestLz4CompressionLongString(t *testing.T) {
	s := "hello world"
	input := []byte(strings.Repeat(s, 50))
	compressedOutput, err := Lz4CompressionService.Compress(input)
	require.Nil(t, err)
	assert.True(t, len(compressedOutput) <= len(input))
	decompressedOutput, err := Lz4CompressionService.Decompress(compressedOutput)
	require.Nil(t, err)
	assert.Equal(t, input, decompressedOutput)

}

func TestCLz4CompressionLongString(t *testing.T) {
	s := "hello world"
	input := []byte(strings.Repeat(s, 50))
	compressedOutput, err := CLz4CompressionService.Compress(input)
	require.Nil(t, err)
	assert.True(t, len(compressedOutput) <= len(input))
	decompressedOutput, err := CLz4CompressionService.Decompress(compressedOutput)
	require.Nil(t, err)
	assert.Equal(t, input, decompressedOutput)

}

func TestBigBufferCompression(t *testing.T) {
	repetitionPerFile := 15

	testFiles := []string{
		"/home/marcin/Downloads/cache/cache_samples/cache_wtf",
		"/home/marcin/Downloads/cache/cache_samples/cache_products",
	}

	var testData = []struct {
		testName string
		provider CompressionProvider
	}{
		{
			testName: "DataDog/zstd",
			provider: ZstdCompressionService,
		},
		{
			testName: "pierrec/lz4",
			provider: Lz4CompressionService,
		},
		{
			testName: "cloudflare/golz4",
			provider: CLz4CompressionService,
		},
		{
			testName: "klauspost/compress/s2",
			provider: S2CompressionService,
		},
		{
			testName: "klauspost/compress/zstd",
			provider: KZstdCompressionService,
		},
		{
			testName: "klauspost/pgzip",
			provider: PgzipCompressionService,
		},
	}

	for _, tt := range testData {
		t.Run(tt.testName, func(t *testing.T) {
			t.Logf("---- %s ----", tt.testName)
			for _, path := range testFiles {
				buf, err := ioutil.ReadFile(path)
				require.Nil(t, err)
				var compressionSum int64
				var decompressionSum int64
				t.Logf("File path %s", path)
				t.Logf("Orginal size %s\n", ByteCount(len(buf)))
				for a := 1; a <= repetitionPerFile; a++ {
					start := time.Now()
					compressedOutput, err := tt.provider.Compress(buf)
					elapsed := time.Since(start)
					require.Nil(t, err)

					// t.Logf("Compression took %s\n", elapsed)
					// t.Logf("Compressed size %s", ByteCount(len(compressedOutput)))
					compressionSum += elapsed.Milliseconds()
					start = time.Now()
					decompressedOutput, err := tt.provider.Decompress(compressedOutput)
					elapsed = time.Since(start)
					require.Nil(t, err)
					// t.Logf("Decompression took %s\n", elapsed)
					// t.Logf("Decmopressed size %s", ByteCount(len(decompressedOutput)))
					decompressionSum += elapsed.Milliseconds()
					if a == repetitionPerFile {
						avgC := (float64(compressionSum)) / (float64(repetitionPerFile))
						avgDC := (float64(decompressionSum)) / (float64(repetitionPerFile))
						t.Logf("Avearge compression time %.2f ms\n", avgC)
						t.Logf("Compressed size %s", ByteCount(len(compressedOutput)))
						t.Logf("Avearge decompression time %.2f ms\n", avgDC)
						t.Logf("Decmopressed size %s", ByteCount(len(decompressedOutput)))

					}
				}
			}
		})
	}
}

func ByteCount(b int) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
