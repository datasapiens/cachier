package cachier

import (
	"math/rand"
	"strings"
	"testing"

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
