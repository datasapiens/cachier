package cachier

import (
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
	compressedOutput, err := cLz4CompressionService.Compress(input)
	require.Nil(t, err)
	assert.True(t, len(compressedOutput) <= len(input))
	decompressedOutput, err := cLz4CompressionService.Decompress(compressedOutput)
	require.Nil(t, err)
	assert.Equal(t, input, decompressedOutput)

}

func TestLz4CompressionFile(t *testing.T) {
	t.SkipNow()
	files := []string{
		"cache_samples/cache_wtf",
		"cache_samples/cache_products",
	}

	for _, path := range files {
		buf, err := ioutil.ReadFile(path)
		require.Nil(t, err)
		start := time.Now()
		compressedOutput, err := Lz4CompressionService.Compress(buf)
		elapsed := time.Since(start)
		require.Nil(t, err)
		t.Logf(" Orginal size %v\n", len(buf))
		t.Logf("Compression took %s\n", elapsed)
		t.Logf("Size %v", len(compressedOutput))
		start = time.Now()
		decompressedOutput, err := Lz4CompressionService.Decompress(compressedOutput)
		elapsed = time.Since(start)
		require.Nil(t, err)
		t.Logf("DeCompression took %s\n", elapsed)
		t.Logf("Size %v", len(decompressedOutput))
	}

}

func TestCLz4CompressionFile(t *testing.T) {
	t.SkipNow()
	files := []string{
		"cache_samples/cache_wtf",
		"cache_products",
	}

	for _, path := range files {
		buf, err := ioutil.ReadFile(path)
		require.Nil(t, err)
		start := time.Now()
		compressedOutput, err := cLz4CompressionService.Compress(buf)
		elapsed := time.Since(start)
		require.Nil(t, err)
		t.Logf(" Orginal size %v\n", len(buf))
		t.Logf("Compression took %s\n", elapsed)
		t.Logf("Size %v", len(compressedOutput))
		start = time.Now()
		decompressedOutput, err := cLz4CompressionService.Decompress(compressedOutput)
		elapsed = time.Since(start)
		require.Nil(t, err)
		t.Logf("DeCompression took %s\n", elapsed)
		t.Logf("Size %v", len(decompressedOutput))
	}

}
