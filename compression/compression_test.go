package compression

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

func TestNoCompressionRandomBytes(t *testing.T) {
	engine := NewEngine()
	input := randTextBytes(1024)
	output, err := engine.Compress(input)
	require.Nil(t, err)
	// output should have one extra byte
	assert.Equal(t, len(input)+1, len(output))
	decompressedOutput, err := engine.Decompress(output)
	require.Nil(t, err)
	assert.Equal(t, len(input), len(decompressedOutput))
	assert.Equal(t, input, decompressedOutput)
}

func TestNoCompressionIncreaseMinSize(t *testing.T) {
	engine := NewEngine()
	input := randTextBytes(2048)
	output, err := engine.Compress(input)
	require.Nil(t, err)
	// output should be smaller than input
	// compression is used
	assert.True(t, len(output) < len(input))
	decompressedOutput, err := engine.Decompress(output)
	require.Nil(t, err)
	assert.Equal(t, len(input), len(decompressedOutput))
	assert.Equal(t, input, decompressedOutput)

	engine.SetMinInputSize(2048)
	output, err = engine.Compress(input)
	require.Nil(t, err)
	// output should have one extra byte, no compression is used
	assert.Equal(t, len(input)+1, len(output))
	decompressedOutput, err = engine.Decompress(output)
	require.Nil(t, err)
	assert.Equal(t, len(input), len(decompressedOutput))
	assert.Equal(t, input, decompressedOutput)

}

func TestNoCompressionLongString(t *testing.T) {
	engine := NewEngine()
	s := "hello world"
	input := []byte(strings.Repeat(s, 50))
	output, err := engine.Compress(input)
	require.Nil(t, err)
	// output should have one extra byte
	assert.Equal(t, len(input)+1, len(output))
	decompressedOutput, err := engine.Decompress(output)
	require.Nil(t, err)
	assert.Equal(t, len(input), len(decompressedOutput))
	assert.Equal(t, input, decompressedOutput)
}

func TestDefaultCompressionLongString(t *testing.T) {
	engine := NewEngine()
	s := "hello world"
	input := []byte(strings.Repeat(s, 400))
	output, err := engine.Compress(input)
	require.Nil(t, err)
	// output should be smaller than input
	assert.True(t, len(output) < len(input))
	decompressedOutput, err := engine.Decompress(output)
	require.Nil(t, err)
	assert.Equal(t, len(input), len(decompressedOutput))
	assert.Equal(t, input, decompressedOutput)
}

func TestAddDefaultCompressionLongString(t *testing.T) {
	engine := NewEngine().AddDefaultProvider(S2CompressionService)
	s := "hello world"
	input := []byte(strings.Repeat(s, 400))
	output, err := engine.Compress(input)
	require.Nil(t, err)
	// output should be smaller than input
	assert.True(t, len(output) < len(input))
	decompressedOutput, err := engine.Decompress(output)
	require.Nil(t, err)
	assert.Equal(t, len(input), len(decompressedOutput))
	assert.Equal(t, input, decompressedOutput)
}

func TestCompressionWithProvider(t *testing.T) {
	engine := NewEngineAll()
	s := "hello world"
	input := []byte(strings.Repeat(s, 400))
	output, err := engine.Compress(input)
	require.Nil(t, err)
	// output should be smaller than input
	assert.True(t, len(output) < len(input))
	//compress the same data with other provider
	output2, err := engine.CompressWithProvider(input, Lz4CompressionService.GetID())
	require.Nil(t, err)
	// two compression method should results diffrent size of compressed input
	assert.True(t, len(output) != len(output2))

	decompressedOutput, err := engine.Decompress(output2)
	require.Nil(t, err)
	assert.Equal(t, len(input), len(decompressedOutput))
	assert.Equal(t, input, decompressedOutput)
}

func TestBigBufferCompression(t *testing.T) {
	t.SkipNow()
	repetitionPerFile := 10
	testFiles := []string{
		"/home/marcin/Downloads/cache/cache_samples/cache_wtf",
		"/home/marcin/Downloads/cache/cache_samples/cache_products",
	}

	var testData = []struct {
		testName string
		provider Provider
	}{
		{
			testName: "DataDog/zstd",
			provider: ZstdCompressionService,
		},
		{
			testName: "klauspost/compress/s2",
			provider: S2CompressionService,
		},
		{
			testName: "cloudflare/golz4",
			provider: Lz4CompressionService,
		},
	}
	engine := NewEngine()
	for _, tt := range testData {
		t.Run(tt.testName, func(t *testing.T) {
			t.Logf("---- %s ----", tt.testName)
			engine.AddDefaultProvider(tt.provider)
			for _, path := range testFiles {
				buf, err := ioutil.ReadFile(path)
				require.Nil(t, err)
				var compressionSum int64
				var decompressionSum int64
				t.Logf("File path %s", path)
				t.Logf("Orginal size %s\n", ByteCount(len(buf)))
				for a := 1; a <= repetitionPerFile; a++ {
					start := time.Now()
					compressedOutput, err := engine.Compress(buf)
					elapsed := time.Since(start)
					require.Nil(t, err)
					compressionSum += elapsed.Milliseconds()
					start = time.Now()
					decompressedOutput, err := engine.Decompress(compressedOutput)
					elapsed = time.Since(start)
					require.Nil(t, err)
					assert.Equal(t, len(buf), len(decompressedOutput))
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
