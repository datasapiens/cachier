package compression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

const providerIDLengthInByte = 1
const originalSizeLengthInByte = 8
const footerSizeInByte = providerIDLengthInByte + originalSizeLengthInByte
const defaultNotCompressedBufferSize = 1024

var byteOrder = binary.LittleEndian

// Names of compression parameters
const (
	CompressionParamLevel       = "level"
	CompressionParamMinInputLen = "minInputLen"
)

// Errors
var (
	ErrMissingFooter            = fmt.Errorf("corrupted input data; cannot extract footer")
	ErrProviderNotFound         = fmt.Errorf("cannot find compression provider by ID")
	ErrCompressionParamNotFound = fmt.Errorf("cannot find compression parameter by name")
	ErrCompressionParamNotInt   = fmt.Errorf("compression parameter is not an integer type")
	ErrCompressionParamNil      = fmt.Errorf("compression parameter map cannot be nil")
)

// Provider defines compression method
type Provider interface {
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte, dstSize int) ([]byte, error)
	GetID() byte
	Configure(params CompressionParams) error
}

// CompressionParams defines compression parameters used by providers
type CompressionParams map[string]interface{}

// GetInt gets value from map and tries to parse it as integer
func (c CompressionParams) GetInt(key string) (int, error) {
	value, found := c[key]
	if !found {
		return 0, ErrCompressionParamNotFound
	}
	i, ok := value.(int)

	if !ok {
		return i, ErrCompressionParamNotInt
	}

	return i, nil
}

// GetIntWithDefault gets value from map and tries to parse it as integer.
// If key is not found in the map the dafule value is returned
func (c CompressionParams) GetIntWithDefault(key string, defaultValue int) (int, error) {
	value, err := c.GetInt(key)
	if err == ErrCompressionParamNotFound {
		return defaultValue, nil
	}
	return value, err
}

// Engine defines compression engine
type Engine struct {
	noCompressionID      byte
	defaultCompressionID byte
	providers            map[byte]Provider
	minInputSize         int
	mutex                sync.RWMutex
}

// NewEngine creates copression engine with given default provider ID
// If providerID == 0 it means no compression so it is returned `nil,nil`;
// defult not compressed buffer size - 1024 bytes
// Supported providers: github.com/DataDog/zstd, github.com/cloudflare/golz4, github.com/klauspost/compress/s2,
func NewEngine(defaultProviderID byte, params CompressionParams) (*Engine, error) {
	if defaultProviderID == 0 {
		// it means no compression, so no error is returned
		return nil, nil
	}

	providers := getBuildInProviders()

	defaultProvider, ok := providers[defaultProviderID]
	if !ok {
		return nil, ErrProviderNotFound
	}

	if params != nil && len(params) > 0 {
		for _, v := range providers {
			if err := v.Configure(params); err != nil {
				return nil, err
			}
		}
	}

	minInputSize, err := params.GetIntWithDefault(CompressionParamMinInputLen, defaultNotCompressedBufferSize)
	if err != nil {
		return nil, err
	}

	return &Engine{
		noCompressionID:      0,
		defaultCompressionID: defaultProvider.GetID(),
		providers:            providers,
		minInputSize:         minInputSize,
	}, nil
}

// Compress compresses input buffer using default compression provider
// If input buffer size < minInputSize the input is not compressed
func (ce *Engine) Compress(input []byte) ([]byte, error) {
	var provider Provider
	ce.mutex.RLock()

	if len(input) <= ce.minInputSize {
		provider = ce.providers[ce.noCompressionID]
	} else {
		provider = ce.providers[ce.defaultCompressionID]
	}
	ce.mutex.RUnlock()

	output, err := provider.Compress(input)
	if err != nil {
		return nil, err
	}

	return ce.addFooter(output, provider.GetID(), len(input))
}

// CompressWithProviderinput compresses input buffer using given compression provider
// The compression provider must be on the list of supported providers
// If input buffer size < minInputSize the input is not compressed
func (ce *Engine) CompressWithProvider(input []byte, providerID byte) ([]byte, error) {

	var provider Provider
	ce.mutex.RLock()

	if len(input) <= ce.minInputSize {
		provider = ce.providers[ce.noCompressionID]
	} else {
		ok := true
		provider, ok = ce.providers[providerID]
		if !ok {
			ce.mutex.RUnlock()
			return nil, ErrProviderNotFound
		}
	}
	ce.mutex.RUnlock()
	output, err := provider.Compress(input)
	if err != nil {
		return nil, err
	}
	return ce.addFooter(output, provider.GetID(), len(input))
}

// Decompress extracts from input the information about used compression method.
// If compression provider is found - the data are decompressed
func (ce *Engine) Decompress(input []byte) ([]byte, error) {
	src, providerID, dstSize, err := ce.extractFooter(input)
	if err != nil {
		return nil, err
	}
	ce.mutex.RLock()
	provider, ok := ce.providers[providerID]
	if !ok {
		ce.mutex.RUnlock()
		return nil, ErrProviderNotFound
	}
	ce.mutex.RUnlock()

	return provider.Decompress(src, dstSize)
}

// AddProvider adds compression provider to the list of supported providers
func (ce *Engine) AddProvider(provider Provider) *Engine {
	ce.mutex.Lock()
	defer ce.mutex.Unlock()

	if ce.providers == nil {
		ce.providers = make(map[byte]Provider)
	}
	ce.providers[provider.GetID()] = provider
	return ce
}

// AddDefaultProvider adds default compression provider
func (ce *Engine) AddDefaultProvider(provider Provider) *Engine {
	ce.mutex.Lock()
	defer ce.mutex.Unlock()

	if ce.providers == nil {
		ce.providers = make(map[byte]Provider)
	}
	ce.providers[provider.GetID()] = provider
	ce.defaultCompressionID = provider.GetID()
	return ce
}

// SetMinInputSize allows to set min input buffer size.
// Buffers smaller than this value are not compressed
func (ce *Engine) SetMinInputSize(minInputSize int) *Engine {
	ce.minInputSize = minInputSize
	return ce
}

// SetDefaultProvider allows to set the defult provider by ID
// The provider must be on the list of supported providers
func (ce *Engine) SetDefaultProvider(id byte) error {
	ce.mutex.Lock()
	defer ce.mutex.Unlock()
	if ce.providers == nil {
		return ErrProviderNotFound
	}
	_, ok := ce.providers[id]
	if !ok {
		return ErrProviderNotFound
	}

	ce.defaultCompressionID = id

	return nil
}

// addFooter addes footer to compressed data
func (ce *Engine) addFooter(compressedInput []byte, providerID byte, inputLenght int) ([]byte, error) {
	if providerID == ce.noCompressionID {
		buff := bytes.NewBuffer(make([]byte, 0, providerIDLengthInByte))
		err := buff.WriteByte(providerID)
		if err != nil {
			return nil, err
		}
		return append(compressedInput, buff.Bytes()...), nil
	}

	buff := bytes.NewBuffer(make([]byte, 0, footerSizeInByte))
	err := binary.Write(buff, byteOrder, uint64(inputLenght))
	if err != nil {
		return nil, err
	}
	err = buff.WriteByte(providerID)
	if err != nil {
		return nil, err
	}

	return append(compressedInput, buff.Bytes()...), nil
}

// extractFooter extracts footer from comressed data and returs:
// - input without footer,
// - used compression provider ID,
// - original size of compressed data
// - error if data are corrupted
func (ce *Engine) extractFooter(input []byte) ([]byte, byte, int, error) {
	providerID := input[len(input)-providerIDLengthInByte]
	if providerID == ce.noCompressionID {
		inputLen := len(input)
		if len(input) < providerIDLengthInByte {
			return nil, 0, 0, ErrMissingFooter
		}
		return input[:inputLen-providerIDLengthInByte], providerID, inputLen - 1, nil
	}

	if len(input) < footerSizeInByte {
		return nil, 0, 0, ErrMissingFooter
	}

	output := input[:len(input)-footerSizeInByte]
	dstSize := byteOrder.Uint64(input[len(input)-footerSizeInByte : len(input)-providerIDLengthInByte])

	return output, providerID, int(dstSize), nil
}
