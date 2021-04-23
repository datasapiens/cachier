package cachier

import (
	"fmt"

	"github.com/datasapiens/cachier/compression"
	lru "github.com/hashicorp/golang-lru"
)

// LRUCache is a wrapper of hashicorp's golang-lru cache which
// implements cachier.Cache interface
type LRUCache struct {
	lru               *lru.Cache
	marshal           func(value interface{}) ([]byte, error)
	unmarshal         func(b []byte, value *interface{}) error
	compressionEngine *compression.Engine
}

// NewLRUCache is a constructor that creates LRU cache of given size
// If you want to compress the entries before storing them the marshal and unmarshal functions must be provided
// If the compression.Engine is nil the marshal and unmarshal functions are not used
func NewLRUCache(
	size int,
	marshal func(value interface{}) ([]byte, error),
	unmarshal func(b []byte, value *interface{}) error,
	compressionEngine *compression.Engine,
) (*LRUCache, error) {
	lruHashicorp, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	return &LRUCache{
		lru:               lruHashicorp,
		marshal:           marshal,
		unmarshal:         unmarshal,
		compressionEngine: compressionEngine,
	}, nil
}

// Get gets a value by given key
func (lc *LRUCache) Get(key string) (interface{}, error) {
	value, found := lc.lru.Get(key)
	if !found {
		return nil, ErrNotFound
	}

	if lc.compressionEngine == nil {
		return value, nil
	}

	return lc.decompress(key, value)
}

func (lc *LRUCache) decompress(key string, value interface{}) (interface{}, error) {
	byteValue, ok := value.([]byte)
	if !ok {
		lc.Delete(key)
		return nil, fmt.Errorf("data in cache are corrupted")
	}

	input, err := lc.compressionEngine.Decompress(byteValue)
	if err != nil {
		lc.Delete(key)
		return nil, err
	}

	var result interface{}
	lc.unmarshal(input, &result)
	return result, nil
}

// Peek gets a value by given key and does not change it's "lruness"
func (lc *LRUCache) Peek(key string) (interface{}, error) {
	value, found := lc.lru.Peek(key)
	if !found {
		return nil, ErrNotFound
	}
	if lc.compressionEngine == nil {
		return value, nil
	}

	return lc.decompress(key, value)
}

// Set stores given key-value pair into cache
func (lc *LRUCache) Set(key string, value interface{}) error {
	if lc.compressionEngine == nil {
		lc.lru.Add(key, value)
		return nil
	}

	marshalledValue, err := lc.marshal(value)
	if err != nil {
		return err
	}

	input, err := lc.compressionEngine.Compress(marshalledValue)
	if err != nil {
		return err
	}
	lc.lru.Add(key, input)
	return nil
}

// Delete removes a key from cache
func (lc *LRUCache) Delete(key string) error {
	lc.lru.Remove(key)
	return nil
}

// Keys returns all the keys in cache
func (lc *LRUCache) Keys() ([]string, error) {
	lruKeys := lc.lru.Keys()
	keys := make([]string, 0, len(lruKeys))

	for i := 0; i < len(lruKeys); i++ {
		keys = append(keys, lruKeys[i].(string))
	}
	return keys, nil
}

// Purge removes all records from the cache
func (lc *LRUCache) Purge() error {
	lc.lru.Purge()
	return nil
}
