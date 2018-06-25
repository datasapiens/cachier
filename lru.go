package cachier

import (
	"github.com/hashicorp/golang-lru"
)

// LRUCache is a wrapper of hashicorp's golang-lru cache which
// implements cachier.Cache interface
type LRUCache struct {
	lru *lru.Cache
}

// NewLRUCache is a constructor that creates LRU cache of given size
func NewLRUCache(size int) (*LRUCache, error) {
	lruHashicorp, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	return &LRUCache{
		lru: lruHashicorp,
	}, nil
}

// Get gets a value by given key
func (lc *LRUCache) Get(key string) (interface{}, error) {
	value, found := lc.lru.Get(key)
	if !found {
		return nil, ErrNotFound
	}
	return value, nil
}

// Peek gets a value by given key and does not change it's "lruness"
func (lc *LRUCache) Peek(key string) (interface{}, error) {
	value, found := lc.lru.Peek(key)
	if !found {
		return nil, ErrNotFound
	}
	return value, nil
}

// Set stores given key-value pair into cache
func (lc *LRUCache) Set(key string, value interface{}) error {
	lc.lru.Add(key, value)
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
