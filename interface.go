package cachier

import (
	"errors"
	"sync"
)

// Errors
var (
	ErrNotFound = errors.New("Key not found")
)

// CacheEngine is an interface for cache engine (e.g. in-memory cache or Redis Cache)
type CacheEngine interface {
	Get(key string) (interface{}, error)
	Peek(key string) (interface{}, error)
	Set(key string, value interface{}) error
	Delete(key string) error
	Keys() ([]string, error)
	Purge() error
}

// Cache is an implementation of a cache (key-value store).
// It needs to be provided with cache engine.
type Cache struct {
	CacheEngine
	computeLocks sync.Map
}

// MakeCache creates cache with provided engine
func MakeCache(engine CacheEngine) *Cache {
	return &Cache{CacheEngine: engine}
}

// GetOrCompute tries to get value from cache. If not found, it computes the
// value using provided evaluator function and stores it into cache.
func (c *Cache) GetOrCompute(key string, evaluator func() (interface{}, error)) (interface{}, error) {
	value, _ := c.computeLocks.LoadOrStore(key, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	defer func() {
		c.computeLocks.Delete(key)
		mutex.Unlock()
	}()

	value, err := c.Get(key)
	if err == nil {
		return value, nil
	}
	if err == ErrNotFound {
		value, err := evaluator()
		if err != nil {
			return nil, err
		}
		err = c.Set(key, value)
		return value, err
	}
	return nil, err
}
