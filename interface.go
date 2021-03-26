// # Cachier

// Cachier is a Go library that provides an interface for dealing with cache.
// There is a CacheEngine interface which requires you to implement common cache
// methods (like Get, Set, Delete, etc). When implemented, you wrap this
// CacheEngine into the Cache struct. This struct has some methods implemented
// like GetOrCompute method (shortcut for fetching a hit or computing/writing
// a miss).

// There are also three implementations included:

//  - LRUCache: a wrapper of hashicorp/golang-lru which fulfills the CacheEngine
//    interface

//  - RedisCache: CacheEngine based on redis

//  - CacheWithSubcache: Implementation of combination of primary cache with
//    fast L1 subcache. E.g. primary Redis cache and fast (and small) LRU
//    subcache. But any other implementations of CacheEngine can be used.

package cachier

import (
	"errors"
	"regexp"
	"strings"
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

// GetOrCompute tries to get value from cache.
// If not found, it computes the value using provided evaluator function and stores it into cache.
// In case of other errors the value is evaluated but not stored in the cache.
func (c *Cache) GetOrCompute(key string, evaluator func() (interface{}, error)) (interface{}, error) {
	value, _ := c.computeLocks.LoadOrStore(key, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()

	value, err := c.Get(key)
	if err == nil {
		c.computeLocks.Delete(key)
		mutex.Unlock()
		return value, nil
	}

	value, evaluatorErr := evaluator()

	if evaluatorErr == nil {
		// value evaluted correctly
		if err == ErrNotFound {
			// Key not found on cache
			go func() {
				// Set key to cache in gorutine
				c.Set(key, value)
				c.computeLocks.Delete(key)
				mutex.Unlock()
			}()
			return value, nil
		}
	} else {
		// evalutation error
		value = nil
		err = evaluatorErr
	}

	c.computeLocks.Delete(key)
	mutex.Unlock()
	return value, err
}

// DeleteWithPrefix removes all keys that start with given prefix
func (c *Cache) DeleteWithPrefix(prefix string) error {
	keys, err := c.Keys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			if err := c.Delete(key); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteRegExp deletes all keys matching the supplied regexp
func (c *Cache) DeleteRegExp(pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	keys, err := c.Keys()
	if err != nil {
		return err
	}

	for _, key := range keys {
		if re.MatchString(key) {
			if err := c.Delete(key); err != nil {
				return err
			}
		}
	}
	return nil
}

// CountRecords counts all keys matching the supplied regexp
func (c *Cache) CountKeys(pattern string) (int, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, err
	}

	keys, err := c.Keys()
	if err != nil {
		return 0, err
	}

	count := 0

	for _, key := range keys {
		if re.MatchString(key) {
			count++
		}
	}
	return count, nil
}
