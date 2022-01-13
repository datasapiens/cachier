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

// Predicate evaluates a condition on the input string
type Predicate func(string) bool

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

type lock struct {
	key   string
	mutex *sync.Mutex
}

// MakeCache creates cache with provided engine
func MakeCache(engine CacheEngine) *Cache {
	return &Cache{CacheEngine: engine}
}

func (c *Cache) lockKey(key string) lock {
	value, _ := c.computeLocks.LoadOrStore(key, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()

	return lock{
		key:   key,
		mutex: mutex,
	}
}

func (c *Cache) unlock(l lock) {
	c.computeLocks.Delete(l.key)
	l.mutex.Unlock()
}

// GetOrCompute tries to get value from cache.
// If not found, it computes the value using provided evaluator function and stores it into cache.
// In case of other errors the value is evaluated but not stored in the cache.
func (c *Cache) GetOrCompute(key string, evaluator func() (interface{}, error)) (interface{}, error) {
	lock := c.lockKey(key)

	value, err := c.Get(key)
	if err == nil {
		c.unlock(lock)
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
				c.unlock(lock)
			}()
			return value, nil
		}
	} else {
		// evalutation error
		value = nil
		err = evaluatorErr
	}

	c.unlock(lock)
	return value, err
}

// GetIndirect gets a key value following any intermediary links
func (c *Cache) GetIndirect(key string, linkResolver func(interface{}) string) (interface{}, error) {
	value, err := c.Get(key)
	if err != nil {
		return nil, err
	}

	if linkResolver != nil {
		if link := linkResolver(value); len(link) > 0 && link != key {
			return c.GetIndirect(link, linkResolver)
		}
	}

	return value, nil
}

// SetIndirect sets cache key including intermediary links
func (c *Cache) SetIndirect(key string, value interface{}, linkResolver func(interface{}) string, linkGenerator func(interface{}) interface{}) error {
	if linkGenerator != nil && linkResolver != nil {
		if linkValue := linkGenerator(value); linkValue != nil {
			link := linkResolver(linkValue)

			if err := c.Set(key, linkValue); err != nil {
				return err
			}

			return c.Set(link, value)
		}
	}

	return c.Set(key, value)
}

// GetOrComputeEx tries to get value from cache.
// If not found, it computes the value using provided evaluator function and stores it into cache.
// In case of other errors the value is evaluated but not stored in the cache.
// Additional parameters (can be nil):
// validator - validates cache records, on false evaluator will be run again (new results will not be validated)
// linkResolver - checks if cached value is a link and returns the key it's pointing to
// linkGenerator - generates intermediate link value if needed when a new record is inserted
// writeApprover - decides if new value is to be written in the cache
func (c *Cache) GetOrComputeEx(key string, evaluator func() (interface{}, error), validator func(interface{}) bool, linkResolver func(interface{}) string, linkGenerator func(interface{}) interface{}, writeApprover func(interface{}) bool) (interface{}, error) {
	lock := c.lockKey(key)

	value, err := c.GetIndirect(key, linkResolver)
	if err == nil && (validator == nil || validator(value)) {
		c.unlock(lock)
		return value, nil
	}

	value, evaluatorErr := evaluator()

	if evaluatorErr == nil {
		// value evaluted correctly
		if err == ErrNotFound {
			if writeApprover == nil || writeApprover(value) {
				// Key not found in cache
				c.SetIndirect(key, value, linkResolver, linkGenerator)
			}
			c.unlock(lock)

			return value, nil
		}
	} else {
		// evalutation error
		value = nil
		err = evaluatorErr
	}

	c.unlock(lock)
	return value, err
}

//DeletePredicate deletes all keys matching the supplied predicate, returns number of deleted keys
func (c *Cache) DeletePredicate(pred Predicate) (int, error) {
	count := 0

	keys, err := c.Keys()
	if err != nil {
		return count, err
	}

	for _, key := range keys {
		if pred(key) {
			if err := c.Delete(key); err != nil {
				return count, err
			}
			count++
		}
	}

	return count, nil
}

// DeleteWithPrefix removes all keys that start with given prefix, returns number of deleted keys
func (c *Cache) DeleteWithPrefix(prefix string) (int, error) {
	pred := func(s string) bool {
		return strings.HasPrefix(s, prefix)
	}

	return c.DeletePredicate(pred)
}

// DeleteRegExp deletes all keys matching the supplied regexp, returns number of deleted keys
func (c *Cache) DeleteRegExp(pattern string) (int, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, err
	}

	return c.DeletePredicate(re.MatchString)
}

// CountPredicate counts cache keys satisfying the given predicate
func (c *Cache) CountPredicate(pred Predicate) (int, error) {
	keys, err := c.Keys()
	if err != nil {
		return 0, err
	}

	count := 0

	for _, key := range keys {
		if pred(key) {
			count++
		}
	}

	return count, nil
}

// CountRegExp counts all keys matching the supplied regexp
func (c *Cache) CountRegExp(pattern string) (int, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, err
	}

	return c.CountPredicate(re.MatchString)
}
