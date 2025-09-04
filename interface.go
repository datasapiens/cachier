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
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/datasapiens/cachier/utils"
)

// Errors
var (
	ErrNotFound      = errors.New("key not found")
	ErrWrongDataType = errors.New("data in wrong format")
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
type Cache[T any] struct {
	engine        CacheEngine
	computeLocks  utils.MutexMap
	writeQueue    *writeQueue[T]
	writeInterval time.Duration
	statsInterval time.Duration
	logger        Logger
}

// MakeCache creates cache with provided engine
func MakeCache[T any](engine CacheEngine, logger Logger) *Cache[T] {
	cache := &Cache[T]{
		engine:        engine,
		computeLocks:  *utils.NewMutexMap(),
		writeQueue:    newWriteQueue[T](),
		writeInterval: 1000 * time.Millisecond, // Default write interval
		statsInterval: 60 * time.Second,        // Default stats interval
		logger:        logger,
	}

	go cache.writeLoop() // Start the write loop in a goroutine

	return cache
}

// GetOrCompute tries to get value from cache.
// If not found, it computes the value using provided evaluator function and stores it into cache.
// In case of other errors the value is evaluated but not stored in the cache.
func (c *Cache[T]) GetOrCompute(key string, evaluator func() (*T, error)) (*T, error) {
	mutex := c.computeLocks.Lock(key)
	value, err := c.getNoLock(key)
	if err == nil {
		c.computeLocks.Unlock(key, mutex)
		return value, nil
	}

	calculatedValue, evaluatorErr := evaluator()

	if evaluatorErr == nil {
		// Key not found on cache
		go func() {
			c.setNoLock(key, calculatedValue)
			c.computeLocks.Unlock(key, mutex)
		}()
		return calculatedValue, nil
	} else {
		// evalutation error
		c.computeLocks.Unlock(key, mutex)
		calculatedValue = nil
		err = evaluatorErr
	}
	return calculatedValue, err
}

// Set stores a key-value pair into cache
func (c *Cache[T]) Set(key string, value *T) error {
	mutex := c.computeLocks.Lock(key)
	defer c.computeLocks.Unlock(key, mutex)
	return c.setNoLock(key, value)
}

// Get gets a cached value by key
func (c *Cache[T]) Get(key string) (*T, error) {
	mutex := c.computeLocks.RLock(key)
	defer c.computeLocks.RUnlock(key, mutex)
	return c.getNoLock(key)
}

// GetIndirect gets a key value following any intermediary links
func (c *Cache[T]) GetIndirect(key string, linkResolver func(*T) string) (*T, error) {
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
func (c *Cache[T]) SetIndirect(key string, value *T, linkResolver func(*T) string, linkGenerator func(*T) *T) error {
	mutex := c.computeLocks.Lock(key)
	defer c.computeLocks.Unlock(key, mutex)
	if linkGenerator != nil && linkResolver != nil {
		if linkValue := linkGenerator(value); linkValue != nil {
			link := linkResolver(linkValue)
			if link != key {
				lock := c.computeLocks.Lock(link)
				defer c.computeLocks.Unlock(link, lock)
				if err := c.setNoLock(key, linkValue); err != nil {
					return err
				}
			}

			return c.setNoLock(link, value)
		}
	}

	return c.setNoLock(key, value)
}

// GetOrComputeEx tries to get value from cache.
// If not found, it computes the value using provided evaluator function and stores it into cache.
// In case of other errors the value is evaluated but not stored in the cache.
// Additional parameters (can be nil):
// validator - validates cache records, on false evaluator will be run again (new results will not be validated)
// linkResolver - checks if cached value is a link and returns the key it's pointing to
// linkGenerator - generates intermediate link value if needed when a new record is inserted
// writeApprover - decides if new value is to be written in the cache
func (c *Cache[T]) GetOrComputeEx(key string, evaluator func() (*T, error), validator func(*T) bool, linkResolver func(*T) string, linkGenerator func(*T) *T, writeApprover func(*T) bool) (*T, error) {
	mutex := c.computeLocks.Lock(key)
	defer c.computeLocks.Unlock(key, mutex)
	value, err := c.getIndirectNoLock(key, linkResolver)
	if err == nil && (validator == nil || validator(value)) {
		return value, nil
	}

	value, evaluatorErr := evaluator()

	if evaluatorErr == nil {
		// value evaluted correctly
		if err == ErrNotFound {
			if writeApprover == nil || writeApprover(value) {
				// Key not found in cache
				c.setIndirectNoLock(key, value, linkResolver, linkGenerator)
			}

			return value, nil
		}
	} else {
		// evalutation error
		value = nil
		err = evaluatorErr
	}

	return value, err
}

// DeletePredicate deletes all keys matching the supplied predicate, returns number of deleted keys
func (c *Cache[T]) DeletePredicate(pred Predicate) error {
	c.writeQueue.DeletePredicate(pred)

	return nil
}

// DeleteWithPrefix removes all keys that start with given prefix, returns number of deleted keys
func (c *Cache[T]) DeleteWithPrefix(prefix string) error {
	pred := func(s string) bool {
		return strings.HasPrefix(s, prefix)
	}

	return c.DeletePredicate(pred)
}

// DeleteRegExp deletes all keys matching the supplied regexp, returns number of deleted keys
func (c *Cache[T]) DeleteRegExp(pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	return c.DeletePredicate(re.MatchString)
}

// CountPredicate counts cache keys satisfying the given predicate
func (c *Cache[T]) CountPredicate(pred Predicate) (int, error) {
	count := c.writeQueue.CountPredicate(pred)

	keys, err := c.Keys()
	if err != nil {
		return 0, err
	}

	for _, key := range keys {
		if pred(key) {
			count++
		}
	}

	return count, nil
}

// CountRegExp counts all keys matching the supplied regexp
func (c *Cache[T]) CountRegExp(pattern string) (int, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, err
	}

	return c.CountPredicate(re.MatchString)
}

// Peek gets a value by given key and does not change it's "lruness"
func (c *Cache[T]) Peek(key string) (*T, error) {
	mutex := c.computeLocks.RLock(key)
	defer c.computeLocks.RUnlock(key, mutex)

	if value, found := c.writeQueue.Get(key); found {
		if value != nil {
			return value, nil
		}
		// If the value is nil, it means the key is invalid (previously failed to write)
		return nil, ErrNotFound
	}

	untypedValue, err := c.engine.Peek(key)
	if err == nil {
		typedValue, ok := untypedValue.(T)
		if ok {
			return &typedValue, nil
		} else {
			err = ErrNotFound
		}
	}

	return nil, err
}

// Delete removes a key from cache
func (c *Cache[T]) Delete(key string) error {
	mutex := c.computeLocks.Lock(key)
	defer c.computeLocks.Unlock(key, mutex)
	c.writeQueue.Delete(key) // Remove from write queue
	return c.engine.Delete(key)
}

// Purge removes all records from the cache
func (c *Cache[T]) Purge() error {
	c.writeQueue.Purge() // Clear the write queue
	return c.engine.Purge()
}

// Keys returns all the keys in cache
func (c *Cache[T]) Keys() ([]string, error) {
	writeQueueKeys := c.writeQueue.Keys()
	engineKeys, err := c.engine.Keys()
	return slices.Concat(writeQueueKeys, engineKeys), err
}

// getNoLock gets a cached value by key
func (c *Cache[T]) getNoLock(key string) (*T, error) {
	// check write qeueue first
	value, found := c.writeQueue.Get(key)
	if found {
		// If the value is nil, it means the key is invalid (previously failed to write)
		if value == nil {
			return nil, ErrNotFound
		}
		return value, nil
	}

	untypedValue, err := c.engine.Get(key)
	if err == nil {
		if reflect.ValueOf(value).Kind() == reflect.Ptr {
			typedValue, ok := untypedValue.(*T)
			if !ok {
				return nil, ErrWrongDataType
			}
			return typedValue, nil
		} else {
			typedValue, ok := untypedValue.(T)
			if !ok {
				return nil, ErrWrongDataType
			}
			return &typedValue, nil
		}
	}

	return nil, err
}

// setNoLock stores a key-value pair into cache
func (c *Cache[T]) setNoLock(key string, value *T) error {
	c.writeQueue.Set(key, value)
	return nil
}

// getIndirectNoLock gets a key value following any intermediary links
func (c *Cache[T]) getIndirectNoLock(key string, linkResolver func(*T) string) (*T, error) {
	value, err := c.getNoLock(key)
	if err != nil {
		return nil, err
	}

	if linkResolver != nil {
		if link := linkResolver(value); len(link) > 0 && link != key {
			// here I want to have lock for link key
			return c.getIndirectNoLock(link, linkResolver)
		}
	}

	return value, nil
}

// setIndirectNoLock sets cache key including intermediary links
func (c *Cache[T]) setIndirectNoLock(key string, value *T, linkResolver func(*T) string, linkGenerator func(*T) *T) error {
	if linkGenerator != nil && linkResolver != nil {
		if linkValue := linkGenerator(value); linkValue != nil {
			link := linkResolver(linkValue)
			if link != key {
				lock := c.computeLocks.Lock(link)
				defer c.computeLocks.Unlock(link, lock)
				if err := c.setNoLock(key, linkValue); err != nil {
					return err
				}
			}

			return c.setNoLock(link, value)
		}
	}

	return c.setNoLock(key, value)
}

// writeLoop is a goroutine that processes the write queue.
func (c *Cache[T]) writeLoop() {
	var lastStatsTime time.Time
	for range time.Tick(c.writeInterval) {
		if time.Since(lastStatsTime) >= c.statsInterval {
			lastStatsTime = time.Now()
			queueSize, valuesSize := c.writeQueue.GetStats()
			c.logger.Print("Write queue stats: ", queueSize, " operations, ", valuesSize, " values")
		}
		for {
			op, ok := c.writeQueue.StartWriting()
			if !ok {
				break // No more items to process
			}

			switch op := op.(type) {
			case *queueOperationSet[T]:
				err := c.engine.Set(op.Key, op.Value)

				c.writeQueue.DoneWriting(err == nil)

			case *queueOperationDelete:
				err := c.engine.Delete(op.Key)

				c.writeQueue.DoneWriting(err == nil)

			case *queueOperationDeletePredicate:
				keys, err := c.engine.Keys()
				if err == nil {
					for _, key := range keys {
						if op.Predicate(key) {
							err = c.engine.Delete(key)
						}
					}
				}

				c.writeQueue.DoneWriting(err == nil)

			case *queueOperationPurge:
				err := c.engine.Purge()

				c.writeQueue.DoneWriting(err == nil)
			}
		}
	}
}
