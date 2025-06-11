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
	"sync"
	"time"

	"github.com/datasapiens/cachier/utils"
)

// Errors
var (
	ErrNotFound      = errors.New("key not found")
	ErrWrongDataType = errors.New("data in wrong format")
)

// kvPair is a key-value pair used for storing in cache.
type kvPair[T any] struct {
	Key   string
	Value *T
}

// writeQueue is not actually a queue, but a map with a maximum size.
type writeQueue[T any] struct {
	sync.Mutex
	Values          map[string]*T       // Map to hold the values for the keys
	CurrentyWriting *kvPair[T]          // Key-value pair being currently written
	InvalidKeys     map[string]struct{} // Keys that failed to write or were not added due to size limit
	Size            int                 // Maximum size of the queue
}

// newWriteQueue creates a new CircularQueue with the specified size
func newWriteQueue[T any](size int) *writeQueue[T] {
	if size <= 0 {
		size = 100 // Default size if invalid size is provided
	}
	return &writeQueue[T]{
		Values:          make(map[string]*T, size),
		InvalidKeys:     make(map[string]struct{}, size), // Map to hold invalid keys
		CurrentyWriting: nil,                             // No key-value pair is currently being written
		Size:            size,
	}
}

// Enqueue adds a new key-value pair to the queue, overwriting the oldest entry if the queue is full
func (q *writeQueue[T]) Enqueue(key string, value *T) {
	q.Lock()
	defer q.Unlock()

	_, exists := q.Values[key]

	// If the key is not already in queue and the queue is full, mark it as invalid
	if !exists && len(q.Values) >= q.Size {
		q.InvalidKeys[key] = struct{}{} // Mark the key as invalid if the queue is full
		return
	}

	delete(q.InvalidKeys, key) // Remove the key from invalid keys
	q.Values[key] = value
}

// Get retrieves the value for a given key from the queue.
//
//	Returns nil, true if the key is invalid. Returns nil, false if the key was not found.
func (q *writeQueue[T]) Get(key string) (*T, bool) {
	q.Lock()
	defer q.Unlock()

	value, exists := q.Values[key]
	if exists {
		return value, true
	}

	if q.CurrentyWriting != nil && q.CurrentyWriting.Key == key {
		return q.CurrentyWriting.Value, true
	}

	if _, invalid := q.InvalidKeys[key]; invalid {
		return nil, true // Key is invalid, return nil
	}

	return nil, false // Key not found
}

// Delete removes a key from the queue
func (q *writeQueue[T]) Delete(key string) {
	q.Lock()
	defer q.Unlock()
	delete(q.Values, key)      // Remove the key from the queue
	delete(q.InvalidKeys, key) // If the key is invalid, remove it from invalid keys
}

// DeletePredicate removes all keys matching the supplied predicate
func (q *writeQueue[T]) DeletePredicate(pred Predicate) []string {
	q.Lock()
	defer q.Unlock()
	removedKeys := make([]string, 0)
	for key := range q.Values {
		if pred(key) {
			delete(q.Values, key) // Remove the key from the queue
			removedKeys = append(removedKeys, key)
		}
	}
	for key := range q.InvalidKeys {
		if pred(key) {
			delete(q.InvalidKeys, key) // Remove the key from invalid keys
			removedKeys = append(removedKeys, key)
		}
	}
	return removedKeys // Return the list of removed keys
}

// Count returns the number of keys in the queue
func (q *writeQueue[T]) Count() int {
	q.Lock()
	defer q.Unlock()
	count := len(q.Values) + len(q.InvalidKeys) // Count both valid and invalid keys
	if q.CurrentyWriting != nil {
		count++ // Include the current writing key-value pair if it exists
	}
	return count
}

// CountPredicate counts the number of keys in the queue that satisfy the given predicate
func (q *writeQueue[T]) CountPredicate(pred Predicate) int {
	q.Lock()
	defer q.Unlock()
	count := 0
	for key := range q.Values {
		if pred(key) {
			count++ // Count valid keys that satisfy the predicate
		}
	}
	for key := range q.InvalidKeys {
		if pred(key) {
			count++ // Count invalid keys that satisfy the predicate
		}
	}
	if q.CurrentyWriting != nil && pred(q.CurrentyWriting.Key) {
		count++ // Include the current writing key-value pair if it satisfies the predicate
	}
	return count // Return the total count
}

// Purge removes all records from the queue
func (q *writeQueue[T]) Purge() {
	q.Lock()
	defer q.Unlock()
	q.Values = make(map[string]*T, q.Size)            // Reset the values map
	q.InvalidKeys = make(map[string]struct{}, q.Size) // Reset the invalid keys map
}

// Keys returns all the keys in the queue
func (q *writeQueue[T]) Keys() []string {
	q.Lock()
	defer q.Unlock()
	keys := make([]string, 0, len(q.Values)+len(q.InvalidKeys))
	for key := range q.Values {
		keys = append(keys, key) // Add valid keys
	}
	for key := range q.InvalidKeys {
		keys = append(keys, key) // Add invalid keys
	}
	if q.CurrentyWriting != nil {
		keys = append(keys, q.CurrentyWriting.Key) // Include the current writing key if it exists
	}
	return keys // Return the list of all keys
}

// StartWriting removes the oldest key-value pair from the queue
func (q *writeQueue[T]) StartWriting() (*kvPair[T], bool) {
	q.Lock()
	defer q.Unlock()

	for key, value := range q.Values {
		q.CurrentyWriting = &kvPair[T]{Key: key, Value: value} // Set the current writing key-value pair
		delete(q.Values, key)                                  // Remove it from the queue
		return q.CurrentyWriting, true                         // Return the first key-value pair found
	}

	return nil, false // Queue is empty
}

// DoneWriting marks the current writing key-value pair as done
func (q *writeQueue[T]) DoneWriting(ok bool) {
	q.Lock()
	defer q.Unlock()
	if q.CurrentyWriting != nil {
		if !ok {
			// If writing was not successful, mark the key as invalid
			q.InvalidKeys[q.CurrentyWriting.Key] = struct{}{}
		}
		q.CurrentyWriting = nil // Reset the current writing key-value pair
	}
}

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
}

// MakeCache creates cache with provided engine
func MakeCache[T any](engine CacheEngine) *Cache[T] {
	cache := &Cache[T]{
		engine:        engine,
		computeLocks:  *utils.NewMutexMap(),
		writeQueue:    newWriteQueue[T](1000),
		writeInterval: 1000 * time.Millisecond, // Default write interval
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
func (c *Cache[T]) DeletePredicate(pred Predicate) ([]string, error) {
	removedKeys := c.writeQueue.DeletePredicate(pred)

	keys, err := c.Keys()
	if err != nil {
		return removedKeys, err
	}

	for _, key := range keys {
		if pred(key) {
			mutex := c.computeLocks.Lock(key)
			if err := c.engine.Delete(key); err != nil {
				c.computeLocks.Unlock(key, mutex)
				return removedKeys, err
			}
			c.computeLocks.Unlock(key, mutex)
			removedKeys = append(removedKeys, key)
		}
	}

	return removedKeys, nil
}

// DeleteWithPrefix removes all keys that start with given prefix, returns number of deleted keys
func (c *Cache[T]) DeleteWithPrefix(prefix string) ([]string, error) {
	pred := func(s string) bool {
		return strings.HasPrefix(s, prefix)
	}

	return c.DeletePredicate(pred)
}

// DeleteRegExp deletes all keys matching the supplied regexp, returns number of deleted keys
func (c *Cache[T]) DeleteRegExp(pattern string) ([]string, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
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
	c.writeQueue.Enqueue(key, value)
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
	for range time.Tick(c.writeInterval) {
		for {
			pair, ok := c.writeQueue.StartWriting()
			if !ok {
				break // No more items to process
			}

			err := c.engine.Set(pair.Key, pair.Value)
			if err != nil {
				c.writeQueue.DoneWriting(false) // Mark as invalid
				continue
			}

			c.writeQueue.DoneWriting(true) // Mark as successfully written
		}
	}
}
