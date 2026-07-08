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

// BulkDeleter is an optional CacheEngine extension: engines that can delete
// many keys cheaply (e.g. redis via batched UNLINK) implement it, and the
// write loop uses it instead of one Delete round trip per key.
type BulkDeleter interface {
	DeleteMany(keys []string) error
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

	// engineMu makes each write-loop cycle (StartWriting-engine-op-
	// DoneWriting) atomic as a unit, so StartWriting's single-consumer
	// invariant holds even if a drain is ever driven from more than one
	// goroutine (e.g. tests calling runOneWriteCycle directly).
	engineMu sync.Mutex

	ticker    *time.Ticker
	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
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
	cache.ticker = time.NewTicker(cache.writeInterval)
	cache.done = make(chan struct{})
	cache.stopped = make(chan struct{})

	go cache.writeLoop() // Start the write loop in a goroutine

	return cache
}

// Close stops the background write loop, waiting for it to perform one
// final best-effort drain and exit. Idempotent and safe to call from
// multiple goroutines. Reads and computes keep working after Close, but
// writes AND invalidations enqueued after the final drain never reach the
// engine (they only mask in-process reads), so stop producing both before
// calling Close. On a Cache built without MakeCache (in-package tests)
// there is no write loop and Close is a no-op.
func (c *Cache[T]) Close() {
	c.closeOnce.Do(func() {
		if c.ticker != nil {
			c.ticker.Stop()
		}
		if c.done != nil {
			close(c.done)
		}
	})
	if c.stopped != nil {
		<-c.stopped
	}
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

	token := c.writeQueue.RegisterToken(key)
	calculatedValue, evaluatorErr := evaluator()

	if evaluatorErr == nil {
		// Key not found on cache
		go func() {
			c.setNoLock(key, calculatedValue, token)
			c.writeQueue.DeregisterToken(key, token)
			c.computeLocks.Unlock(key, mutex)
		}()
		return calculatedValue, nil
	} else {
		// evalutation error
		c.writeQueue.DeregisterToken(key, token)
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
	return c.setNoLock(key, value, nil)
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
	return c.setIndirectNoLock(key, value, linkResolver, linkGenerator, nil)
}

// GetOrComputeEx tries to get value from cache.
// If the cache has no servable value (miss, wrong type, transient engine
// error or validator rejection), it computes the value using the provided
// evaluator function and stores it into cache (subject to writeApprover).
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

	if err != nil && err != ErrNotFound {
		c.logger.Error("error getting value from cache: ", err)
	}

	token := c.writeQueue.RegisterToken(key)
	defer c.writeQueue.DeregisterToken(key, token)

	value, evaluatorErr := evaluator()
	if evaluatorErr != nil {
		return nil, evaluatorErr
	}

	// Reaching the evaluator means the cache had no servable value (miss,
	// wrong type, transient engine error or validator rejection), so the
	// fresh value always replaces whatever is stored — unless an
	// invalidation fenced this compute while it was in flight.
	if writeApprover == nil || writeApprover(value) {
		c.setIndirectNoLock(key, value, linkResolver, linkGenerator, token)
	}

	return value, nil
}

// DeletePredicate deletes all keys matching the supplied predicate. Queued
// writes for matching keys are discarded, in-flight computes for them are
// fenced (their write-back is skipped while the computed value is still
// returned to their callers), and the engine-side delete is enqueued for
// the write loop — this call never blocks on the engine, so invalidating
// a large keyspace adds no caller latency. Reads see the deletion
// immediately via the queued op. Always returns nil: engine failures are
// logged and retried head-of-line by the write loop until they succeed; a
// hard crash before the flush loses the queued invalidation (TTL is the
// backstop).
func (c *Cache[T]) DeletePredicate(pred Predicate) error {
	c.writeQueue.DeletePredicate(pred)

	return nil
}

// DeleteWithPrefix removes all keys that start with the given prefix. Same
// fencing and write-back semantics as DeletePredicate.
func (c *Cache[T]) DeleteWithPrefix(prefix string) error {
	pred := func(s string) bool {
		return strings.HasPrefix(s, prefix)
	}

	return c.DeletePredicate(pred)
}

// DeleteRegExp deletes all keys matching the supplied regexp. Same fencing
// and write-back semantics as DeletePredicate.
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
	if err != nil {
		return nil, err
	}

	return assertCached[T](untypedValue)
}

// Delete removes a key from cache: queued writes for it are discarded and
// the engine-side delete is enqueued for the write loop. It holds the key's
// compute lock, so no compute for this key can be in flight (GetOrCompute's
// write-back goroutine holds that lock until it finishes). Reads see the
// deletion immediately via the queued op. Always returns nil — see
// DeletePredicate for the failure semantics.
func (c *Cache[T]) Delete(key string) error {
	mutex := c.computeLocks.Lock(key)
	defer c.computeLocks.Unlock(key, mutex)

	c.writeQueue.Delete(key)
	return nil
}

// Purge removes all records from the cache: every queued write is dropped,
// every in-flight compute is fenced, and the engine purge is enqueued for
// the write loop. Always returns nil — see DeletePredicate for the failure
// semantics.
func (c *Cache[T]) Purge() error {
	c.writeQueue.Purge()

	return nil
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
	if err != nil {
		return nil, err
	}

	return assertCached[T](untypedValue)
}

// assertCached converts a value read from a cache engine into *T. Engines
// return either the raw *T that was stored (e.g. LRU without compression) or
// a value produced by an unmarshal callback, which may be T or *T depending
// on the callback's convention — all of them must be servable.
func assertCached[T any](v interface{}) (*T, error) {
	if typedValue, ok := v.(*T); ok {
		return typedValue, nil
	}
	if typedValue, ok := v.(T); ok {
		return &typedValue, nil
	}
	return nil, ErrWrongDataType
}

// setNoLock stores a key-value pair into cache. A non-nil token fences the
// write: it is skipped when the token was invalidated by a concurrent
// invalidation (H4).
func (c *Cache[T]) setNoLock(key string, value *T, token *computeToken) error {
	c.writeQueue.TrySet(key, value, token)
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

// setIndirectNoLock sets cache key including intermediary links. The token
// (fencing the compute registered for key, nil for unfenced writes) gates
// each leaf write; a predicate matching only the derived link key marks no
// token, so such writes are only caught while still queued — a pre-existing
// granularity limit of the link feature.
func (c *Cache[T]) setIndirectNoLock(key string, value *T, linkResolver func(*T) string, linkGenerator func(*T) *T, token *computeToken) error {
	if linkGenerator != nil && linkResolver != nil {
		if linkValue := linkGenerator(value); linkValue != nil {
			link := linkResolver(linkValue)
			if link != key {
				lock := c.computeLocks.Lock(link)
				defer c.computeLocks.Unlock(link, lock)
				if err := c.setNoLock(key, linkValue, token); err != nil {
					return err
				}
			}

			return c.setNoLock(link, value, token)
		}
	}

	return c.setNoLock(key, value, token)
}

// writeLoop is a goroutine that processes the write queue.
func (c *Cache[T]) writeLoop() {
	defer close(c.stopped)
	var lastStatsTime time.Time
	for {
		select {
		case <-c.done:
			// Best-effort final drain so a graceful shutdown does not
			// strand queued writes.
			for c.runOneWriteCycle() {
			}
			return
		case <-c.ticker.C:
		}

		for c.runOneWriteCycle() {
		}

		if time.Since(lastStatsTime) >= c.statsInterval {
			queueSize, valuesSize := c.writeQueue.GetStats()
			if queueSize > 0 || valuesSize > 0 {
				lastStatsTime = time.Now()
				c.logger.Print("Write queue stats: ", queueSize, " operations, ", valuesSize, " values")
			}
		}
	}
}

// runOneWriteCycle processes at most one queued operation under engineMu,
// so a whole StartWriting-engine-op-DoneWriting cycle is atomic and
// StartWriting's single-consumer invariant holds even if a drain is ever
// driven from more than one goroutine. Returns false when the caller
// should stop draining: the queue is empty, or the op failed, stays at the
// front, and must wait for the next tick instead of hot-spinning.
func (c *Cache[T]) runOneWriteCycle() bool {
	c.engineMu.Lock()
	defer c.engineMu.Unlock()

	op, ok := c.writeQueue.StartWriting()
	if !ok {
		return false // No more items to process
	}

	var err error

	switch op := op.(type) {
	case *queueOperationSet[T]:
		err = c.engine.Set(op.Key, op.Value)

	case *queueOperationDelete:
		err = c.engine.Delete(op.Key)

	case *queueOperationDeletePredicate:
		var keys []string
		keys, err = c.engine.Keys()
		if err == nil {
			matching := make([]string, 0, len(keys))
			for _, key := range keys {
				if op.Predicate(key) {
					matching = append(matching, key)
				}
			}
			err = c.deleteKeys(matching)
		}

	case *queueOperationPurge:
		err = c.engine.Purge()
	}

	c.writeQueue.DoneWriting(err == nil)

	if err != nil {
		c.logger.Print("write loop error: ", err.Error(), " for operation: ", op.String())
		return false
	}
	return true
}

// deleteKeys removes the given keys from the engine, using the engine's
// bulk fast path when it has one.
func (c *Cache[T]) deleteKeys(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if bulk, ok := c.engine.(BulkDeleter); ok {
		return bulk.DeleteMany(keys)
	}
	for _, key := range keys {
		if err := c.engine.Delete(key); err != nil {
			return err
		}
	}
	return nil
}
