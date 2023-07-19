package cachier

// CacheWithSubcache is a Cache with L1 subcache.
type CacheWithSubcache[T any] struct {
	Cache    *Cache[T]
	Subcache *Cache[T]
}

// Get gets a cached value by key
func (cs *CacheWithSubcache[T]) Get(key string) (interface{}, error) {
	value, err := cs.Subcache.GetOrCompute(key, func() (*T, error) {
		return cs.Cache.Get(key)
	})

	return *value, err
}

// Peek gets a cached key value without side-effects (i.e. without adding to L1 cache)
func (cs *CacheWithSubcache[T]) Peek(key string) (interface{}, error) {
	value, err := cs.Subcache.Peek(key)
	if err == nil {
		return value, err
	}
	return cs.Cache.Peek(key)
}

// Set stores a key-value pair into cache
func (cs *CacheWithSubcache[T]) Set(key string, value interface{}) error {
	typedValue, ok := value.(T)

	if !ok {
		return ErrWrongDataType
	}

	if err := cs.Subcache.Set(key, typedValue); err != nil {
		return err
	}
	return cs.Cache.Set(key, typedValue)
}

// Delete removes a key from cache
func (cs *CacheWithSubcache[T]) Delete(key string) error {
	if err := cs.Cache.Delete(key); err != nil {
		return err
	}
	return cs.Subcache.Delete(key)
}

// Keys returns a slice of all keys in the cache
func (cs *CacheWithSubcache[T]) Keys() ([]string, error) {
	return cs.Cache.Keys()
}

// Purge removes all the records from the cache
func (cs *CacheWithSubcache[T]) Purge() error {
	keys, err := cs.Keys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := cs.Delete(key); err != nil {
			return err
		}
	}
	return nil
}
