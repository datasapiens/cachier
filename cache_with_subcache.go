package cachier

// CacheWithSubcache is a Cache with L1 subcache.
type CacheWithSubcache[Meta any, Data any] struct {
	Cache    *Cache[Meta, Data]
	Subcache *Cache[Meta, Data]
}

// Get gets a cached value by key
func (cs *CacheWithSubcache[M, D]) Get(key string) (interface{}, error) {
	return cs.Subcache.GetOrCompute(key, func() (*D, error) {
		value, err := cs.Cache.Get(key)
		if err == nil {
			typedValue, ok := value.(D)
			if ok {
				return &typedValue, nil
			}

			err = ErrNotFound
		}
		return nil, err
	})
}

// Peek gets a cached key value without side-effects (i.e. without adding to L1 cache)
func (cs *CacheWithSubcache[M, D]) Peek(key string) (interface{}, error) {
	value, err := cs.Subcache.Peek(key)
	if err == nil {
		return value, err
	}
	return cs.Cache.Peek(key)
}

// Set stores a key-value pair into cache
func (cs *CacheWithSubcache[M, D]) Set(key string, value interface{}) error {
	if err := cs.Subcache.Set(key, value); err != nil {
		return err
	}
	return cs.Cache.Set(key, value)
}

// Delete removes a key from cache
func (cs *CacheWithSubcache[M, D]) Delete(key string) error {
	if err := cs.Cache.Delete(key); err != nil {
		return err
	}
	return cs.Subcache.Delete(key)
}

// Keys returns a slice of all keys in the cache
func (cs *CacheWithSubcache[M, D]) Keys() ([]string, error) {
	return cs.Cache.Keys()
}

// Purge removes all the records from the cache
func (cs *CacheWithSubcache[M, D]) Purge() error {
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
