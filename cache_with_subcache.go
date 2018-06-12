package cachier

// CacheWithSubcache is a Cache with L1 subcache.
type CacheWithSubcache struct {
	Cache    *Cache
	Subcache *Cache
}

// Get gets a cached value by key
func (cs *CacheWithSubcache) Get(key string) (interface{}, error) {
	return cs.Subcache.GetOrCompute(key, func() (interface{}, error) { return cs.Cache.Get(key) })
}

// Peek gets a cached key value without side-effects (i.e. without adding to L1 cache)
func (cs *CacheWithSubcache) Peek(key string) (interface{}, error) {
	value, err := cs.Subcache.Peek(key)
	if err == nil {
		return value, err
	}
	return cs.Cache.Peek(key)
}

// Set stores a key-value pair into cache
func (cs *CacheWithSubcache) Set(key string, value interface{}) error {
	if err := cs.Subcache.Set(key, value); err != nil {
		return err
	}
	return cs.Cache.Set(key, value)
}

// Delete removes a key from cache
func (cs *CacheWithSubcache) Delete(key string) error {
	if err := cs.Cache.Delete(key); err != nil {
		return err
	}
	return cs.Subcache.Delete(key)
}

// Keys returns a slice of all keys in the cache
func (cs *CacheWithSubcache) Keys() ([]string, error) {
	return cs.Cache.Keys()
}

// Purge removes all the records from the cache
func (cs *CacheWithSubcache) Purge() error {
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
