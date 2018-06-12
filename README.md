# Cachier

Cachier is a Go library that provides an interfaces for dealing with cache.
There is an CacheEngine interface which requires you to implement common cache
methods (like Get, Set, Delete, etc). When implemented, you wrap this
CacheEngine into the Cache struct and you'll get GetOrCompute method in return.
This method is a shortcut for fetching a value from cache. If the value
is not found, it is evaluated and stored back in the cache.

There are also three implementations included:

 - LRUCache: a wrapper of hashicorp/golang-lru which fulfills the CacheEngine
   interface

 - RedisCache: CacheEngine based on redis

 - CacheWithSubcache: Implementation of combination of primary cache with fast
   L1 subcache. E.g. primary Redis cache and fast (and small) LRU subcache.
   But any other implementations of CacheEngine can be used.
