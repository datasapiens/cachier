[![Build Status](https://travis-ci.org/datasapiens/cachier.svg?branch=master)](https://travis-ci.org/datasapiens/cachier)
[![GoDoc](https://godoc.org/github.com/datasapiens/cachier?status.svg)](https://godoc.org/github.com/datasapiens/cachier)

# Cachier

Cachier is a Go library that provides an interface for dealing with cache.
There is a CacheEngine interface which requires you to implement common cache
methods (like Get, Set, Delete, etc). When implemented, you wrap this
CacheEngine into the Cache struct. This struct has some methods implemented
like GetOrCompute method (shortcut for fetching a hit or computing/writing
a miss).

There are also three implementations included:

 - LRUCache: a wrapper of hashicorp/golang-lru which fulfills the CacheEngine
   interface

 - RedisCache: CacheEngine based on redis

 - CacheWithSubcache: Implementation of combination of primary cache with fast
   L1 subcache. E.g. primary Redis cache and fast (and small) LRU subcache.
   But any other implementations of CacheEngine can be used.
