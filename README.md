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

# Compression

Compression can be used with Redis Cache. There are three compression providers implemented: 
- `Zstd` (github.com/DataDog/zstd),
- `S2` (github.com/klauspost/compress/s2),
- `Lz4` (github.com/cloudflare/golz4).

Every provider has an unique identifier (ID). Provider id must be <= 255. It must be written in one byte

- `NoCompression`   - 0 - special provider for small input data (<= 1KB)
- `Zstd` - 1
- `S2`   - 2 
- `Lz4`  - 3


Input data which are smaller or equal 1KB are never compressed by default

The definition of functions `NewRedisCache` and `NewRedisCacheWithLogger` is extend and the last function argument is the pointer to `compression.Engine`.
If  the `*compression.Engine` == `nil` data are not compressed.

The compression engine uses compresison providers to compress and decompress data. Data are always compressed with the default provider but can be decompressed by multiple providers. Based on the footer added to compressed data the engine selects the right provider to decompress data 

In order to start using compression add `*compression.Engine` to the redis cache constructor

``` 
engine, err := compression.NewEngine(providerID,compressionParameters)
NewRedisCache(
	redisClient 
	keyPrefix,
	marshal,
	unmarshal,
	ttl,
	engine,
```
Where `compression.NewEngine(providerID byte, compressionParameters map[string]interface{})` creates `*compression.Engine` where
- default compression engine is selected based on providerID
- supported compression providers: zstd, lz4, s2
- input <= 1 KB is not compressed
- compressionParameters map contains compression parameters which are used to configure the engine and providers. Two parameters are currently supported:
	- level: compression level used by zstd compression
	- minInputLen: minimum length of data which are compresed, input <= minInputLen is not compressed

Provider id can be:
- 0 - no compression, the function returns nil, nil
- 1 - zstd compression
- 2 - s2 compression
- 3 - lz4 compression

If the engine is created in the following way `compression.NewEngine(1,nil)`
 the data are compressed with Zstd method.

Other compression providers can be easily added to the `Engine`:

- `compression.Engine.AddDefaultProvider(compression.S2CompressionService)` - adds new default compression provider to the engine; since now data are compressed using the s2 provider. The old inputs (alredy compressed with zstd) can be decompressed becasue  the engine contains two providers: zstd, s2

- `compression.Engine.AddProvider(compression.Lz4CompressionService)` - adds new compression provider to the engine; the default provider is not changed

The defult size of not compressed input can be easily changed:

-  `compression.NewEngine(providerID, nil).SetMinInputSize(2048)` -since now input <= 2 KB is not compressed
-  `compression.NewEngine(providerID byte, map[string]interface {} {"minInputLen": 2048}`

If the provider is already added to the `Engine` the default provider can be selected by the provider id
- `compression.Engine.SetDefaultProvider(2)`
- `compression.Engine.SetDefaultProvider(ProviderIDS2)`

## Footer

How does the `Engine` know which provider should be used to decompress data?

There is added a footer to the compressed data. The footer size is:
- 1 byte for `NoCompressionService`
    - compressed_data + provider_id(1 byte)
- 9 bytes for other compression providers
    - compressed_data + size_of_not_compressed_data(8 bytes) + provider_id(1 byte)

## How to implement a new compression provider?

Compression provider has to implement an interface `compression.Provider` by implementing its methods
- `Compress(src []byte) ([]byte, error)`
- `Decompress(src []byte, dstSize int) ([]byte, error)`
- `GetID() byte`
- `Configure(params CompressionParams) error`

Provider cannot manage the footer. The footer is manged by the `Engine`. The `Engine`:
- adds footer to compressed data,
- extracts the compressed data and footer from the input; the providers is suplied with the input without the footer. 

## How change compression level for ZSTD?

Providers can be configured using `compressionParameters`. For example: 
```
NewEngine(ProviderIDZstd, map[string]interface{}{
		CompressionParamLevel: 5,
	})

