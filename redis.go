package cachier

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datasapiens/cachier/compression"
	"github.com/go-redis/redis/v8"
)

// Logger is interface for logging
type Logger interface {
	Error(...interface{})
	Warn(...interface{})
	Print(...interface{})
}

// DummyLogger is implementation of Logger that does not log anything
type DummyLogger struct{}

// Error does nothing
func (d DummyLogger) Error(...interface{}) {}

// Warn does nothing
func (d DummyLogger) Warn(...interface{}) {}

// Print does nothing
func (d DummyLogger) Print(...interface{}) {}

// RedisCache implements cachier.CacheTTL interface using redis storage
type RedisCache struct {
	redisClient       *redis.Client
	keyPrefix         string
	marshal           func(value interface{}) ([]byte, error)
	unmarshal         func(b []byte, value *interface{}) error
	ttl               time.Duration
	logger            Logger
	compressionEngine *compression.Engine
}

var ctx = context.Background()

// NewRedisCache is a constructor that creates a RedisCache
func NewRedisCache(
	redisClient *redis.Client,
	keyPrefix string,
	marshal func(value interface{}) ([]byte, error),
	unmarshal func(b []byte, value *interface{}) error,
	ttl time.Duration,
	compressionEngine *compression.Engine,
) *RedisCache {
	return &RedisCache{
		redisClient:       redisClient,
		keyPrefix:         keyPrefix,
		marshal:           marshal,
		unmarshal:         unmarshal,
		ttl:               ttl,
		logger:            DummyLogger{},
		compressionEngine: compressionEngine,
	}
}

// NewRedisCacheWithLogger is a constructor that creates a RedisCache
func NewRedisCacheWithLogger(
	redisClient *redis.Client,
	keyPrefix string,
	marshal func(value interface{}) ([]byte, error),
	unmarshal func(b []byte, value *interface{}) error,
	ttl time.Duration,
	logger Logger,
	compressionEngine *compression.Engine,
) *RedisCache {
	return &RedisCache{
		redisClient:       redisClient,
		keyPrefix:         keyPrefix,
		marshal:           marshal,
		unmarshal:         unmarshal,
		ttl:               ttl,
		logger:            logger,
		compressionEngine: compressionEngine,
	}
}

// Get gets a cached value by key
func (rc *RedisCache) Get(key string) (v interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
			v = nil
		}
	}()

	rc.logger.Print("redis get " + rc.keyPrefix + key)
	value, err := rc.redisClient.Get(ctx, rc.keyPrefix+key).Result()

	if err == redis.Nil {
		rc.logger.Print("redis: key not found:", key)
		return nil, ErrNotFound
	} else if err != nil {
		rc.logger.Error("redis: error getting data with key: ", key, " error: ", err)
		return nil, err
	}

	var input []byte
	if rc.compressionEngine == nil {
		input = []byte(value)
	} else {
		input, err = rc.compressionEngine.Decompress([]byte(value))
		if err != nil {
			// backward compatibility for not compressed entries
			rc.Delete(key)
			return nil, ErrNotFound
		}
	}

	var result interface{}
	rc.unmarshal(input, &result)
	return result, nil
}

// Peek gets a cached value by key without any sideeffects (identical as Get in this implementation)
func (rc *RedisCache) Peek(key string) (interface{}, error) {
	return rc.Get(key)
}

// Set stores a key-value pair into cache
func (rc *RedisCache) Set(key string, value interface{}) (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	marshalledValue, err := rc.marshal(value)
	if err != nil {
		rc.logger.Error("redis: error marshaling data: ", err)
		return err
	}

	var input []byte
	if rc.compressionEngine == nil {
		input = marshalledValue
	} else {
		input, err = rc.compressionEngine.Compress(marshalledValue)
		if err != nil {
			rc.logger.Error("redis: error compressing data: ", err)
			return err
		}
	}

	rc.logger.Print("redis set " + rc.keyPrefix + key)
	status := rc.redisClient.Set(ctx, rc.keyPrefix+key, input, rc.ttl)
	if status.Err() != nil {
		rc.logger.Error("redis: error setting data in cache: ", err)
		return status.Err()
	}
	return nil
}

// Delete removes a key from cache
func (rc *RedisCache) Delete(key string) error {
	return rc.redisClient.Del(ctx, rc.keyPrefix+key).Err()
}

// Keys returns all the keys in the cache
func (rc *RedisCache) Keys() ([]string, error) {
	keys, err := rc.redisClient.Keys(ctx, rc.keyPrefix+"*").Result()
	if err != nil {
		return nil, err
	}

	strippedKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		strippedKeys = append(strippedKeys, strings.TrimPrefix(key, rc.keyPrefix))
	}

	return strippedKeys, nil
}

// Purge removes all the records from the cache
func (rc *RedisCache) Purge() error {
	//FIXME: delete all keys from redis at once
	keys, err := rc.Keys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := rc.Delete(key); err != nil {
			return err
		}
	}
	return nil
}
