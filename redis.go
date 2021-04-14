package cachier

import (
	"context"
	"strings"
	"time"

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

//
// RedisCache implements cachier.CacheTTL interface using redis storage
type RedisCache struct {
	redisClient *redis.Client
	keyPrefix   string
	marshal     func(value interface{}) ([]byte, error)
	unmarshal   func(b []byte, value *interface{}) error
	ttl         time.Duration
	logger      Logger
}

var ctx = context.Background()

// NewRedisCache is a constructor that creates a RedisCache
func NewRedisCache(
	redisClient *redis.Client,
	keyPrefix string,
	marshal func(value interface{}) ([]byte, error),
	unmarshal func(b []byte, value *interface{}) error,
	ttl time.Duration,
) *RedisCache {
	return &RedisCache{
		redisClient: redisClient,
		keyPrefix:   keyPrefix,
		marshal:     marshal,
		unmarshal:   unmarshal,
		ttl:         ttl,
		logger:      DummyLogger{},
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
) *RedisCache {
	return &RedisCache{
		redisClient: redisClient,
		keyPrefix:   keyPrefix,
		marshal:     marshal,
		unmarshal:   unmarshal,
		ttl:         ttl,
		logger:      logger,
	}
}

// Get gets a cached value by key
func (rc *RedisCache) Get(key string) (interface{}, error) {
	rc.logger.Print("redis get " + rc.keyPrefix + key)
	value, err := rc.redisClient.Get(ctx, rc.keyPrefix+key).Result()

	if err == redis.Nil {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}

	var result interface{}
	rc.unmarshal([]byte(value), &result)
	return result, nil
}

// Peek gets a cached value by key without any sideeffects (identical as Get in this implementation)
func (rc *RedisCache) Peek(key string) (interface{}, error) {
	return rc.Get(key)
}

// Set stores a key-value pair into cache
func (rc *RedisCache) Set(key string, value interface{}) error {
	marshalledValue, err := rc.marshal(value)
	if err != nil {
		return err
	}
	rc.logger.Print("redis set " + rc.keyPrefix + key)
	return rc.redisClient.Set(ctx, rc.keyPrefix+key, marshalledValue, rc.ttl).Err()
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
