package cachier

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/datasapiens/cachier/compression"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func InitLRUCache() *Cache {
	lc, err := NewLRUCache(300, nil, nil, nil)
	if err != nil {
		panic(err)
	}
	return MakeCache(lc)
}

func InitRedis() (*redis.Client, error) {
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost:6379"
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})

	_, err := redisClient.Ping(ctx).Result()

	return redisClient, err
}

func InitRedisCache() (*Cache, error) {
	redisClient, err := InitRedis()

	rc := NewRedisCache(
		redisClient,
		"",
		func(value interface{}) ([]byte, error) {
			return json.Marshal(value)
		},
		func(b []byte, value *interface{}) error {
			return json.Unmarshal(b, value)
		},
		0,
		nil,
	)

	return MakeCache(rc), err
}

func RandStringRunes(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func SetGet(c *Cache, t *testing.T) {
	key := RandStringRunes(10)
	value := rand.ExpFloat64()

	c.Set(key, value)
	cached, err := c.Get(key)
	if err != nil {
		t.Error(err)
	}
	cachedF, ok := cached.(float64)
	if !ok {
		t.Error("received non float")
	}
	if cachedF != value {
		t.Errorf("Expected cachedF to be %f, got %f instead.", value, cachedF)
	}
}

func dosCache(c *Cache, t *testing.T, n int) {
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			SetGet(c, t)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestLRUCache(t *testing.T) {
	c := InitLRUCache()
	dosCache(c, t, 300)
}

func TestRedisCache(t *testing.T) {
	c, err := InitRedisCache()
	if err != nil {
		t.Skipf("skipping because of redis error: %s", err.Error())
	}
	dosCache(c, t, 1000)
}

func TestCacheWithSubCache(t *testing.T) {
	lru := InitLRUCache()
	rc, err := InitRedisCache()

	if err != nil {
		t.Skipf("skipping because of redis error: %s", err.Error())
	}

	c := MakeCache(&CacheWithSubcache{
		Cache:    rc,
		Subcache: lru,
	})

	dosCache(c, t, 1000)
}

func TestRedisCacheWithCompressionJSON(t *testing.T) {
	redisClient, err := InitRedis()
	if err != nil {
		t.Skipf("skipping because of redis error: %s", err.Error())
	}

	rc := NewRedisCache(
		redisClient,
		"",
		func(value interface{}) ([]byte, error) {
			return json.Marshal(value)
		},
		func(b []byte, value *interface{}) error {
			return json.Unmarshal(b, value)
		},
		0,
		compression.NewEngine(),
	)

	cache := MakeCache(rc)
	s := "hello world"
	r := []byte(strings.Repeat(s, 100))
	input := fmt.Sprintf("{\"key\":\"%s\"", string(r))
	key := "hello:world:json:1"
	cache.Delete(key)
	err = cache.Set(key, input)
	require.Nil(t, err)
	output, err := cache.Get(key)
	require.Nil(t, err)
	assert.Equal(t, input, output)

	key = "hello:world:json:2"
	input = fmt.Sprintf("{\"key\":\"%s\"", s)
	cache.Delete(key)
	err = cache.Set(key, input)
	require.Nil(t, err)
	output, err = cache.Get(key)
	require.Nil(t, err)
	assert.Equal(t, input, output)

}

func TestRedisCacheWithCompressionGOB(t *testing.T) {
	redisClient, err := InitRedis()
	if err != nil {
		t.Skipf("skipping because of redis error: %s", err.Error())
	}

	type A struct {
		ID  int
		Key string
	}

	rc := NewRedisCache(
		redisClient,
		"",
		func(value interface{}) ([]byte, error) {
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			enc.Encode(value)
			return buf.Bytes(), nil
		},
		func(b []byte, value *interface{}) error {
			var res *A
			buf := bytes.NewBuffer(b)
			dec := gob.NewDecoder(buf)
			if err := dec.Decode(&res); err != nil {
				return err
			}
			*value = res
			return nil
		},
		0,
		compression.NewEngine(),
	)

	cache := MakeCache(rc)
	s := "hello world"
	r := []byte(strings.Repeat(s, 100))
	key := "hello:world:gob"
	cache.Delete(key)
	a := A{
		ID:  1,
		Key: string(r),
	}
	err = cache.Set(key, &a)
	require.Nil(t, err)
	output, err := cache.Get(key)
	require.Nil(t, err)
	u, ok := output.(*A)
	require.True(t, ok)
	assert.Equal(t, a.Key, u.Key)
}

func TestLRUCacheWithCompressionJSON(t *testing.T) {
	lc, err := NewLRUCache(300,
		func(value interface{}) ([]byte, error) {
			return json.Marshal(value)
		},
		func(b []byte, value *interface{}) error {
			return json.Unmarshal(b, value)
		},
		compression.NewEngine())
	if err != nil {
		panic(err)
	}
	cache := MakeCache(lc)

	s := "hello world"
	r := []byte(strings.Repeat(s, 100))
	input := fmt.Sprintf("{\"key\":\"%s\"", string(r))
	key := "hello:world:json:1"
	cache.Delete(key)
	err = cache.Set(key, input)
	require.Nil(t, err)
	output, err := cache.Get(key)
	require.Nil(t, err)
	assert.Equal(t, input, output)

	key = "hello:world:json:2"
	input = fmt.Sprintf("{\"key\":\"%s\"", s)
	cache.Delete(key)
	err = cache.Set(key, input)
	require.Nil(t, err)
	output, err = cache.Get(key)
	require.Nil(t, err)
	assert.Equal(t, input, output)
}

func TestLRUCacheWithCompressionGOB(t *testing.T) {
	type A struct {
		ID  int
		Key string
	}
	lc, err := NewLRUCache(300,
		func(value interface{}) ([]byte, error) {
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			enc.Encode(value)
			return buf.Bytes(), nil
		},
		func(b []byte, value *interface{}) error {
			var res *A
			buf := bytes.NewBuffer(b)
			dec := gob.NewDecoder(buf)
			if err := dec.Decode(&res); err != nil {
				return err
			}
			*value = res
			return nil
		},
		compression.NewEngine())
	if err != nil {
		panic(err)
	}
	cache := MakeCache(lc)
	s := "hello world"
	r := []byte(strings.Repeat(s, 100))
	key := "hello:world:gob"
	cache.Delete(key)
	a := A{
		ID:  1,
		Key: string(r),
	}
	err = cache.Set(key, &a)
	require.Nil(t, err)
	output, err := cache.Get(key)
	require.Nil(t, err)
	u, ok := output.(*A)
	require.True(t, ok)
	assert.Equal(t, a.Key, u.Key)
}
