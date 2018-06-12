package cachier

import (
	"encoding/json"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/go-redis/redis"
)

func InitLRUCache() *Cache {
	lc, err := NewLRUCache(300)
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

	_, err := redisClient.Ping().Result()

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
