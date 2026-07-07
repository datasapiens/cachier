package cachier

import (
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/datasapiens/cachier/compression"
	"github.com/datasapiens/cachier/utils"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testEntry is the cached value type used across the read-path tests.
type testEntry struct {
	ID  int
	Key string
}

// newTestCache wraps an engine in a Cache without starting the background
// write loop so tests can flush the write queue deterministically.
func newTestCache[T any](engine CacheEngine) *Cache[T] {
	return &Cache[T]{
		engine:       engine,
		computeLocks: *utils.NewMutexMap(),
		writeQueue:   newWriteQueue[T](),
		logger:       DummyLogger{},
	}
}

// flushWriteQueue synchronously drains all pending operations into the
// engine, replicating one writeLoop pass.
func flushWriteQueue[T any](t *testing.T, c *Cache[T]) {
	t.Helper()
	for {
		op, ok := c.writeQueue.StartWriting()
		if !ok {
			return
		}
		var err error
		switch op := op.(type) {
		case *queueOperationSet[T]:
			err = c.engine.Set(op.Key, op.Value)
		case *queueOperationDelete:
			err = c.engine.Delete(op.Key)
		case *queueOperationDeletePredicate:
			var keys []string
			keys, err = c.engine.Keys()
			if err == nil {
				for _, key := range keys {
					if op.Predicate(key) {
						if err = c.engine.Delete(key); err != nil {
							break
						}
					}
				}
			}
		case *queueOperationPurge:
			err = c.engine.Purge()
		}
		c.writeQueue.DoneWriting(err == nil)
		require.NoError(t, err)
	}
}

func jsonMarshal(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// valueUnmarshal produces a value-typed result (*value = res), one of the two
// unmarshaler conventions in the wild.
func valueUnmarshal(b []byte, value *interface{}) error {
	var res testEntry
	if err := json.Unmarshal(b, &res); err != nil {
		return err
	}
	*value = res
	return nil
}

// pointerUnmarshal produces a pointer-typed result (*value = &res), the other
// unmarshaler convention.
func pointerUnmarshal(b []byte, value *interface{}) error {
	var res testEntry
	if err := json.Unmarshal(b, &res); err != nil {
		return err
	}
	*value = &res
	return nil
}

func newZstdEngine(t *testing.T) *compression.Engine {
	t.Helper()
	engine, err := compression.NewEngine(compression.ProviderIDZstd, nil)
	require.NoError(t, err)
	return engine
}

func newMiniredisCache(t *testing.T, unmarshal func([]byte, *interface{}) error) (*RedisCache, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	return NewRedisCache(client, "", jsonMarshal, unmarshal, 0, nil), server
}

// TestReadPathHitsAcrossEngineConventions covers the H7/M9 matrix: Get, Peek
// and GetOrCompute must all hit regardless of whether the engine returns the
// raw stored *T (LRU without compression) or an unmarshaler-produced value
// (redis, LRU with compression) of either convention.
func TestReadPathHitsAcrossEngineConventions(t *testing.T) {
	configs := []struct {
		name      string
		newEngine func(t *testing.T) CacheEngine
	}{
		{"lru-plain", func(t *testing.T) CacheEngine {
			engine, err := NewLRUCache(300, nil, nil, nil)
			require.NoError(t, err)
			return engine
		}},
		{"lru-compressed-value-unmarshaler", func(t *testing.T) CacheEngine {
			engine, err := NewLRUCache(300, jsonMarshal, valueUnmarshal, newZstdEngine(t))
			require.NoError(t, err)
			return engine
		}},
		{"lru-compressed-pointer-unmarshaler", func(t *testing.T) CacheEngine {
			engine, err := NewLRUCache(300, jsonMarshal, pointerUnmarshal, newZstdEngine(t))
			require.NoError(t, err)
			return engine
		}},
		{"redis-value-unmarshaler", func(t *testing.T) CacheEngine {
			engine, _ := newMiniredisCache(t, valueUnmarshal)
			return engine
		}},
		{"redis-pointer-unmarshaler", func(t *testing.T) CacheEngine {
			engine, _ := newMiniredisCache(t, pointerUnmarshal)
			return engine
		}},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			c := newTestCache[testEntry](cfg.newEngine(t))
			want := testEntry{ID: 1, Key: "payload"}
			require.NoError(t, c.Set("k", &want))
			flushWriteQueue(t, c)

			got, err := c.Get("k")
			require.NoError(t, err, "Get must hit after the flush")
			assert.Equal(t, want, *got)

			got, err = c.Peek("k")
			require.NoError(t, err, "Peek must hit after the flush")
			assert.Equal(t, want, *got)

			evaluatorCalled := false
			got, err = c.GetOrCompute("k", func() (*testEntry, error) {
				evaluatorCalled = true
				return &testEntry{ID: 99, Key: "recomputed"}, nil
			})
			require.NoError(t, err)
			assert.False(t, evaluatorCalled, "GetOrCompute must serve the cached value, not recompute")
			assert.Equal(t, want, *got)
		})
	}
}

// TestWrongTypeYieldsErrWrongDataType pins down that widening the asserts
// does not silently accept genuinely foreign values.
func TestWrongTypeYieldsErrWrongDataType(t *testing.T) {
	engine, err := NewLRUCache(300, nil, nil, nil)
	require.NoError(t, err)
	c := newTestCache[testEntry](engine)
	require.NoError(t, engine.Set("k", "definitely not a testEntry"))

	_, err = c.Get("k")
	assert.ErrorIs(t, err, ErrWrongDataType)

	_, err = c.Peek("k")
	assert.ErrorIs(t, err, ErrWrongDataType)
}

// TestRedisCorruptPayloadSelfHeals covers M11 (+ the M2 corollary): a payload
// that fails the unmarshal callback is deleted on read, reads as ErrNotFound,
// and the next compute both returns and persists the fresh value.
func TestRedisCorruptPayloadSelfHeals(t *testing.T) {
	engine, server := newMiniredisCache(t, valueUnmarshal)
	c := newTestCache[testEntry](engine)

	require.NoError(t, server.Set("corrupt", "{not json"))

	_, err := c.Get("corrupt")
	assert.ErrorIs(t, err, ErrNotFound, "corrupt entry must read as a miss")
	assert.False(t, server.Exists("corrupt"), "corrupt entry must be deleted on read")

	want := testEntry{ID: 2, Key: "recomputed"}
	got, err := c.GetOrComputeEx("corrupt", func() (*testEntry, error) {
		return &want, nil
	}, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, want, *got)

	flushWriteQueue(t, c)
	assert.True(t, server.Exists("corrupt"), "recomputed value must be persisted to the engine")
	got, err = c.Get("corrupt")
	require.NoError(t, err)
	assert.Equal(t, want, *got)
}

// TestLRUCompressedCorruptPayloadSelfHeals is the LRU-side M11 case: the
// decompress path must delete entries whose unmarshal fails instead of
// serving nil forever.
func TestLRUCompressedCorruptPayloadSelfHeals(t *testing.T) {
	corruptMarshal := func(value interface{}) ([]byte, error) {
		return []byte("{not json"), nil
	}
	engine, err := NewLRUCache(300, corruptMarshal, valueUnmarshal, newZstdEngine(t))
	require.NoError(t, err)
	c := newTestCache[testEntry](engine)

	seed := testEntry{ID: 1, Key: "seed"}
	require.NoError(t, c.Set("corrupt", &seed))
	flushWriteQueue(t, c)

	_, err = c.Get("corrupt")
	assert.ErrorIs(t, err, ErrNotFound, "corrupt entry must read as a miss")
	assert.False(t, engine.lru.Contains("corrupt"), "corrupt entry must be deleted on read")
}

// TestValidatorRejectionWritesBackRecompute is the M2 repro: when the
// validator rejects a cached entry, the successful recompute must replace it
// in the engine so unconstrained readers see the fresh value.
func TestValidatorRejectionWritesBackRecompute(t *testing.T) {
	engine, err := NewLRUCache(300, nil, nil, nil)
	require.NoError(t, err)
	c := newTestCache[testEntry](engine)

	stale := testEntry{ID: 1, Key: "stale"}
	require.NoError(t, c.Set("k", &stale))
	flushWriteQueue(t, c)

	fresh := testEntry{ID: 2, Key: "fresh"}
	got, err := c.GetOrComputeEx("k",
		func() (*testEntry, error) { return &fresh, nil },
		func(v *testEntry) bool { return v.ID != stale.ID },
		nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, fresh, *got)

	flushWriteQueue(t, c)
	got, err = c.Get("k")
	require.NoError(t, err)
	assert.Equal(t, fresh, *got, "recomputed value must replace the validator-rejected entry")
}

// TestWriteApproverStillBlocksWriteBack guards the always-write-back change:
// writeApprover=false must keep the recompute out of the cache.
func TestWriteApproverStillBlocksWriteBack(t *testing.T) {
	engine, err := NewLRUCache(300, nil, nil, nil)
	require.NoError(t, err)
	c := newTestCache[testEntry](engine)

	stale := testEntry{ID: 1, Key: "stale"}
	require.NoError(t, c.Set("k", &stale))
	flushWriteQueue(t, c)

	fresh := testEntry{ID: 2, Key: "fresh"}
	got, err := c.GetOrComputeEx("k",
		func() (*testEntry, error) { return &fresh, nil },
		func(v *testEntry) bool { return false },
		nil, nil,
		func(v *testEntry) bool { return false })
	require.NoError(t, err)
	assert.Equal(t, fresh, *got, "the computed value is still returned to the caller")

	flushWriteQueue(t, c)
	got, err = c.Get("k")
	require.NoError(t, err)
	assert.Equal(t, stale, *got, "writeApprover=false must block the write-back")
}
