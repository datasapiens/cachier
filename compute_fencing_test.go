package cachier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLRUTestCache builds a Cache over a plain LRU engine without the
// background write loop (house pattern: newTestCache + flushWriteQueue).
func newLRUTestCache(t *testing.T) *Cache[testEntry] {
	t.Helper()
	engine, err := NewLRUCache(300, nil, nil, nil)
	require.NoError(t, err)
	return newTestCache[testEntry](engine)
}

// waitForAsyncWriteBack blocks until GetOrCompute's spawned write-back
// goroutine for key has finished: that goroutine holds key's compute lock
// until after its write attempt, so acquiring the lock is a deterministic
// barrier — no sleeps needed.
func waitForAsyncWriteBack(c *Cache[testEntry], key string) {
	m := c.computeLocks.Lock(key)
	c.computeLocks.Unlock(key, m)
}

// The H4 scenario: an invalidation fires while a compute for a matching key
// is still running. The compute's result must be returned to the caller but
// must never be written back — otherwise the pre-event value is resurrected
// and served until the next (possibly never-recurring) invalidation event.
// Firing the invalidation from inside the evaluator is deterministic: the
// compute is by construction in flight at that moment.

func TestGetOrComputeEx_DeletePredicateMidCompute_SkipsWriteBack(t *testing.T) {
	c := newLRUTestCache(t)

	want := testEntry{ID: 1, Key: "computed-against-stale-data"}
	got, err := c.GetOrComputeEx("k", func() (*testEntry, error) {
		require.NoError(t, c.DeletePredicate(func(key string) bool { return key == "k" }))
		return &want, nil
	}, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, want, *got, "the computed value is still returned to the caller")

	flushWriteQueue(t, c)
	_, err = c.Get("k")
	assert.ErrorIs(t, err, ErrNotFound, "a write fenced by a mid-compute invalidation must never land")
}

func TestGetOrComputeEx_PurgeMidCompute_SkipsWriteBack(t *testing.T) {
	c := newLRUTestCache(t)

	want := testEntry{ID: 1, Key: "computed-against-stale-data"}
	got, err := c.GetOrComputeEx("k", func() (*testEntry, error) {
		require.NoError(t, c.Purge())
		return &want, nil
	}, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, want, *got)

	flushWriteQueue(t, c)
	_, err = c.Get("k")
	assert.ErrorIs(t, err, ErrNotFound, "a write fenced by a mid-compute purge must never land")
}

func TestGetOrComputeEx_NonMatchingPredicateMidCompute_KeepsWrite(t *testing.T) {
	c := newLRUTestCache(t)

	want := testEntry{ID: 1, Key: "fresh"}
	got, err := c.GetOrComputeEx("k", func() (*testEntry, error) {
		require.NoError(t, c.DeletePredicate(func(key string) bool { return key == "other" }))
		return &want, nil
	}, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, want, *got)

	flushWriteQueue(t, c)
	got, err = c.Get("k")
	require.NoError(t, err, "a non-matching invalidation must not suppress the write")
	assert.Equal(t, want, *got)
}

func TestGetOrCompute_DeletePredicateMidCompute_SkipsAsyncWriteBack(t *testing.T) {
	c := newLRUTestCache(t)

	want := testEntry{ID: 1, Key: "computed-against-stale-data"}
	got, err := c.GetOrCompute("k", func() (*testEntry, error) {
		require.NoError(t, c.DeletePredicate(func(key string) bool { return key == "k" }))
		return &want, nil
	})
	require.NoError(t, err)
	assert.Equal(t, want, *got)

	waitForAsyncWriteBack(c, "k")
	flushWriteQueue(t, c)
	_, err = c.Get("k")
	assert.ErrorIs(t, err, ErrNotFound, "GetOrCompute's async write-back must honor a mid-compute invalidation")
}

func TestGetOrCompute_PurgeMidCompute_SkipsAsyncWriteBack(t *testing.T) {
	c := newLRUTestCache(t)

	want := testEntry{ID: 1, Key: "computed-against-stale-data"}
	got, err := c.GetOrCompute("k", func() (*testEntry, error) {
		require.NoError(t, c.Purge())
		return &want, nil
	})
	require.NoError(t, err)
	assert.Equal(t, want, *got)

	waitForAsyncWriteBack(c, "k")
	flushWriteQueue(t, c)
	_, err = c.Get("k")
	assert.ErrorIs(t, err, ErrNotFound, "GetOrCompute's async write-back must honor a mid-compute purge")
}

func TestGetOrCompute_NonMatchingPredicateMidCompute_KeepsAsyncWrite(t *testing.T) {
	c := newLRUTestCache(t)

	want := testEntry{ID: 1, Key: "fresh"}
	got, err := c.GetOrCompute("k", func() (*testEntry, error) {
		require.NoError(t, c.DeletePredicate(func(key string) bool { return key == "other" }))
		return &want, nil
	})
	require.NoError(t, err)
	assert.Equal(t, want, *got)

	waitForAsyncWriteBack(c, "k")
	flushWriteQueue(t, c)
	got, err = c.Get("k")
	require.NoError(t, err, "a non-matching invalidation must not suppress the async write")
	assert.Equal(t, want, *got)
}
