package cachier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countKey returns how many times key occurs in keys.
func countKey(keys []string, key string) int {
	count := 0
	for _, k := range keys {
		if k == key {
			count++
		}
	}
	return count
}

// A key that was flushed to the engine and then updated sits in both the
// write queue and the engine; Keys must report it once (L5).
func TestKeysDedupesQueueAndEngine(t *testing.T) {
	c := newTestCache[testEntry](newFakeEngine())

	require.NoError(t, c.Set("updated", &testEntry{ID: 1}))
	flushWriteQueue(t, c)
	require.NoError(t, c.Set("updated", &testEntry{ID: 2}))
	require.NoError(t, c.Set("pending", &testEntry{ID: 3}))

	keys, err := c.Keys()
	require.NoError(t, err)
	assert.Equal(t, 1, countKey(keys, "updated"))
	assert.Equal(t, 1, countKey(keys, "pending"))
	assert.Len(t, keys, 2)
}

// CountPredicate used to count the write queue separately and then iterate
// Keys(), which already includes the queue — every unflushed key counted
// twice (L5).
func TestCountPredicateCountsUnflushedKeyOnce(t *testing.T) {
	c := newTestCache[testEntry](newFakeEngine())

	require.NoError(t, c.Set("updated", &testEntry{ID: 1}))
	flushWriteQueue(t, c)
	require.NoError(t, c.Set("updated", &testEntry{ID: 2}))

	count, err := c.CountPredicate(func(string) bool { return true })
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// A queued delete masks the key for Get/Peek; Keys and CountPredicate must
// exclude it the same way even though the engine still holds it until the
// write loop flushes the tombstone (L5).
func TestKeysExcludesKeyPendingDeletion(t *testing.T) {
	c := newTestCache[testEntry](newFakeEngine())

	require.NoError(t, c.Set("doomed", &testEntry{ID: 1}))
	require.NoError(t, c.Set("kept", &testEntry{ID: 2}))
	flushWriteQueue(t, c)
	require.NoError(t, c.Delete("doomed"))

	keys, err := c.Keys()
	require.NoError(t, err)
	assert.Equal(t, []string{"kept"}, keys)

	count, err := c.CountPredicate(func(string) bool { return true })
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestKeysExcludesKeysPendingPredicateDeletion(t *testing.T) {
	c := newTestCache[testEntry](newFakeEngine())

	require.NoError(t, c.Set("tag:a", &testEntry{ID: 1}))
	require.NoError(t, c.Set("tag:b", &testEntry{ID: 2}))
	require.NoError(t, c.Set("other", &testEntry{ID: 3}))
	flushWriteQueue(t, c)
	require.NoError(t, c.DeleteWithPrefix("tag:"))

	keys, err := c.Keys()
	require.NoError(t, err)
	assert.Equal(t, []string{"other"}, keys)
}

func TestKeysExcludesEverythingAfterPurge(t *testing.T) {
	c := newTestCache[testEntry](newFakeEngine())

	require.NoError(t, c.Set("k1", &testEntry{ID: 1}))
	require.NoError(t, c.Set("k2", &testEntry{ID: 2}))
	flushWriteQueue(t, c)
	require.NoError(t, c.Purge())

	keys, err := c.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)

	count, err := c.CountPredicate(func(string) bool { return true })
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
