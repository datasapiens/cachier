package cachier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A miss in both layers must surface ErrNotFound instead of dereferencing
// the nil compute result (L6).
func TestCacheWithSubcacheGetMissReturnsError(t *testing.T) {
	cs := &CacheWithSubcache[testEntry]{
		Cache:    newTestCache[testEntry](newFakeEngine()),
		Subcache: newTestCache[testEntry](newFakeEngine()),
	}

	var value interface{}
	var err error
	require.NotPanics(t, func() {
		value, err = cs.Get("missing")
	})
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Equal(t, testEntry{}, value)
}

// A hit in the primary cache is served and promoted into the L1 subcache.
func TestCacheWithSubcacheGetHit(t *testing.T) {
	cs := &CacheWithSubcache[testEntry]{
		Cache:    newTestCache[testEntry](newFakeEngine()),
		Subcache: newTestCache[testEntry](newFakeEngine()),
	}
	require.NoError(t, cs.Cache.Set("hit", &testEntry{ID: 7, Key: "hit"}))

	value, err := cs.Get("hit")
	require.NoError(t, err)
	assert.Equal(t, testEntry{ID: 7, Key: "hit"}, value)

	promoted, err := cs.Subcache.Get("hit")
	require.NoError(t, err)
	assert.Equal(t, testEntry{ID: 7, Key: "hit"}, *promoted)
}
