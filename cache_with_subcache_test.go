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
