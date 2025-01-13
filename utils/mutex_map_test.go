package utils

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMytexMap(t *testing.T) {

	mm := NewMutexMap()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mm.RLock("test1")
			mm.RLock("test")
			time.Sleep(100 * time.Millisecond)
			mm.RUnlock("test")
			mm.RUnlock("test1")
		}()
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mm.Lock("test2")
			mm.Lock("test")
			time.Sleep(100 * time.Millisecond)
			mm.Unlock("test")
			mm.Unlock("test2")
		}()
	}

	wg.Wait()

	// all locks should be released, no locks should be found
	_, found := mm.locks["test"]
	assert.False(t, found)
	_, found = mm.locks["test1"]
	assert.False(t, found)
	_, found = mm.locks["test2"]
	assert.False(t, found)

	// 1st lock
	mm.Lock("test")
	lock, found := mm.locks["test"]
	assert.True(t, found)
	assert.Equal(t, int32(0), atomic.LoadInt32(&lock.waiters))
	// 2nd lock
	go mm.Lock("test")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&lock.waiters))
	// 3rd lock
	go mm.Lock("test")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(2), atomic.LoadInt32(&lock.waiters))
	// 1st unlock
	mm.Unlock("test")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&lock.waiters))
	// 2nd unlock
	mm.Unlock("test")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&lock.waiters))
	// 3rd unlock
	mm.Unlock("test")
	_, found = mm.locks["test"]
	assert.False(t, found)
}
