package utils

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMutexMap(t *testing.T) {

	mm := NewMutexMap()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t1 := mm.RLock("test1")
			t := mm.RLock("test")
			time.Sleep(100 * time.Millisecond)
			mm.RUnlock("test", t)
			mm.RUnlock("test1", t1)
		}()
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t2 := mm.Lock("test2")
			t := mm.Lock("test")
			time.Sleep(100 * time.Millisecond)
			mm.Unlock("test", t)
			mm.Unlock("test2", t2)
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
	t1 := mm.Lock("test")
	lock, found := mm.locks["test"]
	assert.True(t, found)
	assert.Equal(t, int32(1), atomic.LoadInt32(&lock.callers))
	// 2nd lock
	var t2 *sync.RWMutex
	go func() { t2 = mm.Lock("test") }()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(2), atomic.LoadInt32(&lock.callers))
	// 3rd lock
	var t3 *sync.RWMutex
	go func() { t3 = mm.Lock("test") }()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(3), atomic.LoadInt32(&lock.callers))
	// 1st unlock
	mm.Unlock("test", t1)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(2), atomic.LoadInt32(&lock.callers))
	// 2nd unlock
	mm.Unlock("test", t2)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&lock.callers))
	// 3rd unlock
	mm.Unlock("test", t3)
	_, found = mm.locks["test"]
	assert.False(t, found)
}

func TestMutexMapComplexRLock(t *testing.T) {

	mm := NewMutexMap()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			l1 := mm.RLock("lock1")
			l2 := mm.RLock("lock2")
			l3 := mm.RLock("lock1")
			time.Sleep(100 * time.Millisecond)
			mm.RUnlock("lock1", l1)
			mm.RUnlock("lock2", l2)
			mm.RUnlock("lock1", l3)
		}()
		go func() {
			defer wg.Done()

			l1_2 := mm.RLock("lock1")
			time.Sleep(100 * time.Millisecond)
			l2_2 := mm.RLock("lock2")
			l3_2 := mm.RLock("lock1")
			time.Sleep(200 * time.Millisecond)
			mm.RUnlock("lock1", l1_2)
			mm.RUnlock("lock2", l2_2)
			time.Sleep(200 * time.Millisecond)
			mm.RUnlock("lock1", l3_2)
		}()
		go func() {
			defer wg.Done()
			l1_3 := mm.RLock("lock1")
			l2_3 := mm.RLock("lock2")
			l3_3 := mm.RLock("lock1")
			time.Sleep(200 * time.Millisecond)
			mm.RUnlock("lock1", l1_3)
			mm.RUnlock("lock2", l2_3)
			time.Sleep(200 * time.Millisecond)
			mm.RUnlock("lock1", l3_3)
		}()
	}
	wg.Wait()

}

func TestMutexMapComplexLock(t *testing.T) {

	mm := NewMutexMap()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(3)

		go func() {
			defer wg.Done()
			l1 := mm.Lock("lock1")
			l2 := mm.Lock("lock2")
			l3 := mm.Lock("lock3")
			time.Sleep(100 * time.Millisecond)
			mm.Unlock("lock1", l1)
			mm.Unlock("lock2", l2)
			mm.Unlock("lock3", l3)
		}()
		go func() {
			defer wg.Done()

			l1_2 := mm.Lock("lock1")
			time.Sleep(100 * time.Millisecond)
			l2_2 := mm.Lock("lock2")
			l3_2 := mm.Lock("lock3")
			time.Sleep(200 * time.Millisecond)
			mm.Unlock("lock1", l1_2)
			mm.Unlock("lock2", l2_2)
			time.Sleep(200 * time.Millisecond)
			mm.Unlock("lock3", l3_2)
		}()
		go func() {
			defer wg.Done()
			l1_3 := mm.Lock("lock1")
			l2_3 := mm.Lock("lock2")
			l3_3 := mm.Lock("lock3")
			time.Sleep(200 * time.Millisecond)
			mm.Unlock("lock1", l1_3)
			mm.Unlock("lock2", l2_3)
			time.Sleep(200 * time.Millisecond)
			mm.Unlock("lock3", l3_3)
		}()
	}
	wg.Wait()

}
