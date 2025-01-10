package utils

import (
	"sync"
	"sync/atomic"
)

// MutexMap provides a locking mechanism based on provided name
type MutexMap struct {
	mutex sync.Mutex
	locks map[string]*lockEntry
}

// NewMutexMap creates a new MutexMap
func NewMutexMap() *MutexMap {
	return &MutexMap{
		locks: make(map[string]*lockEntry),
	}
}

func (m *MutexMap) lock(name string, readonly bool) {
	m.mutex.Lock()
	nameLock, exists := m.locks[name]
	if !exists {
		nameLock = &lockEntry{
			waiters: 1,
		}
		m.locks[name] = nameLock
	} else {
		nameLock.inc()
	}

	m.mutex.Unlock()
	if readonly {
		nameLock.rlock()
	} else {
		nameLock.lock()
	}
	// I used the resource, I am not waiting any more to access the lock
	nameLock.dec()
}

func (m *MutexMap) RLock(name string) {
	m.lock(name, true)
}

// Lock locks a mutex with the given name.
func (m *MutexMap) Lock(name string) {
	m.lock(name, false)
}

func (m *MutexMap) unlock(name string, readonly bool) {
	m.mutex.Lock()
	nameLock, exists := m.locks[name]
	if !exists {
		m.mutex.Unlock()
		return
	}

	if nameLock.count() == 0 {
		delete(m.locks, name)
	}
	if readonly {
		nameLock.runlock()
	} else {
		nameLock.unlock()
	}

	m.mutex.Unlock()

}

// Unlock unlocks a mutex with the given name.
func (m *MutexMap) Unlock(name string) {
	m.unlock(name, false)
}

func (m *MutexMap) RUnlock(name string) {
	m.unlock(name, true)
}

// lockCtr represents a lock for a given name.
type lockEntry struct {
	mutex   sync.RWMutex
	waiters int32
}

func (l *lockEntry) lock() {
	l.mutex.Lock()
}

func (l *lockEntry) rlock() {
	l.mutex.RLock()
}

func (l *lockEntry) unlock() {
	l.mutex.Unlock()
}

func (l *lockEntry) runlock() {
	l.mutex.RUnlock()
}

// inc increments the number of waiters waiting for the lock
func (l *lockEntry) inc() {
	atomic.AddInt32(&l.waiters, 1)
}

// dec decrements the number of waiters waiting on the lock
func (l *lockEntry) dec() {
	atomic.AddInt32(&l.waiters, -1)
}

// count gets the current number of waiters
func (l *lockEntry) count() int32 {
	return atomic.LoadInt32(&l.waiters)
}
