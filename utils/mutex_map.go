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

func (m *MutexMap) lock(name string, readonly bool) *sync.RWMutex {
	m.mutex.Lock()
	nameLock, exists := m.locks[name]
	if !exists {
		nameLock = &lockEntry{
			callers: 1,
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

	return &nameLock.mutex
}

func (m *MutexMap) RLock(name string) *sync.RWMutex {
	return m.lock(name, true)
}

// Lock locks a mutex with the given name.
func (m *MutexMap) Lock(name string) *sync.RWMutex {
	return m.lock(name, false)
}

func (m *MutexMap) unlock(name string, readonly bool, mutex *sync.RWMutex) {
	m.mutex.Lock()
	nameLock, exists := m.locks[name]
	if !exists || &nameLock.mutex != mutex {
		if mutex != nil {
			if readonly {
				mutex.RUnlock()
			} else {
				mutex.Unlock()
			}
		}
		m.mutex.Unlock()
		return
	}

	if readonly {
		nameLock.runlock()
	} else {
		nameLock.unlock()
	}
	nameLock.dec()
	if nameLock.count() == 0 {
		delete(m.locks, name)
	}

	m.mutex.Unlock()

}

// Unlock unlocks a mutex with the given name.
func (m *MutexMap) Unlock(name string, mutex *sync.RWMutex) {
	m.unlock(name, false, mutex)
}

func (m *MutexMap) RUnlock(name string, mutex *sync.RWMutex) {
	m.unlock(name, true, mutex)
}

// lockCtr represents a lock for a given name.
type lockEntry struct {
	mutex   sync.RWMutex
	callers int32
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
	atomic.AddInt32(&l.callers, 1)
}

// dec decrements the number of waiters waiting on the lock
func (l *lockEntry) dec() {
	atomic.AddInt32(&l.callers, -1)
}

// count gets the current number of waiters
func (l *lockEntry) count() int32 {
	return atomic.LoadInt32(&l.callers)
}
