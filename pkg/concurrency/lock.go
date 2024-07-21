package concurrency

import (
	"errors"
	"sync"
)

// Indicates whether a lock is a reader or a writer lock.
type LockType int

const (
	R_LOCK LockType = 0
	W_LOCK LockType = 1
)

// A resource.
type Resource struct {
	tableName   string
	resourceKey int64
}

// Get resource table name.
func (r *Resource) GetTableName() string {
	return r.tableName
}

// Get resource key.
func (r *Resource) GetResourceKey() int64 {
	return r.resourceKey
}

// Lock manager handles transaction-level locks over database resources.
type LockManager struct {
	lmMtx sync.Mutex
	locks map[Resource]*sync.RWMutex
}

// Construct a new lock manager.
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[Resource]*sync.RWMutex),
	}
}

// Lock a resource.
func (lm *LockManager) Lock(r Resource, lType LockType) error {
	// Safely acquire the lock itself, initializing it if needed.
	lm.lmMtx.Lock()
	lock, found := lm.locks[r]
	if !found {
		lm.locks[r] = &sync.RWMutex{}
		lock = lm.locks[r]
	}
	lm.lmMtx.Unlock()
	// Lock accordingly.
	switch lType {
	case R_LOCK:
		lock.RLock()
	case W_LOCK:
		lock.Lock()
	}
	return nil
}

// Unlock a resource.
func (lm *LockManager) Unlock(r Resource, lType LockType) error {
	// Safely acquire the lock itself.
	lm.lmMtx.Lock()
	lock, found := lm.locks[r]
	if !found {
		return errors.New("tried to unlock nonexistent resource")
	}
	lm.lmMtx.Unlock()
	// Unlock accordingly.
	switch lType {
	case R_LOCK:
		lock.RUnlock()
	case W_LOCK:
		lock.Unlock()
	}
	return nil
}
