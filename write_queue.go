package cachier

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"

	"github.com/gammazero/deque"
)

const (
	QueueOperationSet = iota
	QueueOperationDelete
	QueueOperationDeletePredicate
	QueueOperationPurge
)

type queueOperation interface {
	Includes(op queueOperation) bool
	IncludesKey(key string) bool
	String() string
}

type queueOperationWithKey interface {
	queueOperation
	GetKey() string
}

type queueOperationSet[T any] struct {
	Key   string
	Value *T
}

func (o *queueOperationSet[T]) GetType() int {
	return QueueOperationSet
}

func (o *queueOperationSet[T]) GetKey() string {
	return o.Key
}

func (o *queueOperationSet[T]) String() string {
	return fmt.Sprintf("Set(%s)", o.Key)
}

func (o *queueOperationSet[T]) Includes(op queueOperation) bool {
	if op, ok := op.(queueOperationWithKey); ok {
		return o.Key == op.GetKey()
	}

	return false
}

func (o *queueOperationSet[T]) IncludesKey(key string) bool {
	return o.Key == key
}

type queueOperationDelete struct {
	Key string
}

func (o *queueOperationDelete) GetType() int {
	return QueueOperationDelete
}

func (o *queueOperationDelete) GetKey() string {
	return o.Key
}

func (o *queueOperationDelete) String() string {
	return fmt.Sprintf("Delete(%s)", o.Key)
}

func (o *queueOperationDelete) Includes(op queueOperation) bool {
	if op, ok := op.(queueOperationWithKey); ok {
		return o.Key == op.GetKey()
	}

	return false
}

func (o *queueOperationDelete) IncludesKey(key string) bool {
	return o.Key == key
}

type queueOperationDeletePredicate struct {
	Predicate Predicate
}

func (o *queueOperationDeletePredicate) GetType() int {
	return QueueOperationDeletePredicate
}

func (o *queueOperationDeletePredicate) String() string {
	funcName := runtime.FuncForPC(reflect.ValueOf(o.Predicate).Pointer()).Name()
	return fmt.Sprintf("DeletePredicate(%s)", funcName)
}

func (o *queueOperationDeletePredicate) Includes(op queueOperation) bool {
	if op, ok := op.(queueOperationWithKey); ok {
		return o.Predicate(op.GetKey())
	}

	return false
}

func (o *queueOperationDeletePredicate) IncludesKey(key string) bool {
	return o.Predicate(key)
}

type queueOperationPurge struct{}

func (o *queueOperationPurge) GetType() int {
	return QueueOperationPurge
}

func (o *queueOperationPurge) String() string {
	return "Purge()"
}

func (o *queueOperationPurge) Includes(op queueOperation) bool {
	return true
}

func (o *queueOperationPurge) IncludesKey(key string) bool {
	return true
}

// computeToken fences one in-flight GetOrCompute(Ex) call against
// invalidations that run without that key's compute lock (DeletePredicate,
// DeleteWithPrefix, DeleteRegExp, Purge). The invalidated flag is read and
// written ONLY under the owning writeQueue's mutex: checking it and
// enqueueing the write happen in one critical section (TrySet), as do
// marking it and discarding queued writes (Discard/DiscardPredicate), so a
// stale write can neither slip in after a discard nor dodge the mark.
type computeToken struct {
	invalidated bool
}

type writeQueue[T any] struct {
	sync.Mutex
	Queue            deque.Deque[queueOperation] // Queue to hold write cache operations
	Values           map[string]*T               // Map to hold currently valid values that were not yet written
	CurrentlyWriting queueOperation              // Queue write operation that is currently being processed
	tokens           map[string]*computeToken    // Active compute tokens by key (H4 fencing)
}

// newWriteQueue creates a new CircularQueue with the specified size
func newWriteQueue[T any]() *writeQueue[T] {
	return &writeQueue[T]{
		Values:           make(map[string]*T),
		CurrentlyWriting: nil,
		tokens:           make(map[string]*computeToken),
	}
}

// removeOverridden removes all operations from the queue that are overridden by the provided operation
func (q *writeQueue[T]) removeOverridden(op queueOperation) {
	i := 0
	for i < q.Queue.Len() {
		iOp := q.Queue.At(i)
		if op.Includes(iOp) {
			q.Queue.Remove(i)
		} else {
			i++
		}
	}
}

// Get retrieves the value for a given key from the queue.
//
//	Returns nil, true if the key is invalid. Returns nil, false if the key was not found.
func (q *writeQueue[T]) Get(key string) (*T, bool) {
	q.Lock()
	defer q.Unlock()

	if value, ok := q.Values[key]; ok {
		return value, true
	}

	for it := range q.Queue.Iter() {
		if it.IncludesKey(key) {
			return nil, true
		}
	}

	return nil, false // Key not found
}

// Set adds a new key-value pair to the queue
func (q *writeQueue[T]) Set(key string, value *T) {
	q.TrySet(key, value, nil)
}

// TrySet stores a key-value pair like Set unless the supplied token was
// invalidated by a concurrent Discard/DiscardPredicate. A nil token is
// never invalidated. Reports whether the value was stored.
func (q *writeQueue[T]) TrySet(key string, value *T, token *computeToken) bool {
	q.Lock()
	defer q.Unlock()

	if token != nil && token.invalidated {
		return false
	}

	op := &queueOperationSet[T]{Key: key, Value: value}
	q.removeOverridden(op)
	q.Queue.PushBack(op)

	q.Values[key] = value
	return true
}

// RegisterToken creates the active compute token for key. Callers must hold
// key's compute lock from before RegisterToken until after DeregisterToken
// — that discipline is what guarantees at most one live token per key, so a
// new registration never overwrites a token another compute still holds.
func (q *writeQueue[T]) RegisterToken(key string) *computeToken {
	q.Lock()
	defer q.Unlock()

	token := &computeToken{}
	q.tokens[key] = token
	return token
}

// DeregisterToken removes key's token once its compute has finished. Must
// run before the caller releases key's compute lock (see RegisterToken);
// the identity check additionally keeps a stale caller from ever removing
// a newer compute's token should that discipline ever be broken.
func (q *writeQueue[T]) DeregisterToken(key string, token *computeToken) {
	q.Lock()
	defer q.Unlock()

	if q.tokens[key] == token {
		delete(q.tokens, key)
	}
}

// Discard removes any queued Set for key without queueing a delete op (the
// caller deletes from the engine synchronously) and marks key's in-flight
// compute token, if any, so its pending write-back is skipped. Callers run
// under engineMu, so no write cycle is in flight (CurrentlyWriting is nil)
// while the queue is swept.
func (q *writeQueue[T]) Discard(key string) {
	q.discardMatching(func(k string) bool { return k == key })
}

// DiscardPredicate removes every queued Set whose key matches pred and
// marks every matching in-flight compute token. Same engineMu caveat as
// Discard.
func (q *writeQueue[T]) DiscardPredicate(pred Predicate) {
	q.discardMatching(pred)
}

func (q *writeQueue[T]) discardMatching(pred Predicate) {
	q.Lock()
	defer q.Unlock()

	i := 0
	for i < q.Queue.Len() {
		if op, ok := q.Queue.At(i).(*queueOperationSet[T]); ok && pred(op.Key) {
			q.Queue.Remove(i)
		} else {
			i++
		}
	}
	for key := range q.Values {
		if pred(key) {
			delete(q.Values, key)
		}
	}
	for key, token := range q.tokens {
		if pred(key) {
			token.invalidated = true
		}
	}
}

// Delete queues a key for deletion
func (q *writeQueue[T]) Delete(key string) {
	q.Lock()
	defer q.Unlock()

	op := &queueOperationDelete{Key: key}
	q.removeOverridden(op)
	q.Queue.PushBack(op)

	delete(q.Values, key)
}

// DeletePredicate queues a deletion of all keys matching the supplied predicate
func (q *writeQueue[T]) DeletePredicate(pred Predicate) {
	q.Lock()
	defer q.Unlock()

	op := &queueOperationDeletePredicate{Predicate: pred}
	q.removeOverridden(op)
	q.Queue.PushBack(op)

	for key := range q.Values {
		if pred(key) {
			delete(q.Values, key)
		}
	}
}

// Count returns the number of keys in the queue
func (q *writeQueue[T]) Count() int {
	q.Lock()
	defer q.Unlock()

	return len(q.Values)
}

// CountPredicate counts the number of keys in the queue that satisfy the given predicate
func (q *writeQueue[T]) CountPredicate(pred Predicate) int {
	q.Lock()
	defer q.Unlock()

	count := 0
	for key := range q.Values {
		if pred(key) {
			count++ // Count valid keys that satisfy the predicate
		}
	}

	return count // Return the total count
}

// Purge removes all records from the queue
func (q *writeQueue[T]) Purge() {
	q.Lock()
	defer q.Unlock()

	op := &queueOperationPurge{}
	q.Queue.Clear()
	q.Queue.PushBack(op)

	q.Values = make(map[string]*T) // Reset the values map
}

// Keys returns all the keys in the queue
func (q *writeQueue[T]) Keys() []string {
	q.Lock()
	defer q.Unlock()

	keys := make([]string, 0, len(q.Values)+1)
	for key := range q.Values {
		keys = append(keys, key) // Add valid keys
	}

	return keys // Return the list of all keys
}

// StartWriting removes the oldest key-value pair from the queue
func (q *writeQueue[T]) StartWriting() (queueOperation, bool) {
	q.Lock()
	defer q.Unlock()

	if q.CurrentlyWriting != nil {
		panic("write operation already in progress")
	}

	if q.Queue.Len() == 0 {
		return nil, false
	}

	op := q.Queue.At(0)
	q.CurrentlyWriting = op

	return op, true
}

// DoneWriting marks the current writing key-value pair as done
func (q *writeQueue[T]) DoneWriting(ok bool) {
	q.Lock()
	defer q.Unlock()

	// The queue could have been changed since the StartWriting call,
	// so we need to check if the first operation is the same as the current writing operation
	if ok && q.Queue.Len() > 0 && q.Queue.At(0) == q.CurrentlyWriting {
		// Remove the completed operation from the front of the queue
		q.Queue.PopFront()

		// If it's a set operation, and the value was not overridden
		if op, ok := q.CurrentlyWriting.(*queueOperationSet[T]); ok {
			if value, ok := q.Values[op.Key]; ok && value == op.Value {
				delete(q.Values, op.Key)
			}
		}
	}

	q.CurrentlyWriting = nil // Reset the current writing operation
}

// GetStats returns the current size of the queue
func (q *writeQueue[T]) GetStats() (int, int) {
	q.Lock()
	defer q.Unlock()

	return q.Queue.Len(), len(q.Values)
}
