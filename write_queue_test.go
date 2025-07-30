package cachier

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewWriteQueue(t *testing.T) {
	q := newWriteQueue[string]()

	assert.NotNil(t, q)
	assert.NotNil(t, q.Values)
	assert.Equal(t, 0, q.Queue.Len())
	assert.Equal(t, 0, len(q.Values))
	assert.Nil(t, q.CurrentlyWriting)
}

func TestWriteQueue_Set_Get(t *testing.T) {
	q := newWriteQueue[string]()

	// Test setting and getting a value
	value := "test_value"
	q.Set("key1", &value)

	// Verify the value is in the Values map
	assert.Equal(t, 1, len(q.Values))
	assert.Equal(t, &value, q.Values["key1"])

	// Verify Get returns the correct value
	result, found := q.Get("key1")
	assert.True(t, found)
	assert.Equal(t, &value, result)

	// Test getting non-existent key
	result, found = q.Get("nonexistent")
	assert.False(t, found)
	assert.Nil(t, result)
}

func TestWriteQueue_Set_Overrides(t *testing.T) {
	q := newWriteQueue[string]()

	// Set initial value
	value1 := "value1"
	q.Set("key1", &value1)

	// Set new value for same key
	value2 := "value2"
	q.Set("key1", &value2)

	// Should have only one operation in queue (the override removed the first)
	assert.Equal(t, 1, q.Queue.Len())

	// Values map should have the latest value
	assert.Equal(t, &value2, q.Values["key1"])

	// Get should return the latest value
	result, found := q.Get("key1")
	assert.True(t, found)
	assert.Equal(t, &value2, result)
}

func TestWriteQueue_Delete(t *testing.T) {
	q := newWriteQueue[string]()

	// Set a value first
	value := "test_value"
	q.Set("key1", &value)

	// Delete the key
	q.Delete("key1")

	// Should have 1 operation in queue (the delete operation)
	assert.Equal(t, 1, q.Queue.Len())

	// Values map should not contain the key
	assert.NotContains(t, q.Values, "key1")

	// Get should return nil, true (indicating the key is marked for deletion)
	result, found := q.Get("key1")
	assert.True(t, found)
	assert.Nil(t, result)
}

func TestWriteQueue_Delete_OverridesSet(t *testing.T) {
	q := newWriteQueue[string]()

	// Set a value
	value := "test_value"
	q.Set("key1", &value)

	// Delete should override the set operation
	q.Delete("key1")

	// Should have only 1 operation (delete override set)
	assert.Equal(t, 1, q.Queue.Len())

	// Values should not contain the key
	assert.NotContains(t, q.Values, "key1")
}

func TestWriteQueue_DeletePredicate(t *testing.T) {
	q := newWriteQueue[string]()

	// Set multiple values
	value1 := "value1"
	value2 := "value2"
	value3 := "value3"
	q.Set("test_key1", &value1)
	q.Set("test_key2", &value2)
	q.Set("other_key", &value3)

	// Delete keys starting with "test_"
	predicate := func(key string) bool {
		return strings.HasPrefix(key, "test_")
	}
	q.DeletePredicate(predicate)

	// Should have 2 operations in queue (remaining Set for "other_key" + DeletePredicate)
	assert.Equal(t, 2, q.Queue.Len())

	// Values should only contain "other_key"
	assert.Equal(t, 1, len(q.Values))
	assert.Contains(t, q.Values, "other_key")
	assert.NotContains(t, q.Values, "test_key1")
	assert.NotContains(t, q.Values, "test_key2")

	// Test Get behavior
	result, found := q.Get("test_key1")
	assert.True(t, found)
	assert.Nil(t, result)

	result, found = q.Get("other_key")
	assert.True(t, found)
	assert.Equal(t, &value3, result)
}

func TestWriteQueue_Purge(t *testing.T) {
	q := newWriteQueue[string]()

	// Set multiple values
	value1 := "value1"
	value2 := "value2"
	q.Set("key1", &value1)
	q.Set("key2", &value2)

	// Purge
	q.Purge()

	// Should have 1 operation in queue (Purge)
	assert.Equal(t, 1, q.Queue.Len())

	// Values should be empty
	assert.Equal(t, 0, len(q.Values))

	// Any key should return nil, true (purged)
	result, found := q.Get("key1")
	assert.True(t, found)
	assert.Nil(t, result)
}

func TestWriteQueue_Count(t *testing.T) {
	q := newWriteQueue[string]()

	// Initially empty
	assert.Equal(t, 0, q.Count())

	// Add some values
	value1 := "value1"
	value2 := "value2"
	q.Set("key1", &value1)
	q.Set("key2", &value2)

	assert.Equal(t, 2, q.Count())

	// Delete one
	q.Delete("key1")
	assert.Equal(t, 1, q.Count())

	// Purge
	q.Purge()
	assert.Equal(t, 0, q.Count())
}

func TestWriteQueue_CountPredicate(t *testing.T) {
	q := newWriteQueue[string]()

	// Add values
	value1 := "value1"
	value2 := "value2"
	value3 := "value3"
	q.Set("test_key1", &value1)
	q.Set("test_key2", &value2)
	q.Set("other_key", &value3)

	// Count keys starting with "test_"
	predicate := func(key string) bool {
		return strings.HasPrefix(key, "test_")
	}

	count := q.CountPredicate(predicate)
	assert.Equal(t, 2, count)

	// Delete one matching key
	q.Delete("test_key1")
	count = q.CountPredicate(predicate)
	assert.Equal(t, 1, count)
}

func TestWriteQueue_Keys(t *testing.T) {
	q := newWriteQueue[string]()

	// Initially empty
	keys := q.Keys()
	assert.Equal(t, 0, len(keys))

	// Add values
	value1 := "value1"
	value2 := "value2"
	q.Set("key1", &value1)
	q.Set("key2", &value2)

	keys = q.Keys()
	assert.Equal(t, 2, len(keys))
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")

	// Delete one
	q.Delete("key1")
	keys = q.Keys()
	assert.Equal(t, 1, len(keys))
	assert.Contains(t, keys, "key2")
	assert.NotContains(t, keys, "key1")
}

func TestWriteQueue_StartWriting_DoneWriting(t *testing.T) {
	q := newWriteQueue[string]()

	// Initially no operations
	op, ok := q.StartWriting()
	assert.False(t, ok)
	assert.Nil(t, op)

	// Add an operation
	value := "test_value"
	q.Set("key1", &value)

	// Start writing
	op, ok = q.StartWriting()
	assert.True(t, ok)
	assert.NotNil(t, op)

	// Check that CurrentlyWriting is set
	assert.Equal(t, op, q.CurrentlyWriting)

	// Verify it's a Set operation
	setOp, isSet := op.(*queueOperationSet[string])
	assert.True(t, isSet)
	assert.Equal(t, "key1", setOp.Key)
	assert.Equal(t, &value, setOp.Value)

	// Done writing
	q.DoneWriting(true)
	assert.Nil(t, q.CurrentlyWriting)
}

func TestWriteQueue_OperationOrdering(t *testing.T) {
	q := newWriteQueue[string]()

	// Add multiple operations
	value1 := "value1"
	value2 := "value2"
	q.Set("key1", &value1)
	q.Set("key2", &value2)
	q.Delete("key3")

	// Operations should be processed in order
	op1, ok1 := q.StartWriting()
	assert.True(t, ok1)
	q.DoneWriting(true)

	op2, ok2 := q.StartWriting()
	assert.True(t, ok2)
	q.DoneWriting(true)

	op3, ok3 := q.StartWriting()
	assert.True(t, ok3)
	q.DoneWriting(true)

	// Should be no more operations
	op4, ok4 := q.StartWriting()
	assert.False(t, ok4)
	assert.Nil(t, op4)

	// Verify operation types
	assert.IsType(t, &queueOperationSet[string]{}, op1)
	assert.IsType(t, &queueOperationSet[string]{}, op2)
	assert.IsType(t, &queueOperationDelete{}, op3)
}

func TestQueueOperationSet_Methods(t *testing.T) {
	value := "test_value"
	op := &queueOperationSet[string]{Key: "key1", Value: &value}

	assert.Equal(t, QueueOperationSet, op.GetType())
	assert.Equal(t, "key1", op.GetKey())
	assert.True(t, op.IncludesKey("key1"))
	assert.False(t, op.IncludesKey("key2"))

	// Test Includes with another operation
	otherOp := &queueOperationSet[string]{Key: "key1", Value: &value}
	assert.True(t, op.Includes(otherOp))

	otherOp2 := &queueOperationSet[string]{Key: "key2", Value: &value}
	assert.False(t, op.Includes(otherOp2))

	deleteOp := &queueOperationDelete{Key: "key1"}
	assert.True(t, op.Includes(deleteOp))
}

func TestQueueOperationDelete_Methods(t *testing.T) {
	op := &queueOperationDelete{Key: "key1"}

	assert.Equal(t, QueueOperationDelete, op.GetType())
	assert.Equal(t, "key1", op.GetKey())
	assert.True(t, op.IncludesKey("key1"))
	assert.False(t, op.IncludesKey("key2"))

	// Test Includes
	value := "test"
	setOp := &queueOperationSet[string]{Key: "key1", Value: &value}
	assert.True(t, op.Includes(setOp))

	otherDeleteOp := &queueOperationDelete{Key: "key1"}
	assert.True(t, op.Includes(otherDeleteOp))
}

func TestQueueOperationDeletePredicate_Methods(t *testing.T) {
	predicate := func(key string) bool {
		return strings.HasPrefix(key, "test_")
	}
	op := &queueOperationDeletePredicate{Predicate: predicate}

	assert.Equal(t, QueueOperationDeletePredicate, op.GetType())
	assert.True(t, op.IncludesKey("test_key"))
	assert.False(t, op.IncludesKey("other_key"))

	// Test Includes
	value := "test"
	setOp := &queueOperationSet[string]{Key: "test_key", Value: &value}
	assert.True(t, op.Includes(setOp))

	setOp2 := &queueOperationSet[string]{Key: "other_key", Value: &value}
	assert.False(t, op.Includes(setOp2))
}

func TestQueueOperationPurge_Methods(t *testing.T) {
	op := &queueOperationPurge{}

	assert.Equal(t, QueueOperationPurge, op.GetType())
	assert.True(t, op.IncludesKey("any_key"))

	// Test Includes - purge includes everything
	value := "test"
	setOp := &queueOperationSet[string]{Key: "any_key", Value: &value}
	assert.True(t, op.Includes(setOp))

	deleteOp := &queueOperationDelete{Key: "any_key"}
	assert.True(t, op.Includes(deleteOp))
}

func TestWriteQueue_ConcurrentAccess(t *testing.T) {
	q := newWriteQueue[int]()

	// Number of goroutines
	numGoroutines := 10
	numOperationsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Start multiple goroutines performing operations
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numOperationsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", goroutineID, j)
				value := goroutineID*1000 + j

				// Perform various operations
				q.Set(key, &value)
				q.Get(key)

				if j%10 == 0 {
					q.Delete(key)
				}

				if j%20 == 0 {
					q.Count()
					q.Keys()
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify the queue is still in a consistent state
	assert.NotNil(t, q.Values)
	count := q.Count()
	assert.GreaterOrEqual(t, count, 0)

	keys := q.Keys()
	assert.Equal(t, count, len(keys))
}

func TestWriteQueue_RemoveOverridden(t *testing.T) {
	q := newWriteQueue[string]()

	// Add multiple operations for the same key
	value1 := "value1"
	value2 := "value2"
	value3 := "value3"

	q.Set("key1", &value1)
	q.Set("key1", &value2)
	q.Set("key1", &value3)

	// Should have only 1 operation (the last set)
	assert.Equal(t, 1, q.Queue.Len())

	// Add a delete operation - should override all sets for this key
	q.Delete("key1")
	assert.Equal(t, 1, q.Queue.Len())

	// Verify the operation is a delete
	op, ok := q.StartWriting()
	assert.True(t, ok)
	assert.IsType(t, &queueOperationDelete{}, op)
}

func TestWriteQueue_NilValues(t *testing.T) {
	q := newWriteQueue[string]()

	// Set with nil value
	q.Set("key1", nil)

	// Should store nil in Values map
	assert.Contains(t, q.Values, "key1")
	assert.Nil(t, q.Values["key1"])

	// Get should return nil, true
	result, found := q.Get("key1")
	assert.True(t, found)
	assert.Nil(t, result)
}

func TestWriteQueue_ComplexScenario(t *testing.T) {
	q := newWriteQueue[string]()

	// Complex scenario: mix of operations
	value1 := "value1"
	value2 := "value2"
	value3 := "value3"

	// Set multiple keys
	q.Set("user:1", &value1)
	q.Set("user:2", &value2)
	q.Set("admin:1", &value3)

	// Delete predicate for user keys
	userPredicate := func(key string) bool {
		return strings.HasPrefix(key, "user:")
	}
	q.DeletePredicate(userPredicate)

	// Should have 2 operations (remaining Set for "admin:1" + DeletePredicate)
	assert.Equal(t, 2, q.Queue.Len())

	// Only admin:1 should remain in values
	assert.Equal(t, 1, len(q.Values))
	assert.Contains(t, q.Values, "admin:1")

	// Add another set after predicate delete
	value4 := "value4"
	q.Set("user:3", &value4)

	// Should have 3 operations now (admin:1 Set + DeletePredicate + user:3 Set)
	assert.Equal(t, 3, q.Queue.Len())

	// Values should contain admin:1 and user:3
	assert.Equal(t, 2, len(q.Values))
	assert.Contains(t, q.Values, "admin:1")
	assert.Contains(t, q.Values, "user:3")

	// Test Get for deleted key
	result, found := q.Get("user:1")
	assert.True(t, found)
	assert.Nil(t, result)

	// Test Get for existing keys
	result, found = q.Get("admin:1")
	assert.True(t, found)
	assert.Equal(t, &value3, result)

	result, found = q.Get("user:3")
	assert.True(t, found)
	assert.Equal(t, &value4, result)
}

// Test edge case: empty predicate (matches nothing)
func TestWriteQueue_DeletePredicate_EmptyMatch(t *testing.T) {
	q := newWriteQueue[string]()

	// Set some values
	value1 := "value1"
	value2 := "value2"
	q.Set("key1", &value1)
	q.Set("key2", &value2)

	// Predicate that matches nothing
	predicate := func(key string) bool {
		return false
	}
	q.DeletePredicate(predicate)

	// Should have 3 operations (2 sets + 1 delete predicate)
	assert.Equal(t, 3, q.Queue.Len())

	// Values should still contain both keys
	assert.Equal(t, 2, len(q.Values))
	assert.Contains(t, q.Values, "key1")
	assert.Contains(t, q.Values, "key2")
}

// Test edge case: predicate that matches everything
func TestWriteQueue_DeletePredicate_MatchAll(t *testing.T) {
	q := newWriteQueue[string]()

	// Set some values
	value1 := "value1"
	value2 := "value2"
	q.Set("key1", &value1)
	q.Set("key2", &value2)

	// Predicate that matches everything
	predicate := func(key string) bool {
		return true
	}
	q.DeletePredicate(predicate)

	// Should have 1 operation (just the delete predicate)
	assert.Equal(t, 1, q.Queue.Len())

	// Values should be empty
	assert.Equal(t, 0, len(q.Values))

	// All keys should return nil, true (deleted by predicate)
	result, found := q.Get("key1")
	assert.True(t, found)
	assert.Nil(t, result)

	result, found = q.Get("key2")
	assert.True(t, found)
	assert.Nil(t, result)
}

// Test DoneWriting with failure
func TestWriteQueue_DoneWriting_Failure(t *testing.T) {
	q := newWriteQueue[string]()

	// Add multiple operations
	value1 := "value1"
	value2 := "value2"
	q.Set("key1", &value1)
	q.Set("key2", &value2)

	// Start writing first operation
	op1, ok1 := q.StartWriting()
	assert.True(t, ok1)
	assert.NotNil(t, op1)

	// Mark as failed
	q.DoneWriting(false)

	// Operation should still be in queue (not removed on failure)
	assert.Equal(t, 2, q.Queue.Len())
	assert.Nil(t, q.CurrentlyWriting)

	// Next StartWriting should return the same operation
	op2, ok2 := q.StartWriting()
	assert.True(t, ok2)
	assert.Equal(t, op1, op2)
}

// Test mixed operation types and their interaction
func TestWriteQueue_MixedOperations(t *testing.T) {
	q := newWriteQueue[string]()

	// Test sequence: Set, Delete, Set again
	value1 := "value1"
	value2 := "value2"

	q.Set("key1", &value1)
	q.Delete("key1")
	q.Set("key1", &value2)

	// Should have 1 operation (final set overrides delete)
	assert.Equal(t, 1, q.Queue.Len())

	// Values should contain the latest value
	assert.Equal(t, 1, len(q.Values))
	assert.Equal(t, &value2, q.Values["key1"])

	// Get should return the latest value
	result, found := q.Get("key1")
	assert.True(t, found)
	assert.Equal(t, &value2, result)
}

// Test purge overrides everything
func TestWriteQueue_PurgeOverridesAll(t *testing.T) {
	q := newWriteQueue[string]()

	// Add various operations
	value1 := "value1"
	value2 := "value2"
	q.Set("key1", &value1)
	q.Set("key2", &value2)
	q.Delete("key3")

	predicate := func(key string) bool {
		return strings.HasPrefix(key, "test_")
	}
	q.DeletePredicate(predicate)

	// Purge should override everything
	q.Purge()

	// Should have only 1 operation (purge)
	assert.Equal(t, 1, q.Queue.Len())

	// Values should be empty
	assert.Equal(t, 0, len(q.Values))

	// Any key should return nil, true (purged)
	result, found := q.Get("key1")
	assert.True(t, found)
	assert.Nil(t, result)
}

// Test GetKey method is implemented correctly for all operation types
func TestQueueOperation_GetKey_Interface(t *testing.T) {
	value := "test"

	// Test that operations with keys implement the interface
	setOp := &queueOperationSet[string]{Key: "key1", Value: &value}
	deleteOp := &queueOperationDelete{Key: "key1"}

	// These should implement queueOperationWithKey
	var setInterface queueOperationWithKey = setOp
	var deleteInterface queueOperationWithKey = deleteOp

	assert.Equal(t, "key1", setInterface.GetKey())
	assert.Equal(t, "key1", deleteInterface.GetKey())

	// Test predicate and purge operations don't implement GetKey interface
	predicateOp := &queueOperationDeletePredicate{Predicate: func(string) bool { return true }}
	purgeOp := &queueOperationPurge{}

	// These should only implement queueOperation, not queueOperationWithKey
	_, setIsWithKey := interface{}(setOp).(queueOperationWithKey)
	_, deleteIsWithKey := interface{}(deleteOp).(queueOperationWithKey)
	_, predicateIsWithKey := interface{}(predicateOp).(queueOperationWithKey)
	_, purgeIsWithKey := interface{}(purgeOp).(queueOperationWithKey)

	assert.True(t, setIsWithKey)
	assert.True(t, deleteIsWithKey)
	assert.False(t, predicateIsWithKey)
	assert.False(t, purgeIsWithKey)
}

// Test operation type constants
func TestQueueOperation_Types(t *testing.T) {
	value := "test"

	setOp := &queueOperationSet[string]{Key: "key1", Value: &value}
	deleteOp := &queueOperationDelete{Key: "key1"}
	predicateOp := &queueOperationDeletePredicate{Predicate: func(string) bool { return true }}
	purgeOp := &queueOperationPurge{}

	assert.Equal(t, QueueOperationSet, setOp.GetType())
	assert.Equal(t, QueueOperationDelete, deleteOp.GetType())
	assert.Equal(t, QueueOperationDeletePredicate, predicateOp.GetType())
	assert.Equal(t, QueueOperationPurge, purgeOp.GetType())

	// Verify the constants have expected values
	assert.Equal(t, 0, QueueOperationSet)
	assert.Equal(t, 1, QueueOperationDelete)
	assert.Equal(t, 2, QueueOperationDeletePredicate)
	assert.Equal(t, 3, QueueOperationPurge)
}

// Test empty queue edge cases
func TestWriteQueue_EmptyQueue_EdgeCases(t *testing.T) {
	q := newWriteQueue[string]()

	// Operations on empty queue
	assert.Equal(t, 0, q.Count())
	assert.Equal(t, 0, len(q.Keys()))

	predicate := func(key string) bool { return true }
	assert.Equal(t, 0, q.CountPredicate(predicate))

	// Get from empty queue
	result, found := q.Get("nonexistent")
	assert.False(t, found)
	assert.Nil(t, result)

	// StartWriting from empty queue
	op, ok := q.StartWriting()
	assert.False(t, ok)
	assert.Nil(t, op)

	// DoneWriting when not writing
	q.DoneWriting(true)  // Should not panic
	q.DoneWriting(false) // Should not panic
}

// Test large number of operations performance
func TestWriteQueue_LargeOperations(t *testing.T) {
	q := newWriteQueue[int]()

	numOps := 1000

	// Add many operations
	for i := 0; i < numOps; i++ {
		value := i
		q.Set(fmt.Sprintf("key_%d", i), &value)
	}

	assert.Equal(t, numOps, q.Count())
	assert.Equal(t, numOps, q.Queue.Len())

	// Test predicate that matches half
	predicate := func(key string) bool {
		return strings.Contains(key, "key_5") // matches key_5, key_50-59, key_500-599
	}

	expectedMatches := 0
	for i := 0; i < numOps; i++ {
		if strings.Contains(fmt.Sprintf("key_%d", i), "key_5") {
			expectedMatches++
		}
	}

	count := q.CountPredicate(predicate)
	assert.Equal(t, expectedMatches, count)

	// Delete with predicate
	q.DeletePredicate(predicate)

	// Should have (numOps - matches + 1) operations (remaining sets + delete predicate)
	expectedRemainingOps := numOps - expectedMatches + 1
	assert.Equal(t, expectedRemainingOps, q.Queue.Len())
}
