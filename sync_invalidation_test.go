package cachier

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errEngineBoom = errors.New("engine boom")

// fakeEngine is an instrumented in-memory CacheEngine: each method's error
// is injectable and Set can be gated on a channel, so tests can provoke
// exact orderings between the write loop and synchronous invalidations.
type fakeEngine struct {
	mu    sync.Mutex
	store map[string]interface{}

	onSet func(key string) // runs synchronously inside Set, before storing

	deleteErr error
	keysErr   error
	purgeErr  error

	purgeCalls atomic.Int32
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{store: make(map[string]interface{})}
}

func (f *fakeEngine) Get(key string) (interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.store[key]; ok {
		return v, nil
	}
	return nil, ErrNotFound
}

func (f *fakeEngine) Peek(key string) (interface{}, error) {
	return f.Get(key)
}

func (f *fakeEngine) Set(key string, value interface{}) error {
	if f.onSet != nil {
		f.onSet(key)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store[key] = value
	return nil
}

func (f *fakeEngine) Delete(key string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.store, key)
	return nil
}

func (f *fakeEngine) Keys() ([]string, error) {
	if f.keysErr != nil {
		return nil, f.keysErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.store))
	for k := range f.store {
		keys = append(keys, k)
	}
	return keys, nil
}

func (f *fakeEngine) Purge() error {
	f.purgeCalls.Add(1)
	if f.purgeErr != nil {
		return f.purgeErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store = make(map[string]interface{})
	return nil
}

func (f *fakeEngine) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.store[key]
	return ok
}

func (f *fakeEngine) seed(key string, value *testEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store[key] = value
}

// recordingLogger captures log lines so tests can assert on them.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) log(args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprint(args...))
}

func (l *recordingLogger) Error(args ...interface{}) { l.log(args...) }
func (l *recordingLogger) Warn(args ...interface{})  { l.log(args...) }
func (l *recordingLogger) Print(args ...interface{}) { l.log(args...) }

func (l *recordingLogger) countContaining(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := 0
	for _, line := range l.lines {
		if strings.Contains(line, sub) {
			count++
		}
	}
	return count
}

// M12 (async form, decided 2026-07-08): invalidations never block on the
// engine — they mask in-process reads immediately via the queued op, the
// engine-side work happens on the write loop, and a failing engine op is
// retried head-of-line until it succeeds.

func TestDelete_MasksReadsImmediatelyAndRemovesOnFlush(t *testing.T) {
	engine := newFakeEngine()
	engine.seed("k", &testEntry{ID: 1})
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.Delete("k"))
	_, err := c.Get("k")
	assert.ErrorIs(t, err, ErrNotFound, "reads must see the deletion before the flush")
	assert.True(t, engine.has("k"), "the engine delete is deferred to the write loop")

	flushWriteQueue(t, c)
	assert.False(t, engine.has("k"), "the flush must remove the key from the engine")
}

func TestDelete_FailedEngineDeleteRetriesUntilRecovery(t *testing.T) {
	engine := newFakeEngine()
	engine.seed("k", &testEntry{ID: 1})
	engine.deleteErr = errEngineBoom
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.Delete("k"), "Delete reports nil by design; failures are the write loop's to retry")

	assert.False(t, c.runOneWriteCycle(), "the failing delete stops the drain")
	assert.True(t, engine.has("k"))

	engine.deleteErr = nil
	assert.True(t, c.runOneWriteCycle(), "the op stays at the front and succeeds once the engine recovers")
	assert.False(t, engine.has("k"))
}

func TestDelete_DiscardsPendingSet(t *testing.T) {
	engine := newFakeEngine()
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.Set("k", &testEntry{ID: 1}))
	require.NoError(t, c.Delete("k"))

	flushWriteQueue(t, c)
	assert.False(t, engine.has("k"), "a Set queued before Delete must never reach the engine")
	_, err := c.Get("k")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestDeletePredicate_MasksReadsImmediatelyAndRemovesOnFlush(t *testing.T) {
	engine := newFakeEngine()
	engine.seed("a:1", &testEntry{ID: 1})
	engine.seed("a:2", &testEntry{ID: 2})
	engine.seed("b:1", &testEntry{ID: 3})
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.DeleteWithPrefix("a:"))
	_, err := c.Get("a:1")
	assert.ErrorIs(t, err, ErrNotFound, "reads must see the predicate deletion before the flush")
	got, err := c.Get("b:1")
	require.NoError(t, err, "non-matching keys must stay readable")
	assert.Equal(t, testEntry{ID: 3}, *got)
	assert.True(t, engine.has("a:1"), "the engine delete is deferred to the write loop")

	flushWriteQueue(t, c)
	assert.False(t, engine.has("a:1"), "matching keys must be gone after the flush")
	assert.False(t, engine.has("a:2"), "matching keys must be gone after the flush")
	assert.True(t, engine.has("b:1"), "non-matching keys must survive")
}

func TestDeletePredicate_EngineFailureRetriesUntilRecovery(t *testing.T) {
	engine := newFakeEngine()
	engine.seed("k", &testEntry{ID: 1})
	engine.keysErr = errEngineBoom
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.DeletePredicate(func(string) bool { return true }), "DeletePredicate reports nil by design")

	assert.False(t, c.runOneWriteCycle(), "the failing enumeration stops the drain")
	assert.False(t, c.runOneWriteCycle(), "the op stays at the front while the engine is down")
	assert.True(t, engine.has("k"))

	engine.keysErr = nil
	assert.True(t, c.runOneWriteCycle(), "the retry succeeds once the engine recovers")
	assert.False(t, engine.has("k"))
}

func TestPurge_MasksReadsImmediatelyAndClearsOnFlush(t *testing.T) {
	engine := newFakeEngine()
	engine.seed("k1", &testEntry{ID: 1})
	engine.seed("k2", &testEntry{ID: 2})
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.Purge())
	_, err := c.Get("k1")
	assert.ErrorIs(t, err, ErrNotFound, "reads must see the purge before the flush")
	assert.True(t, engine.has("k1"), "the engine purge is deferred to the write loop")

	flushWriteQueue(t, c)
	assert.False(t, engine.has("k1"), "the engine must be cleared after the flush")
	assert.False(t, engine.has("k2"), "the engine must be cleared after the flush")
}

func TestPurge_DiscardsPendingSets(t *testing.T) {
	engine := newFakeEngine()
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.Set("k", &testEntry{ID: 1}))
	require.NoError(t, c.Purge())

	flushWriteQueue(t, c)
	assert.False(t, engine.has("k"), "a Set queued before Purge must never reach the engine")
	_, err := c.Get("k")
	assert.ErrorIs(t, err, ErrNotFound)
}

// Ordering: an invalidation issued while an engine write is in flight must
// not block the caller, and FIFO puts its queued op behind the in-flight
// Set — so once both flush, the key is absent.

func TestDeletePredicate_DoesNotBlockOnInFlightEngineWrite(t *testing.T) {
	engine := newFakeEngine()
	setStarted := make(chan struct{})
	setRelease := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(setRelease) }) }
	defer release()
	engine.onSet = func(string) {
		close(setStarted)
		<-setRelease
	}
	c := MakeCache[testEntry](engine, DummyLogger{})
	t.Cleanup(c.Close)

	require.NoError(t, c.Set("k", &testEntry{ID: 1}))
	<-setStarted // the write loop is now blocked inside engine.Set("k")

	done := make(chan error, 1)
	go func() { done <- c.DeletePredicate(func(string) bool { return true }) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("DeletePredicate must not block on an in-flight engine write")
	}

	release()
	assert.Eventually(t, func() bool { return !engine.has("k") }, 5*time.Second, 50*time.Millisecond,
		"the queued predicate delete lands after the in-flight write, leaving the key absent")
}

// M10: a queued purge that keeps failing must be attempted once per tick —
// logged, then yielding to the next tick — instead of hot-spinning silently
// inside a single tick.

func TestWriteLoop_FailingPurgeYieldsToNextTick(t *testing.T) {
	engine := newFakeEngine()
	engine.purgeErr = errEngineBoom
	logger := &recordingLogger{}
	c := MakeCache[testEntry](engine, logger)
	t.Cleanup(c.Close)

	require.NoError(t, c.Purge())

	time.Sleep(2500 * time.Millisecond)

	// The generous upper bound absorbs tick catch-up after CI scheduler
	// stalls; a hot-spinning loop produces millions of calls in this window.
	calls := engine.purgeCalls.Load()
	assert.GreaterOrEqual(t, calls, int32(1), "the queued purge must be attempted")
	assert.LessOrEqual(t, calls, int32(5), "a failing purge must be retried once per tick, not hot-spun within one tick")
	assert.NotZero(t, logger.countContaining("write loop error"), "the purge failure must be logged")
}

// Deterministic companion to the tick-based M10 test: one cycle = one
// attempt, the failing op stays at the front for the next cycle.

func TestRunOneWriteCycle_FailingOpStopsDrain(t *testing.T) {
	engine := newFakeEngine()
	engine.purgeErr = errEngineBoom
	logger := &recordingLogger{}
	c := newTestCache[testEntry](engine)
	c.logger = logger

	require.NoError(t, c.Purge())

	assert.False(t, c.runOneWriteCycle(), "a failing op must stop the drain")
	assert.Equal(t, int32(1), engine.purgeCalls.Load(), "exactly one attempt per cycle")
	assert.False(t, c.runOneWriteCycle(), "the op stays at the front and is retried on the next cycle")
	assert.Equal(t, int32(2), engine.purgeCalls.Load())
	assert.Equal(t, 2, logger.countContaining("write loop error"), "every failed attempt must be logged")
}

// Close: stops the write loop after one final drain, idempotent. Writes
// and invalidations enqueued after Close never reach the engine (they only
// mask in-process reads) — the documented contract.

func TestClose_StopsWriteLoopAndDrains(t *testing.T) {
	engine := newFakeEngine()
	c := MakeCache[testEntry](engine, DummyLogger{})

	require.NoError(t, c.Set("k", &testEntry{ID: 1}))
	c.Close()
	assert.True(t, engine.has("k"), "Close must drain queued writes before stopping")

	require.NoError(t, c.Delete("k"))
	_, err := c.Get("k")
	assert.ErrorIs(t, err, ErrNotFound, "a post-Close invalidation still masks in-process reads")
	assert.True(t, engine.has("k"), "a post-Close invalidation never reaches the engine — stop invalidating before Close")

	require.NoError(t, c.Set("later", &testEntry{ID: 2}))
	time.Sleep(1200 * time.Millisecond)
	assert.False(t, engine.has("later"), "no write loop may flush writes enqueued after Close")
}

func TestClose_Idempotent(t *testing.T) {
	c := MakeCache[testEntry](newFakeEngine(), DummyLogger{})
	c.Close()
	c.Close() // must not panic or block
}

// RedisCache.Keys iterates with SCAN; behavior must match the old KEYS
// implementation: all prefixed keys, stripped, nothing else.

func TestRedisKeys_ScanRespectsPrefix(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	rc := NewRedisCache(client, "pfx:", jsonMarshal, valueUnmarshal, 0, nil)

	require.NoError(t, rc.Set("a", &testEntry{ID: 1}))
	require.NoError(t, rc.Set("b", &testEntry{ID: 2}))
	require.NoError(t, server.Set("other:c", "x")) // outside the prefix

	keys, err := rc.Keys()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, keys)
}

// bulkFakeEngine is fakeEngine plus a recording BulkDeleter fast path.
type bulkFakeEngine struct {
	*fakeEngine
	manyMu    sync.Mutex
	manyCalls [][]string
}

func (b *bulkFakeEngine) DeleteMany(keys []string) error {
	b.manyMu.Lock()
	b.manyCalls = append(b.manyCalls, append([]string(nil), keys...))
	b.manyMu.Unlock()
	for _, key := range keys {
		if err := b.fakeEngine.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func TestPredicateFlushUsesBulkDeleter(t *testing.T) {
	engine := &bulkFakeEngine{fakeEngine: newFakeEngine()}
	engine.seed("a:1", &testEntry{ID: 1})
	engine.seed("a:2", &testEntry{ID: 2})
	engine.seed("b:1", &testEntry{ID: 3})
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.DeleteWithPrefix("a:"))
	flushWriteQueue(t, c)

	require.Len(t, engine.manyCalls, 1, "the flush must use the engine's bulk fast path, not per-key deletes")
	assert.ElementsMatch(t, []string{"a:1", "a:2"}, engine.manyCalls[0])
	assert.False(t, engine.has("a:1"))
	assert.True(t, engine.has("b:1"))
}

// RedisCache.Purge and DeleteMany remove keys in batched UNLINKs (one round
// trip per redisDeleteBatchSize keys) instead of one DEL per key.

func TestRedisPurge_BatchedUnlinkClearsLargeKeyspace(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	rc := NewRedisCache(client, "pfx:", jsonMarshal, valueUnmarshal, 0, nil)

	const n = 1100 // spans multiple UNLINK batches
	for i := 0; i < n; i++ {
		require.NoError(t, rc.Set(fmt.Sprintf("k%d", i), &testEntry{ID: i}))
	}
	require.NoError(t, server.Set("other:c", "x")) // outside the prefix

	require.NoError(t, rc.Purge())

	keys, err := rc.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys, "all prefixed keys must be gone")
	assert.True(t, server.Exists("other:c"), "keys outside the prefix must survive")
}
