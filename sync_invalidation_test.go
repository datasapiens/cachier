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

// M12: invalidations must reach the engine synchronously — before the call
// returns, not on a later write-loop tick — and must report the engine's
// real error instead of unconditionally returning nil.

func TestDelete_SynchronouslyRemovesFromEngine(t *testing.T) {
	engine := newFakeEngine()
	engine.seed("k", &testEntry{ID: 1})
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.Delete("k"))
	assert.False(t, engine.has("k"), "Delete must remove the key from the engine before returning, not on a later flush")
}

func TestDelete_ReturnsEngineError(t *testing.T) {
	engine := newFakeEngine()
	engine.deleteErr = errEngineBoom
	c := newTestCache[testEntry](engine)

	assert.ErrorIs(t, c.Delete("k"), errEngineBoom)
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

func TestDeletePredicate_SynchronouslyRemovesMatchingEngineKeys(t *testing.T) {
	engine := newFakeEngine()
	engine.seed("a:1", &testEntry{ID: 1})
	engine.seed("a:2", &testEntry{ID: 2})
	engine.seed("b:1", &testEntry{ID: 3})
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.DeleteWithPrefix("a:"))
	assert.False(t, engine.has("a:1"), "matching keys must be gone from the engine when the call returns")
	assert.False(t, engine.has("a:2"), "matching keys must be gone from the engine when the call returns")
	assert.True(t, engine.has("b:1"), "non-matching keys must survive")
}

func TestDeletePredicate_ReturnsEngineKeysError(t *testing.T) {
	engine := newFakeEngine()
	engine.keysErr = errEngineBoom
	c := newTestCache[testEntry](engine)

	assert.ErrorIs(t, c.DeletePredicate(func(string) bool { return true }), errEngineBoom)
}

func TestDeletePredicate_ReturnsEngineDeleteError(t *testing.T) {
	engine := newFakeEngine()
	engine.seed("k", &testEntry{ID: 1})
	engine.deleteErr = errEngineBoom
	c := newTestCache[testEntry](engine)

	assert.ErrorIs(t, c.DeletePredicate(func(string) bool { return true }), errEngineBoom)
}

func TestPurge_SynchronouslyClearsEngine(t *testing.T) {
	engine := newFakeEngine()
	engine.seed("k1", &testEntry{ID: 1})
	engine.seed("k2", &testEntry{ID: 2})
	c := newTestCache[testEntry](engine)

	require.NoError(t, c.Purge())
	assert.False(t, engine.has("k1"), "Purge must clear the engine before returning")
	assert.False(t, engine.has("k2"), "Purge must clear the engine before returning")
}

func TestPurge_ReturnsEngineError(t *testing.T) {
	engine := newFakeEngine()
	engine.purgeErr = errEngineBoom
	c := newTestCache[testEntry](engine)

	assert.ErrorIs(t, c.Purge(), errEngineBoom)
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

// M12 ordering: a synchronous invalidation must not interleave with an
// in-flight write-loop engine op — it lands strictly after, so the key is
// absent once both complete.

func TestDeletePredicate_WaitsForInFlightEngineWrite(t *testing.T) {
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
	case <-done:
		t.Fatal("DeletePredicate returned while an engine write for a matching key was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	release()
	require.NoError(t, <-done)
	assert.False(t, engine.has("k"), "the predicate delete must land after the in-flight write, leaving the key absent")
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

	// Seed the queue directly: after M12, Cache.Purge is synchronous and
	// never enqueues, but the write loop must still handle a queued purge.
	c.writeQueue.Purge()

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

	c.writeQueue.Purge()

	assert.False(t, c.runOneWriteCycle(), "a failing op must stop the drain")
	assert.Equal(t, int32(1), engine.purgeCalls.Load(), "exactly one attempt per cycle")
	assert.False(t, c.runOneWriteCycle(), "the op stays at the front and is retried on the next cycle")
	assert.Equal(t, int32(2), engine.purgeCalls.Load())
	assert.Equal(t, 2, logger.countContaining("write loop error"), "every failed attempt must be logged")
}

// Close: stops the write loop after one final drain, idempotent, and the
// synchronous invalidations keep working afterwards.

func TestClose_StopsWriteLoopAndDrains(t *testing.T) {
	engine := newFakeEngine()
	c := MakeCache[testEntry](engine, DummyLogger{})

	require.NoError(t, c.Set("k", &testEntry{ID: 1}))
	c.Close()
	assert.True(t, engine.has("k"), "Close must drain queued writes before stopping")

	require.NoError(t, c.Delete("k"))
	assert.False(t, engine.has("k"), "synchronous invalidations must keep working after Close")

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
