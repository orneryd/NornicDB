package resultstream

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordingObserver struct {
	mu       sync.Mutex
	outcomes []string
	active   int64
	bytes    int64
}

func (o *recordingObserver) CursorEvent(outcome string) {
	o.mu.Lock()
	o.outcomes = append(o.outcomes, outcome)
	o.mu.Unlock()
}

func (o *recordingObserver) CursorUsage(active, retainedBytes int64) {
	o.mu.Lock()
	o.active, o.bytes = active, retainedBytes
	o.mu.Unlock()
}

func TestRegistryProgressesBeyondInitialPopulation(t *testing.T) {
	var expansions atomic.Int32
	stream, err := NewProgressive([][]any{{0}, {1}}, false, 2, func(_ context.Context, depth int) ([][]any, bool, error) {
		expansions.Add(1)
		if depth > 9 {
			depth = 9
		}
		rows := make([][]any, depth)
		for index := range rows {
			rows[index] = []any{index}
		}
		return rows, depth >= 9, nil
	})
	require.NoError(t, err)

	registry, err := NewRegistry(Config{TTL: time.Minute, MaxStreams: 4, MaxPageSize: 4})
	require.NoError(t, err)
	t.Cleanup(registry.Close)

	scope := Scope{Owner: "alice", Database: "nornic"}
	first, err := registry.Start(context.Background(), scope, stream, 2)
	require.NoError(t, err)
	require.Equal(t, [][]any{{0}, {1}}, first.Rows)
	require.True(t, first.HasMore)
	require.NotEmpty(t, first.QID)

	second, err := registry.Pull(context.Background(), scope, first.QID, 3)
	require.NoError(t, err)
	require.Equal(t, [][]any{{2}, {3}, {4}}, second.Rows)
	require.True(t, second.HasMore)

	replayed, err := registry.Pull(context.Background(), scope, first.QID, 3)
	require.NoError(t, err)
	require.Equal(t, second.Rows, replayed.Rows)
	require.Equal(t, second.QID, replayed.QID)

	qid := second.QID
	var all [][]any
	for qid != "" {
		page, pullErr := registry.Pull(context.Background(), scope, qid, 4)
		require.NoError(t, pullErr)
		all = append(all, page.Rows...)
		qid = page.QID
	}
	require.Equal(t, [][]any{{5}, {6}, {7}, {8}}, all)
	require.GreaterOrEqual(t, expansions.Load(), int32(2))
}

func TestRegistryBindsScopeAndDiscardsStream(t *testing.T) {
	stream, err := NewProgressive([][]any{{"first"}, {"second"}}, false, 2, func(_ context.Context, _ int) ([][]any, bool, error) {
		return [][]any{{"first"}, {"second"}, {"third"}}, true, nil
	})
	require.NoError(t, err)

	registry, err := NewRegistry(Config{TTL: time.Minute, MaxStreams: 4, MaxPageSize: 2})
	require.NoError(t, err)
	t.Cleanup(registry.Close)

	scope := Scope{Owner: "alice", Database: "nornic"}
	first, err := registry.Start(context.Background(), scope, stream, 1)
	require.NoError(t, err)

	_, err = registry.Pull(context.Background(), Scope{Owner: "bob", Database: "nornic"}, first.QID, 1)
	require.ErrorIs(t, err, ErrInvalidQID)

	require.NoError(t, registry.Discard(scope, first.QID))
	_, err = registry.Pull(context.Background(), scope, first.QID, 1)
	require.ErrorIs(t, err, ErrGoneQID)
}

func TestRegistryResolveDatabaseValidatesOwnerAndRequestedBinding(t *testing.T) {
	stream, err := NewProgressive([][]any{{"first"}, {"second"}}, true, 2, nil)
	require.NoError(t, err)
	registry, err := NewRegistry(Config{TTL: time.Minute, MaxStreams: 1, MaxPageSize: 1})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	page, err := registry.Start(context.Background(), Scope{Owner: "alice", Database: "tenant"}, stream, 1)
	require.NoError(t, err)

	database, err := registry.ResolveDatabase("alice", page.QID, "")
	require.NoError(t, err)
	require.Equal(t, "tenant", database)
	_, err = registry.ResolveDatabase("alice", page.QID, "other")
	require.ErrorIs(t, err, ErrInvalidQID)
	_, err = registry.ResolveDatabase("bob", page.QID, "")
	require.ErrorIs(t, err, ErrInvalidQID)
}

func TestRegistryDisabled(t *testing.T) {
	registry, err := NewRegistry(Config{Disabled: true})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	stream, err := NewProgressive([][]any{{"one"}, {"two"}}, true, 2, nil)
	require.NoError(t, err)

	scope := Scope{Owner: "owner", Database: "neo4j"}
	_, err = registry.Start(context.Background(), scope, stream, 1)
	require.ErrorIs(t, err, ErrDisabled)
	_, err = registry.Pull(context.Background(), scope, "qid", 1)
	require.ErrorIs(t, err, ErrDisabled)
	require.ErrorIs(t, registry.Discard(scope, "qid"), ErrDisabled)
}

func TestRegistryObserverTracksLifecycleAndUsage(t *testing.T) {
	registry, err := NewRegistry(Config{TTL: time.Minute, MaxStreams: 2, MaxPageSize: 1})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	observer := &recordingObserver{}
	registry.SetObserver(observer)
	stream, err := NewProgressive([][]any{{"one"}, {"two"}}, true, 2, nil)
	require.NoError(t, err)
	scope := Scope{Owner: "owner", Database: "neo4j"}

	page, err := registry.Start(context.Background(), scope, stream, 1)
	require.NoError(t, err)
	require.NoError(t, registry.Discard(scope, page.QID))

	observer.mu.Lock()
	defer observer.mu.Unlock()
	require.Equal(t, []string{"start", "discard"}, observer.outcomes)
	require.Zero(t, observer.active)
	require.Zero(t, observer.bytes)
}

func TestRegistryDoesNotPublishExhaustedFirstPage(t *testing.T) {
	stream, err := NewProgressive([][]any{{"only"}}, true, 1, nil)
	require.NoError(t, err)
	registry, err := NewRegistry(Config{TTL: time.Minute, MaxStreams: 1, MaxPageSize: 1})
	require.NoError(t, err)
	t.Cleanup(registry.Close)

	page, err := registry.Start(context.Background(), Scope{Owner: "alice", Database: "nornic"}, stream, 1)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"only"}}, page.Rows)
	require.False(t, page.HasMore)
	require.Empty(t, page.QID)
	require.Zero(t, registry.count.Load())

	_, err = stream.Pull(context.Background(), 0, 1)
	require.ErrorIs(t, err, ErrClosed)
}

func TestRegistryExpiryIsFixedAndRemovesExpiredStream(t *testing.T) {
	stream, err := NewProgressive([][]any{{0}, {1}, {2}, {3}}, true, 4, nil)
	require.NoError(t, err)
	registry, err := NewRegistry(Config{TTL: time.Second, MaxStreams: 1, MaxPageSize: 1})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	scope := Scope{Owner: "alice", Database: "nornic"}

	first, err := registry.Start(context.Background(), scope, stream, 1)
	require.NoError(t, err)
	second, err := registry.Pull(context.Background(), scope, first.QID, 1)
	require.NoError(t, err)
	require.Equal(t, first.ExpiresAt, second.ExpiresAt)
	require.Eventually(t, func() bool {
		_, pullErr := registry.Pull(context.Background(), scope, second.QID, 1)
		return errors.Is(pullErr, ErrExpiredQID)
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, registry.count.Load())
}

func TestRegistryReapsAbandonedExpiredStream(t *testing.T) {
	stream, err := NewProgressive([][]any{{0}, {1}}, true, 2, nil)
	require.NoError(t, err)
	registry, err := NewRegistry(Config{TTL: 100 * time.Millisecond, MaxStreams: 1, MaxPageSize: 1})
	require.NoError(t, err)
	t.Cleanup(registry.Close)

	_, err = registry.Start(context.Background(), Scope{Owner: "alice", Database: "nornic"}, stream, 1)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return registry.count.Load() == 0 }, 2*time.Second, 10*time.Millisecond)

	replacement, err := NewProgressive([][]any{{2}, {3}}, true, 2, nil)
	require.NoError(t, err)
	_, err = registry.Start(context.Background(), Scope{Owner: "bob", Database: "nornic"}, replacement, 1)
	require.NoError(t, err)
}

func TestRegistryCloseReleasesStreamsAndRejectsOperations(t *testing.T) {
	stream, err := NewProgressive([][]any{{0}, {1}}, true, 2, nil)
	require.NoError(t, err)
	registry, err := NewRegistry(Config{TTL: time.Minute, MaxStreams: 1, MaxPageSize: 1})
	require.NoError(t, err)
	scope := Scope{Owner: "alice", Database: "nornic"}
	first, err := registry.Start(context.Background(), scope, stream, 1)
	require.NoError(t, err)

	registry.Close()
	require.Zero(t, registry.count.Load())
	_, err = registry.Pull(context.Background(), scope, first.QID, 1)
	require.ErrorIs(t, err, ErrClosed)
	_, err = stream.Pull(context.Background(), 0, 1)
	require.ErrorIs(t, err, ErrClosed)
}

func TestRegistryConcurrentReplayIsDeterministic(t *testing.T) {
	stream, err := NewProgressive([][]any{{0}, {1}, {2}, {3}}, true, 4, nil)
	require.NoError(t, err)
	registry, err := NewRegistry(Config{TTL: time.Minute, MaxStreams: 1, MaxPageSize: 2})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	scope := Scope{Owner: "alice", Database: "nornic"}
	first, err := registry.Start(context.Background(), scope, stream, 1)
	require.NoError(t, err)

	const workers = 16
	pages := make(chan *Page, workers)
	errs := make(chan error, workers)
	for range workers {
		go func() {
			page, pullErr := registry.Pull(context.Background(), scope, first.QID, 2)
			pages <- page
			errs <- pullErr
		}()
	}
	for range workers {
		require.NoError(t, <-errs)
		page := <-pages
		require.Equal(t, [][]any{{1}, {2}}, page.Rows)
		require.NotEmpty(t, page.QID)
	}
}

func TestRegistryEnforcesPerOwnerStreamLimit(t *testing.T) {
	registry, err := NewRegistry(Config{TTL: time.Minute, MaxStreams: 3, MaxStreamsPerOwner: 1, MaxPageSize: 1})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	newStream := func() Stream {
		stream, streamErr := NewProgressive([][]any{{0}, {1}}, true, 2, nil)
		require.NoError(t, streamErr)
		return stream
	}

	_, err = registry.Start(context.Background(), Scope{Owner: "alice", Database: "one"}, newStream(), 1)
	require.NoError(t, err)
	_, err = registry.Start(context.Background(), Scope{Owner: "alice", Database: "two"}, newStream(), 1)
	require.ErrorIs(t, err, ErrCapacity)
	_, err = registry.Start(context.Background(), Scope{Owner: "bob", Database: "one"}, newStream(), 1)
	require.NoError(t, err)
}

type sizedTestStream struct {
	Stream
	bytes int64
}

func (s sizedTestStream) RetainedBytes() int64 { return s.bytes }

type growingSizedTestStream struct {
	*Progressive
	bytes atomic.Int64
}

func (s *growingSizedTestStream) RetainedBytes() int64 { return s.bytes.Load() }

func TestRegistryEnforcesPerOwnerRetainedByteLimit(t *testing.T) {
	registry, err := NewRegistry(Config{
		TTL: time.Minute, MaxStreams: 3, MaxStreamsPerOwner: 3, MaxPageSize: 1,
		MaxRetainedBytes: 100, MaxRetainedBytesPerOwner: 60,
	})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	newStream := func(bytes int64) Stream {
		stream, streamErr := NewProgressive([][]any{{0}, {1}}, true, 2, nil)
		require.NoError(t, streamErr)
		return sizedTestStream{Stream: stream, bytes: bytes}
	}

	first, err := registry.Start(context.Background(), Scope{Owner: "alice", Database: "one"}, newStream(40), 1)
	require.NoError(t, err)
	_, err = registry.Start(context.Background(), Scope{Owner: "alice", Database: "two"}, newStream(30), 1)
	require.ErrorIs(t, err, ErrCapacity)
	require.NoError(t, registry.Discard(Scope{Owner: "alice", Database: "one"}, first.QID))
	_, err = registry.Start(context.Background(), Scope{Owner: "alice", Database: "two"}, newStream(60), 1)
	require.NoError(t, err)
}

func TestRegistryRejectsProgressiveRetainedByteGrowth(t *testing.T) {
	var stream *growingSizedTestStream
	progressive, err := NewProgressive([][]any{{0}, {1}}, false, 2, func(_ context.Context, _ int) ([][]any, bool, error) {
		stream.bytes.Store(80)
		return [][]any{{0}, {1}, {2}}, true, nil
	})
	require.NoError(t, err)
	stream = &growingSizedTestStream{Progressive: progressive}
	stream.bytes.Store(40)

	registry, err := NewRegistry(Config{
		TTL: time.Minute, MaxStreams: 2, MaxStreamsPerOwner: 2, MaxPageSize: 2,
		MaxRetainedBytes: 70, MaxRetainedBytesPerOwner: 70,
	})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	scope := Scope{Owner: "alice", Database: "nornic"}
	page, err := registry.Start(context.Background(), scope, stream, 1)
	require.NoError(t, err)

	_, err = registry.Pull(context.Background(), scope, page.QID, 2)
	require.ErrorIs(t, err, ErrCapacity)
	require.Zero(t, registry.retained.Load())
	require.Zero(t, registry.count.Load())
}

func TestScopeDigestIsKeyedAndBindsOwnerAndDatabase(t *testing.T) {
	var firstSecret [32]byte
	var secondSecret [32]byte
	firstSecret[0] = 1
	secondSecret[0] = 2
	scope := Scope{Owner: "sub:alice", Database: "nornic"}

	require.Equal(t, scopeDigest(firstSecret, scope), scopeDigest(firstSecret, scope))
	require.NotEqual(t, scopeDigest(firstSecret, scope), scopeDigest(secondSecret, scope))
	require.NotEqual(t, scopeDigest(firstSecret, scope), scopeDigest(firstSecret, Scope{Owner: "sub:bob", Database: "nornic"}))
	require.NotEqual(t, scopeDigest(firstSecret, scope), scopeDigest(firstSecret, Scope{Owner: "sub:alice", Database: "other"}))
}

func TestTokenMACMatchesStandardHMACSHA256(t *testing.T) {
	var secret [32]byte
	for index := range secret {
		secret[index] = byte(index + 1)
	}
	body := make([]byte, tokenBodyBytes)
	for index := range body {
		body[index] = byte(index * 3)
	}

	standard := hmac.New(sha256.New, secret[:])
	_, err := standard.Write(body)
	require.NoError(t, err)
	actual := tokenMAC(secret, body)
	require.Equal(t, standard.Sum(nil), actual[:])
}
