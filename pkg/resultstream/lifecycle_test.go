package resultstream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type lifecycleStream struct {
	closes atomic.Int32
	err error
}

func (s *lifecycleStream) Pull(ctx context.Context, position uint64, n int) (*Page, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	if position > 0 && s.err != nil { return nil, s.err }
	return &Page{Rows: [][]any{{position}}, Next: position + 1, HasMore: position == 0}, nil
}
func (s *lifecycleStream) Close() error { s.closes.Add(1); return nil }
func (s *lifecycleStream) RetainedBytes() int64 { return 128 }

func TestStreamLifecycleEndPaths(t *testing.T) {
	for _, end := range []string{"last_page", "discard", "error", "disconnect", "invalidation", "timeout", "shutdown"} {
		t.Run(end, func(t *testing.T) {
			r, err := NewRegistry(Config{MaxStreams: 1, MaxStreamsPerOwner: 1, TTL: time.Hour})
			require.NoError(t, err)
			t.Cleanup(r.Close)
			scope := Scope{Owner: "alice", Database: "one"}
			s := &lifecycleStream{}
			first, err := r.Start(context.Background(), scope, s, 1)
			require.NoError(t, err)
			switch end {
			case "last_page":
				last, err := r.Pull(context.Background(), scope, first.QID, 1)
				require.NoError(t, err)
				require.False(t, last.HasMore)
			case "discard": require.NoError(t, r.Discard(scope, first.QID))
			case "error", "invalidation":
				s.err = errors.New("producer failed")
				if end == "invalidation" { s.err = ErrInvalidated }
				_, err := r.Pull(context.Background(), scope, first.QID, 1)
				require.ErrorIs(t, err, s.err)
			case "disconnect":
				ctx, cancel := context.WithCancel(context.Background()); cancel()
				_, err := r.Pull(ctx, scope, first.QID, 1)
				require.ErrorIs(t, err, context.Canceled)
			case "timeout": r.sweepExpired(first.ExpiresAt.Unix())
			case "shutdown": r.Close()
			}
			require.Zero(t, r.count.Load(), "ended streams must release admission")
			require.Zero(t, r.retained.Load())
			require.Empty(t, r.owners)
			require.EqualValues(t, 1, s.closes.Load())
			require.NoError(t, r.Discard(scope, first.QID), "DISCARD after every end is idempotent")
			require.NoError(t, r.Discard(scope, first.QID))
			require.EqualValues(t, 1, s.closes.Load())
			r.Close()
			require.EqualValues(t, 1, s.closes.Load())
		})
	}
}

func TestStreamLifecycleSequentialReadToEnd(t *testing.T) {
	r, err := NewRegistry(Config{MaxStreams: 1, MaxStreamsPerOwner: 1, TTL: time.Hour})
	require.NoError(t, err)
	t.Cleanup(r.Close)
	scope := Scope{Owner: "alice", Database: "one"}
	for i := 0; i < 100; i++ {
		first, err := r.Start(context.Background(), scope, &lifecycleStream{}, 1)
		require.NoError(t, err, "continuation %d", i)
		_, err = r.Pull(context.Background(), scope, first.QID, 1)
		require.NoError(t, err)
	}
}

func TestStreamLifecycleConcurrentEndsReleaseOnce(t *testing.T) {
	for i := 0; i < 50; i++ {
		r, err := NewRegistry(Config{TTL: time.Hour})
		require.NoError(t, err)
		scope := Scope{Owner: "alice", Database: "one"}
		s := &lifecycleStream{}
		first, err := r.Start(context.Background(), scope, s, 1)
		require.NoError(t, err)
		var wg sync.WaitGroup
		for _, end := range []func(){
			func() { _, _ = r.Pull(context.Background(), scope, first.QID, 1) },
			func() { _ = r.Discard(scope, first.QID) },
			func() { r.sweepExpired(first.ExpiresAt.Unix()) },
			func() { r.DiscardDatabase(scope.Database) },
			r.Close,
		} {
			wg.Add(1)
			go func() { defer wg.Done(); end() }()
		}
		wg.Wait()
		require.EqualValues(t, 1, s.closes.Load())
		require.Zero(t, r.count.Load())
		require.Zero(t, r.retained.Load())
		require.Empty(t, r.owners)
	}
}

func TestStreamLifecycleScopeAfterEnd(t *testing.T) {
	r, err := NewRegistry(Config{TTL: time.Hour})
	require.NoError(t, err)
	t.Cleanup(r.Close)
	scope := Scope{Owner: "alice", Database: "one"}
	first, err := r.Start(context.Background(), scope, &lifecycleStream{}, 1)
	require.NoError(t, err)
	require.NoError(t, r.Discard(scope, first.QID))
	for _, wrong := range []Scope{{Owner: "bob", Database: "one"}, {Owner: "alice", Database: "two"}} {
		require.ErrorIs(t, r.Discard(wrong, first.QID), ErrInvalidQID)
		_, err := r.ResolveDatabase(wrong.Owner, first.QID, wrong.Database)
		require.ErrorIs(t, err, ErrInvalidQID)
	}
	database, err := r.ResolveDatabase(scope.Owner, first.QID, "")
	require.NoError(t, err)
	require.Equal(t, scope.Database, database)
	_, err = r.Pull(context.Background(), scope, first.QID, 1)
	require.ErrorIs(t, err, ErrGoneQID)
	require.ErrorIs(t, r.Discard(scope, first.QID[:len(first.QID)-1]), ErrInvalidQID)
}

func TestStreamLifecycleDiscardDuringExpansion(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	s, err := NewProgressive([][]any{{0}, {1}}, false, 2, func(context.Context, int) ([][]any, bool, error) {
		close(entered)
		<-resume
		return [][]any{{0}, {1}, {2}}, true, nil
	})
	require.NoError(t, err)
	r, err := NewRegistry(Config{})
	require.NoError(t, err)
	t.Cleanup(r.Close)
	scope := Scope{Owner: "alice", Database: "one"}
	first, err := r.Start(context.Background(), scope, s, 1)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { _, err := r.Pull(context.Background(), scope, first.QID, 1); done <- err }()
	<-entered
	require.NoError(t, r.Discard(scope, first.QID))
	close(resume)
	require.ErrorIs(t, <-done, ErrClosed)
	require.Empty(t, s.rows, "an in-flight expansion must not republish closed rows")
}
