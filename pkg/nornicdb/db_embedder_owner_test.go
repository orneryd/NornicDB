package nornicdb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestDB_EmbedderOwnershipAliasesAndReplacement(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	first := &closingTestEmbedder{mockEmbedder: mockEmbedder{dims: 4, model: "first"}}
	_, accepted := db.OwnEmbedder(first) // Constructor/health-check ownership.
	require.True(t, accepted)
	require.True(t, db.SetEmbedder(embed.NewCachedEmbedder(first, 16)))
	db.SetDefaultEmbedConfig(&embed.Config{Provider: "local", Model: "first", Dimensions: 4})
	// Multiple cache and registry aliases must not introduce more owners.
	require.True(t, db.SetEmbedder(embed.NewCachedEmbedder(first, 32)))
	second := &closingTestEmbedder{mockEmbedder: mockEmbedder{dims: 4, model: "second"}}
	require.True(t, db.SetEmbedder(second))
	require.Zero(t, first.closes.Load(), "a query snapshot may still use the old provider")
	require.NoError(t, db.Close())
	require.EqualValues(t, 1, first.closes.Load())
	require.EqualValues(t, 1, second.closes.Load())
	require.NoError(t, db.Close())
	require.EqualValues(t, 1, first.closes.Load())
	require.EqualValues(t, 1, second.closes.Load())
}

func TestDB_EmbedderOwnershipReleaseIsIdempotent(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	e := &closingTestEmbedder{mockEmbedder: mockEmbedder{dims: 4}}
	release, accepted := db.OwnEmbedder(e)
	require.True(t, accepted)
	require.NoError(t, release())
	require.NoError(t, release())
	require.Empty(t, db.ownedEmbedders, "failed attempts must not accumulate during retries")
	require.NoError(t, db.Close())
	require.EqualValues(t, 1, e.closes.Load())
}

type orderedClosingEmbedder struct {
	closingTestEmbedder
	check func()
	err   error
}

func (e *orderedClosingEmbedder) Close() error {
	e.check()
	e.closes.Add(1)
	return e.err
}

func TestDB_EmbedderOwnershipClosesAfterUsersAndReturnsError(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	var joined atomic.Bool
	require.True(t, db.RunBackgroundTask(func() {
		<-db.LifetimeContext().Done()
		joined.Store(true)
	}))
	closeErr := errors.New("model release failed")
	e := &orderedClosingEmbedder{err: closeErr, check: func() {
		require.True(t, joined.Load(), "initialization must join before releasing models")
		require.True(t, db.GetEmbedQueue().closed.Load(), "queue must stop before releasing models")
	}}
	e.dims = 4
	require.True(t, db.SetEmbedder(e))
	require.ErrorIs(t, db.Close(), closeErr)
	require.EqualValues(t, 1, e.closes.Load())
}

func TestDB_EmbedderOwnershipPerDatabaseConstruction(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "installed", true: "constructing"}[pending], func(t *testing.T) {
			db, err := Open("", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			e := &closingTestEmbedder{mockEmbedder: mockEmbedder{dims: 4, model: "per-db"}}
			entered := make(chan struct{})
			db.embedderFactory = func(context.Context, *embed.Config) (embed.Embedder, error) {
				close(entered)
				if pending {
					<-db.LifetimeContext().Done()
				}
				return e, nil
			}
			result := make(chan embed.Embedder, 1)
			go func() { result <- db.embedderForConfig(&embed.Config{Provider: "local", Model: "per-db"}) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("model construction did not start")
			}
			if !pending {
				require.Same(t, e, <-result)
			}
			require.NoError(t, db.Close())
			if pending {
				require.Nil(t, <-result, "do not publish a model constructed during shutdown")
			}
			require.EqualValues(t, 1, e.closes.Load())
		})
	}
}
