package nornicdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// installRunningInitialBuild replaces the default database's search entry with
// one whose initial build is still running, until the returned finish is called.
func installRunningInitialBuild(t *testing.T, db *DB) (finish func()) {
	t.Helper()
	dbName := db.defaultDatabaseName()
	entry := &dbSearchService{dbName: dbName}
	finish = holdInitialBuild(entry)
	db.searchServicesMu.Lock()
	db.searchServices[dbName] = entry
	db.searchServicesMu.Unlock()
	return finish
}

// holdInitialBuild marks entry's initial build as running until finish is
// called; finish returns once the build completion has published.
func holdInitialBuild(entry *dbSearchService) (finish func()) {
	release := make(chan struct{})
	entry.build.Go(func() error { <-release; return nil })
	var once sync.Once
	return func() {
		once.Do(func() {
			close(release)
			<-entry.build.Done()
		})
	}
}

// A search that finds the initial build running waits for it; a caller whose
// context ends during that wait gets its own cancellation or deadline, never
// the build-in-progress error (which the caller could not tell from a
// still-running build and would retry).
func TestSearchAfterInitialBuildReturnsTheCallersCancellation(t *testing.T) {
	building := func(calls *int) func() (*search.SearchResponse, error) {
		return func() (*search.SearchResponse, error) {
			*calls++
			return nil, search.ErrSearchIndexBuilding
		}
	}

	t.Run("cancelled before the wait", func(t *testing.T) {
		db, err := Open("", DefaultConfig())
		require.NoError(t, err)
		defer db.Close()
		finish := installRunningInitialBuild(t, db)
		defer finish()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		response, err := db.searchAfterInitialBuild(ctx, building(&calls))
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, response)
		require.Equal(t, 1, calls)
	})

	t.Run("deadline during the wait", func(t *testing.T) {
		db, err := Open("", DefaultConfig())
		require.NoError(t, err)
		defer db.Close()
		finish := installRunningInitialBuild(t, db)
		defer finish()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		calls := 0
		response, err := db.searchAfterInitialBuild(ctx, building(&calls))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, response)
		require.Equal(t, 1, calls)
	})

	t.Run("cancelled with the build already finished", func(t *testing.T) {
		db, err := Open("", DefaultConfig())
		require.NoError(t, err)
		defer db.Close()
		finish := installRunningInitialBuild(t, db)
		finish()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		run := func() (*search.SearchResponse, error) {
			calls++
			if calls == 1 {
				return nil, search.ErrSearchIndexBuilding
			}
			return &search.SearchResponse{}, nil
		}
		response, err := db.searchAfterInitialBuild(ctx, run)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, response)
		require.Equal(t, 1, calls)
	})

	t.Run("build finishes during the wait", func(t *testing.T) {
		db, err := Open("", DefaultConfig())
		require.NoError(t, err)
		defer db.Close()
		finish := installRunningInitialBuild(t, db)

		calls := 0
		want := &search.SearchResponse{}
		run := func() (*search.SearchResponse, error) {
			calls++
			if calls == 1 {
				finish()
				return nil, search.ErrSearchIndexBuilding
			}
			return want, nil
		}
		response, err := db.searchAfterInitialBuild(context.Background(), run)
		require.NoError(t, err)
		require.Same(t, want, response)
		require.Equal(t, 2, calls)
	})
}
