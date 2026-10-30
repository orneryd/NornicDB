package nornicdb

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Reproduce the no-embedder MCP schedule through its public shared owners:
// an admitted search has a DB-child context while the transport stays live.
func TestCloseCancelsLexicalSearchCompletion(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeferSearchWarmup = true
	db, err := Open("", cfg)
	require.NoError(t, err)
	defer db.Close()
	engine := db.GetStorage()
	_, err = engine.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "needle"}})
	require.NoError(t, err)
	// An independently constructed real service prevents the DB registry's
	// asynchronous first-use warmup from rebuilding the fixture mid-request.
	svc := search.NewService(engine, search.DefaultVectorDimensions, nil)
	defer svc.Close()
	require.NoError(t, svc.BuildIndexes(t.Context()))
	request := t.Context()
	useCtx, finish, err := db.GetCypherExecutor().QueryEmbedderBinding().BeginUse(request)
	require.NoError(t, err)
	defer func() {
		if finish != nil {
			finish()
		}
	}()
	closed := make(chan error, 1)
	var once sync.Once
	opts := search.DefaultSearchOptions()
	filtered := false
	opts.Where = func(*storage.Node) bool {
		filtered = true
		once.Do(func() { go func() { closed <- db.Close() }() })
		<-useCtx.Done()
		return true
	}
	response, searchErr := search.SearchTextChunks(useCtx, "needle", opts, nil, nil, svc.Search)
	finish()
	finish = nil
	if filtered {
		require.NoError(t, <-closed)
	}
	require.True(t, filtered, "Close must interrupt actual lexical work")
	require.NoError(t, request.Err(), "the transport context remains live")
	require.ErrorIs(t, useCtx.Err(), context.Canceled)
	t.Logf("transport=%v db_child=%v returned_error=%v response=%+v", request.Err(), useCtx.Err(), searchErr, response)
	require.ErrorIs(t, searchErr, context.Canceled)
	require.Nil(t, response)
}
