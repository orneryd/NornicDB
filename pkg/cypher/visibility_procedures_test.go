package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// kp.suppress hides a node or relationship from MATCH, traversal
// and counts, with decay disabled; reveal() still sees it; unsuppress shows
// it again; inside an explicit transaction it takes effect on COMMIT and not
// after ROLLBACK (#1064).
func TestDbVisibilitySuppress(t *testing.T) {
	ctx := context.Background()
	// An on-disk engine: reveal() opens its scope on a BadgerEngine.
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
	run := func(t *testing.T, query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	run(t, "CREATE (:Doc {id: 'a'})-[:CITES {id: 'link'}]->(:Doc {id: 'b'}), (:Doc {id: 'c'})")
	count := func(t *testing.T) int64 { return run(t, "MATCH (n:Doc) RETURN count(n)")[0][0].(int64) }
	require.Equal(t, int64(3), count(t))

	run(t, "MATCH (n:Doc {id: 'b'}) CALL kp.suppress(n)")
	require.Equal(t, int64(2), count(t), "a suppressed node isn't counted")
	require.Empty(t, run(t, "MATCH (n:Doc {id: 'b'}) RETURN n.id"))
	require.Empty(t, run(t, "MATCH (:Doc {id: 'a'})-[:CITES]->(b) RETURN b.id"), "nor traversed to")
	require.Equal(t, [][]interface{}{{"b"}}, run(t, "MATCH (n:Doc {id: 'b'}) RETURN reveal(n).id"))

	run(t, "MATCH (n:Doc {id: 'b'}) WHERE reveal(n) IS NOT NULL CALL kp.unsuppress(n)")
	require.Equal(t, int64(3), count(t))
	require.Equal(t, [][]interface{}{{"b"}}, run(t, "MATCH (:Doc {id: 'a'})-[:CITES]->(b) RETURN b.id"))

	// A relationship.
	run(t, "MATCH ()-[r:CITES {id: 'link'}]->() CALL kp.suppress(r)")
	require.Empty(t, run(t, "MATCH (:Doc {id: 'a'})-[r:CITES]->() RETURN r.id"))
	require.Equal(t, int64(3), count(t), "its endpoints stay")
	run(t, "MATCH ()-[r:CITES {id: 'link'}]->() WHERE reveal(r) IS NOT NULL CALL kp.unsuppress(r)")
	require.Equal(t, [][]interface{}{{"link"}}, run(t, "MATCH (:Doc {id: 'a'})-[r:CITES]->() RETURN r.id"))

	// Explicit transactions: applied on COMMIT, dropped on ROLLBACK.
	inTx := func(t *testing.T, terminal string) {
		t.Helper()
		_, err := exec.Execute(WithTransactionControl(ctx), "BEGIN", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "MATCH (n:Doc {id: 'c'}) CALL kp.suppress(n)", nil)
		require.NoError(t, err)
		_, err = exec.Execute(WithTransactionControl(ctx), terminal, nil)
		require.NoError(t, err)
	}
	inTx(t, "ROLLBACK")
	require.Equal(t, int64(3), count(t))
	inTx(t, "COMMIT")
	require.Equal(t, int64(2), count(t))

	for _, query := range []string{"CALL kp.suppress(1)", "CALL kp.unsuppress(null)"} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, "needs a node or a relationship", query)
	}
}

func TestManualSuppressionEngineLookup(t *testing.T) {
	require.Nil(t, manualSuppressionEngine(nil))
	base := newTestMemoryEngine(t)
	namespaced := storage.NewNamespacedEngine(base, "test")
	require.Equal(t, namespaced, manualSuppressionEngine(namespaced))
	require.Equal(t, namespaced, manualSuppressionEngine(&transactionStorageWrapper{underlying: namespaced}))
}

// With no storage events wired to search, as while the server's GraphQL
// handler holds them, suppress drops the node from search and unsuppress
// indexes it again itself (#1064).
func TestDbVisibilitySuppressUpdatesSearch(t *testing.T) {
	ctx := context.Background()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	store := storage.NewNamespacedEngine(engine, "test")
	exec := NewStorageExecutor(store)
	_, err = exec.Execute(ctx, "CREATE (:Doc {name: 'quokkalicious'})", nil)
	require.NoError(t, err)
	svc := search.NewService(store, search.DefaultVectorDimensions, nil)
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)
	hits := func(t *testing.T) int {
		t.Helper()
		response, err := svc.Search(ctx, "quokkalicious", nil, search.DefaultSearchOptions())
		require.NoError(t, err)
		return len(response.Results)
	}
	require.Equal(t, 1, hits(t))

	_, err = exec.Execute(ctx, "MATCH (n:Doc) CALL kp.suppress(n)", nil)
	require.NoError(t, err)
	require.Equal(t, 0, hits(t))
	_, err = exec.Execute(ctx, "MATCH (n:Doc) WHERE reveal(n) IS NOT NULL CALL kp.unsuppress(n)", nil)
	require.NoError(t, err)
	require.Equal(t, 1, hits(t))
}

// The two procedures are listed with localized descriptions (#1064).
func TestVisibilityProcedureMetadata(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	count := 0
	for _, spec := range globalProcedureRegistry.ListBuiltIns() {
		if spec.Name != "kp.suppress" && spec.Name != "kp.unsuppress" {
			continue
		}
		count++
		require.NotEmpty(t, spec.DescriptionMessage.ID, spec.Name)
		require.Equal(t, spec.Description, spec.DescriptionMessage.Fallback, spec.Name)
		require.Equal(t, ProcedureModeWrite, spec.Mode, spec.Name)
	}
	require.Equal(t, 2, count)
}

type selfUnwrappingEngine struct{ storage.Engine }

func (s *selfUnwrappingEngine) GetInnerEngine() storage.Engine { return s }

// A store without on-demand suppression refuses the procedures; a node or
// relationship no longer stored fails with ErrNotFound (#1064).
func TestDbVisibilitySuppressErrors(t *testing.T) {
	ctx := context.Background()
	type unsuppressible struct{ storage.Engine }
	plain := NewStorageExecutor(unsuppressible{Engine: newTestMemoryEngine(t)})
	_, err := plain.callKpSuppress(ctx, "kp.suppress", []interface{}{&storage.Node{ID: "x"}}, true)
	require.ErrorContains(t, err, "not supported by this storage engine")
	// A namespace over such a store says the same.
	namespaced := NewStorageExecutor(storage.NewNamespacedEngine(unsuppressible{Engine: newTestMemoryEngine(t)}, "test"))
	_, err = namespaced.callKpSuppress(ctx, "kp.unsuppress", []interface{}{&storage.Edge{ID: "x"}}, false)
	require.ErrorContains(t, err, "not supported by this storage engine")

	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
	_, err = exec.callKpSuppress(ctx, "kp.suppress", []interface{}{&storage.Node{ID: "missing"}}, true)
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = exec.callKpSuppress(ctx, "kp.suppress", []interface{}{&storage.Edge{ID: "missing"}}, true)
	require.ErrorIs(t, err, storage.ErrNotFound)

	require.Nil(t, manualSuppressionEngine(&selfUnwrappingEngine{Engine: newTestMemoryEngine(t)}))
}
