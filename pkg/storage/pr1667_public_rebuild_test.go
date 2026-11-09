package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A real reopen rebuild is held at its existing pre-commit seam. Public reads
// must use complete stored bodies rather than the unfinished secondary index.
func TestPR1667PublicReadsDuringLabelRebuild(t *testing.T) {
	dir := t.TempDir()
	initial, err := fixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	for _, namespace := range []string{"alpha", "beta"} {
		for i := 0; i < 3; i++ {
			_, err := initial.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("%s:n%d", namespace, i)), Labels: []string{"Repair"}, Properties: map[string]any{"value": int64(i)}})
			require.NoError(t, err)
		}
	}
	storage.ReviewRemoveDerivedLabelIndex(t, initial)
	require.NoError(t, initial.Close())
	entered, release := make(chan struct{}), make(chan struct{})
	var once, opened sync.Once
	unblock := func() { opened.Do(func() { close(release) }) }
	restore := storage.ReviewPauseLabelBackfillChunks(func() { once.Do(func() { close(entered); <-release }) })
	defer restore()
	defer unblock()
	engine, err := fixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	// Cleanup must release the worker before the fixture closes and joins it.
	t.Cleanup(unblock)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("rebuild did not reach existing chunk seam")
	}
	done := storage.ReviewLabelBackfillDone(t, engine)
	require.NotNil(t, done)
	for _, parser := range []struct {
		name         string
		selectParser func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			restoreParser := parser.selectParser()
			defer restoreParser()
			for _, namespace := range []string{"alpha", "beta", "empty"} {
				want := 3
				if namespace == "empty" {
					want = 0
				}
				executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, namespace), cypher.QueryCachePolicy{})
				for _, query := range []string{"MATCH (n:Repair) RETURN n.value ORDER BY n.value", "MATCH (n:Repair) RETURN count(n)"} {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					result, err := executor.Execute(ctx, query, nil)
					cancel()
					require.NoError(t, err, "%s/%s/%s", parser.name, namespace, query)
					if query == "MATCH (n:Repair) RETURN count(n)" {
						require.Len(t, result.Rows, 1)
						require.EqualValues(t, want, result.Rows[0][0])
					} else {
						require.Len(t, result.Rows, want, "%s/%s complete population", parser.name, namespace)
						for i, row := range result.Rows {
							require.EqualValues(t, i, row[0])
						}
					}
				}
				if want > 0 {
					merged, err := executor.Execute(context.Background(), "MERGE (n:Repair {value: 1}) RETURN n.value", nil)
					require.NoError(t, err, "%s/%s MERGE during repair", parser.name, namespace)
					require.Equal(t, [][]any{{int64(1)}}, merged.Rows)
					rows, err := executor.Execute(context.Background(), "MATCH (n:Repair) RETURN count(n)", nil)
					require.NoError(t, err)
					require.EqualValues(t, want, rows.Rows[0][0], "MERGE must not duplicate an unindexed stored node")
				}
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, engine.StreamNodesByLabelProjectedInScope(canceled, "alpha:", "absent", nil, func(*storage.Node) error { t.Error("canceled callback ran"); return nil }), context.Canceled)
	// A transaction pinned before readiness cannot borrow a later view's
	// positive readiness proof after the rebuild publishes.
	pinned, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pinned.Rollback() })
	require.NoError(t, pinned.SetNamespace("alpha"))
	unblock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("released rebuild did not finish")
	}
	count := 0
	require.NoError(t, pinned.StreamNodesByLabelProjected(context.Background(), "Repair", nil, func(node *storage.Node) error { count++; return nil }))
	require.Equal(t, 3, count, "pre-readiness snapshot keeps its complete authoritative population")
	require.NoError(t, pinned.Rollback())
	for _, namespace := range []string{"alpha", "beta"} {
		nodes, err := engine.GetNodesByLabelInScope(context.Background(), namespace+":", "Repair")
		require.NoError(t, err)
		require.Len(t, nodes, 3)
	}
	require.NoError(t, engine.Close())
	reopened, err := fixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	require.Nil(t, storage.ReviewLabelBackfillDone(t, reopened), "durable ready marker must survive restart")
	nodes, err := reopened.GetNodesByLabel(context.Background(), "Repair")
	require.NoError(t, err)
	require.Len(t, nodes, 6)
}
