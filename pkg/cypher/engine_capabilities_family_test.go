package cypher

import (
	"context"
	"fmt"
	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
	"time"
)

type engineCapabilityConditionalHistory struct{ *storage.MemoryEngine }

func (*engineCapabilityConditionalHistory) ReadCapabilityAvailable(reflect.Type) bool { return false }

type engineCapabilityHistoricalCore struct{ storage.Engine }

func (e *engineCapabilityHistoricalCore) GetInnerEngine() storage.Engine { return e.Engine }

func TestEngineDecoratorsHistoricalAvailabilityBoundary(t *testing.T) {
	for _, state := range []string{"present", "absent", "unavailable"} {
		t.Run(state, func(t *testing.T) {
			base := storage.NewMemoryEngineWithMVCCHistory()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			tenant := storage.NewNamespacedEngine(base, "tenant")
			properties := map[string]interface{}{"v": int64(7), "from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "to": time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)}
			_, err := tenant.CreateNode(&storage.Node{ID: "fact", Labels: []string{"HistoryBoundary"}, Properties: properties})
			require.NoError(t, err)
			head, err := tenant.GetNodeCurrentHead("fact")
			require.NoError(t, err)
			require.NoError(t, tenant.DeleteNode("fact"))
			var inner storage.Engine = base
			if state == "absent" {
				inner = &engineCapabilityHistoricalCore{Engine: base}
			}
			if state == "unavailable" {
				inner = &engineCapabilityConditionalHistory{base}
			}
			executor := NewStorageExecutor(storage.NewNamespacedEngine(inner, "tenant"))
			ctx := context.Background()
			_, err = executor.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
			_, err = executor.Execute(ctx, "CREATE (:HistoryBoundary {v:8})", nil)
			require.NoError(t, err)
			txView := executor.txContext.storageWrapper
			require.NotNil(t, txView)
			query := fmt.Sprintf("CALL db.temporal.asOf('HistoryBoundary','v',7,'from','to','2024-01-15T00:00:00Z','%s',%d) YIELD node RETURN node.v", head.Version.CommitTimestamp.Format(time.RFC3339Nano), head.Version.CommitSequence)
			for name, engine := range engineCapabilityStacks(txView) {
				t.Run(name, func(t *testing.T) {
					reader := NewStorageExecutor(engine)
					history, err := reader.Execute(ctx, query, nil)
					if state == "present" {
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(7)}}, history.Rows)
					} else {
						require.ErrorIs(t, err, storage.ErrNotImplemented)
					}
					pending, err := reader.Execute(ctx, "MATCH (n:HistoryBoundary) RETURN n.v", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(8)}}, pending.Rows)
					_, supported := capabilityForTest[storage.MVCCIndexedVisibilityEngine](engine)
					require.Equal(t, state == "present", supported)
				})
			}
		})
	}
}

func engineCapabilityStacks(view storage.Engine) map[string]storage.Engine {
	return map[string]storage.Engine{
		"direct": view,
		"T":      storage.NewTracedEngine(view),
		"W":      storage.NewWALEngine(view, nil),
		"WT":     storage.NewWALEngine(storage.NewTracedEngine(view), nil),
		"TW":     storage.NewTracedEngine(storage.NewWALEngine(view, nil)),
		"TWTW":   storage.NewTracedEngine(storage.NewWALEngine(storage.NewTracedEngine(storage.NewWALEngine(view, nil)), nil)),
	}
}

func TestEngineDecoratorsTransactionReadBoundary(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	_, err := tenant.CreateNode(&storage.Node{ID: "old", Labels: []string{"ReviewTx"}, Properties: map[string]interface{}{"id": int64(1)}})
	require.NoError(t, err)
	_, err = tenant.CreateNode(&storage.Node{ID: "gone", Labels: []string{"ReviewGone"}, Properties: map[string]interface{}{"id": int64(5)}})
	require.NoError(t, err)
	other := storage.NewNamespacedEngine(base, "other")
	_, err = other.CreateNode(&storage.Node{ID: "foreign", Labels: []string{"ReviewTx"}, Properties: map[string]interface{}{"id": int64(99)}})
	require.NoError(t, err)
	ctx := context.Background()
	exec := NewStorageExecutor(tenant)
	for _, q := range []string{"BEGIN", "MATCH (n:ReviewTx) SET n.id=3", "CREATE (:ReviewTx {id:4})", "MATCH (n:ReviewGone) DELETE n"} {
		_, err = exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
	require.NotNil(t, exec.txContext.storageWrapper)
	for name, engine := range engineCapabilityStacks(exec.txContext.storageWrapper) {
		t.Run(name, func(t *testing.T) {
			reader := NewStorageExecutor(engine)
			for i, query := range []struct {
				text string
				want [][]interface{}
			}{
				{"MATCH (n:ReviewTx) RETURN n.id ORDER BY n.id", [][]interface{}{{int64(3)}, {int64(4)}}},
				{"MATCH (n:ReviewTx) RETURN count(n)", [][]interface{}{{int64(2)}}},
				{"MATCH (n:ReviewGone) RETURN count(n)", [][]interface{}{{int64(0)}}},
			} {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					result, err := reader.Execute(ctx, query.text, nil)
					require.NoError(t, err, query.text)
					require.Equal(t, query.want, result.Rows, query.text)
				})
			}
		})
	}
	nodes, err := tenant.GetNodesByLabel("ReviewTx")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, int64(1), nodes[0].Properties["id"])
}

func TestEngineDecoratorsTemporalProcedure(t *testing.T) {
	base := storage.NewMemoryEngineWithMVCCHistory()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	other := storage.NewNamespacedEngine(base, "other")
	_, err := other.CreateNode(&storage.Node{ID: "fact", Labels: []string{"DecoratorFact"}, Properties: map[string]interface{}{"key": "k", "from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "to": time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), "id": int64(99)}})
	require.NoError(t, err)
	tenant := storage.NewNamespacedEngine(base, "tenant")
	_, err = tenant.CreateNode(&storage.Node{ID: "fact", Labels: []string{"DecoratorFact"}, Properties: map[string]interface{}{"key": "k", "from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "to": time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), "id": int64(7)}})
	require.NoError(t, err)
	head, err := tenant.GetNodeCurrentHead("fact")
	require.NoError(t, err)
	require.NoError(t, tenant.DeleteNode("fact"))
	query := fmt.Sprintf("CALL db.temporal.asOf('DecoratorFact','key','k','from','to','2024-01-15T00:00:00Z','%s',%d) YIELD node RETURN node.id", head.Version.CommitTimestamp.Format(time.RFC3339Nano), head.Version.CommitSequence)
	t.Log(query)
	for name, engine := range engineCapabilityStacks(tenant) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			executor := NewStorageExecutor(engine)
			result, err := executor.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
			_, err = executor.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer func() { _, rollbackErr := executor.Execute(ctx, "ROLLBACK", nil); require.NoError(t, rollbackErr) }()
			_, err = executor.Execute(ctx, "CREATE (:DecoratorFact {id:8})", nil)
			require.NoError(t, err)
			// The same decorated transaction owns both its historical read and pending
			// state; capability discovery must not replace it with committed storage.
			require.NotNil(t, executor.txContext.storageWrapper)
			for stack, transaction := range engineCapabilityStacks(executor.txContext.storageWrapper) {
				t.Run("transaction-"+stack, func(t *testing.T) {
					reader := NewStorageExecutor(transaction)
					historical, err := reader.Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(7)}}, historical.Rows)
					pending, err := reader.Execute(ctx, "MATCH (n:DecoratorFact) RETURN n.id", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(8)}}, pending.Rows)
				})
			}
		})
	}
}
func TestEngineDecoratorsCompositeTargeting(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	a := storage.NewNamespacedEngine(base, "a")
	b := storage.NewNamespacedEngine(base, "b")
	for _, seed := range []struct {
		engine storage.Engine
		id     storage.NodeID
		value  int64
	}{{a, "first", 1}, {b, "second", 2}} {
		_, err := seed.engine.CreateNode(&storage.Node{ID: seed.id, Labels: []string{"DecoratorComposite"}, Properties: map[string]interface{}{"id": seed.value}})
		require.NoError(t, err)
	}
	view := storage.NewCompositeEngine(map[string]storage.Engine{"a": a, "b": b}, map[string]string{"a": "a", "b": "b"}, map[string]string{"a": "read_write", "b": "read_write"})
	for name, engine := range engineCapabilityStacks(view) {
		t.Run(name, func(t *testing.T) {
			result, err := NewStorageExecutor(engine).Execute(context.Background(), "MATCH (n:DecoratorComposite) RETURN n.id ORDER BY n.id", nil)
			if result != nil {
				t.Logf("actual rows=%v", result.Rows)
			}
			require.Error(t, err, "composite root must require explicit constituent targeting")
			code, _ := nerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.NotAllowed", code)
		})
	}
}

type decoratedCompositeManager struct {
	DatabaseManagerInterface
	root storage.Engine
}

func (m *decoratedCompositeManager) GetStorageForUse(name, token string) (interface{}, error) {
	if name == "pcomp" {
		return m.root, nil
	}
	return m.DatabaseManagerInterface.GetStorageForUse(name, token)
}

func TestEngineDecoratorsExplicitCompositeTarget(t *testing.T) {
	fixture := newCompositeFixture(t)
	for name, engine := range engineCapabilityStacks(fixture.storage) {
		t.Run(name, func(t *testing.T) {
			executor := NewStorageExecutor(engine)
			executor.SetDatabaseManager(&decoratedCompositeManager{DatabaseManagerInterface: fixture.dbManager, root: engine})
			for _, query := range []string{
				"USE pcomp.other MATCH (n:EI) RETURN n.v",
				":USE pcomp.other\nMATCH (n:EI) RETURN n.v",
			} {
				result, err := executor.Execute(context.Background(), query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{"o"}}, result.Rows)
			}
			result, err := executor.Execute(context.Background(), "RETURN graph.names() AS g", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{[]interface{}{"pcomp.other", "pcomp.third"}}}, result.Rows)
		})
	}
}

func TestEngineDecoratorsSuppressedCountBoundary(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	_, err := tenant.CreateNode(&storage.Node{ID: "hidden", Labels: []string{"DecoratorHidden"}})
	require.NoError(t, err)
	require.NoError(t, tenant.SetManualSuppression(storage.StoredEntityNodeKind, "hidden", true))
	ctx := context.Background()
	for name, engine := range engineCapabilityStacks(tenant) {
		t.Run(name, func(t *testing.T) {
			executor := NewStorageExecutor(engine)
			for _, statement := range []string{"MATCH (n:DecoratorHidden) RETURN count(n)", "BEGIN", "MATCH (n:DecoratorHidden) RETURN count(n)", "ROLLBACK"} {
				result, err := executor.Execute(ctx, statement, nil)
				require.NoError(t, err)
				if statement[0] == 'M' {
					require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
				}
			}
		})
	}
}

func (e *engineCapabilityHistoricalCore) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return e.Engine, true
}
