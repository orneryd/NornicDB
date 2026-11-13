package cypher

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func newHistoricalQueryStore(t *testing.T) *storage.BadgerEngine {
	t.Helper()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, EngineOptions: storage.EngineOptions{RetentionPolicy: storage.RetentionPolicy{MaxVersionsPerKey: 100}}}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	return base
}

func TestHistoricalQueryUsesRetainedLabelsAndProperties(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newHistoricalQueryStore(t), "history"))
			_, err := exec.Execute(t.Context(), "CREATE (:Account {name:'before'})", nil)
			require.NoError(t, err)
			at := time.Now().UTC()
			_, err = exec.Execute(t.Context(), "MATCH (n:Account) SET n.name='after'", nil)
			require.NoError(t, err)
			result, err := exec.Execute(t.Context(), "CALL db.temporal.asOf($at, 'MATCH (n:Account {name:$name}) RETURN n.name AS name', {name:'before'}) YIELD row RETURN row.name AS name", map[string]interface{}{"at": at})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"before"}}, result.Rows)
		})
	}
}

func TestHistoricalQueryRejectsWritesBeforeEffects(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newHistoricalQueryStore(t), "history_admission"))
	for _, body := range []string{"CREATE (:Forbidden)", "CALL apoc.cypher.run('CREATE (:Forbidden)', {}) YIELD value RETURN value", "CREATE INDEX forbidden FOR (n:Forbidden) ON (n.name)", "BEGIN"} {
		_, err := exec.Execute(t.Context(), "CALL db.temporal.asOf($at,$query) YIELD row RETURN row", map[string]interface{}{"at": time.Now().UTC(), "query": body})
		require.ErrorContains(t, err, "read-only", body)
	}
	result, err := exec.Execute(t.Context(), "MATCH (n:Forbidden) RETURN count(n) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestHistoricalQueryArgumentValidation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "history_arguments"))
	for _, query := range []string{"CALL db.temporal.asOf('not a date','RETURN 1')", "CALL db.temporal.asOf(datetime(),42)", "CALL db.temporal.asOf(datetime(),'RETURN 1',42)", "CALL db.temporal.asOf(datetime(),'RETURN 1',{},42)"} {
		_, err := exec.Execute(t.Context(), query, nil)
		require.Error(t, err, query)
	}
}

func TestHistoricalQueryDoesNotResurrectDeletedOrRecreatedEntities(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			base := newHistoricalQueryStore(t)
			ctx := t.Context()
			for _, node := range []*storage.Node{{ID: "history_lifetimes:a", Labels: []string{"Account"}, Properties: map[string]interface{}{"name": "old"}}, {ID: "history_lifetimes:b", Labels: []string{"Asset"}}} {
				_, err := base.CreateNode(ctx, node)
				require.NoError(t, err)
			}
			edge := &storage.Edge{ID: "history_lifetimes:owns", StartNode: "history_lifetimes:a", EndNode: "history_lifetimes:b", Type: "OWNS"}
			require.NoError(t, base.CreateEdge(ctx, edge))
			at := time.Now().UTC()
			exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "history_lifetimes"))
			read := func(query string) *ExecuteResult {
				t.Helper()
				result, err := exec.Execute(ctx, "CALL db.temporal.asOf($at,$query) YIELD row RETURN row", map[string]interface{}{"at": at, "query": query})
				require.NoError(t, err)
				return result
			}
			query := "MATCH (a:Account)-[r:OWNS]->(b:Asset) RETURN a.name AS name"
			require.Len(t, read(query).Rows, 1)
			require.NoError(t, base.DeleteEdge(ctx, edge.ID))
			require.Empty(t, read(query).Rows)
			require.NoError(t, base.CreateEdge(ctx, edge))
			require.Empty(t, read(query).Rows)
			require.NoError(t, base.DeleteNode(ctx, "history_lifetimes:a"))
			require.Empty(t, read("MATCH (a:Account) RETURN a.name AS name").Rows)
			_, err := base.CreateNode(ctx, &storage.Node{ID: "history_lifetimes:a", Labels: []string{"Account"}, Properties: map[string]interface{}{"name": "new"}})
			require.NoError(t, err)
			require.Empty(t, read("MATCH (a:Account) RETURN a.name AS name").Rows)
		})
	}
}

func TestHistoricalQueryFindsOldLabelAndIsolatesParameters(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newHistoricalQueryStore(t), "history_labels"))
	ctx := t.Context()
	for _, q := range []string{"CREATE (:Before {name:'old'})", "CREATE INDEX current_names FOR (n:Before) ON (n.name)"} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
	at := time.Now().UTC()
	_, err := exec.Execute(ctx, "MATCH (n:Before) SET n:After, n.name='new' REMOVE n:Before", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "CALL db.temporal.asOf($at,'MATCH (n:Before {name:$name}) RETURN n.name AS name',{name:'old'}) YIELD row RETURN row.name", map[string]interface{}{"at": at, "name": "outer-must-not-win"})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"old"}}, result.Rows)
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf($at,'RETURN $outer AS leaked',{}) YIELD row RETURN row", map[string]interface{}{"at": at, "outer": "must-not-leak"})
	require.Error(t, err)
}

func TestHistoricalQueryReportsPrunedHistory(t *testing.T) {
	base := newHistoricalQueryStore(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "history_prune"))
	ctx := t.Context()
	_, err := exec.Execute(ctx, "CREATE (:Account {name:'one'})", nil)
	require.NoError(t, err)
	at := time.Now().UTC()
	for _, query := range []string{"MATCH (n:Account) SET n.name='two'", "MATCH (n:Account) SET n.name='three'"} {
		_, err = exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	removed, err := base.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	require.Positive(t, removed)
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf($at,'MATCH (n:Account) RETURN n.name AS name') YIELD row RETURN row", map[string]interface{}{"at": at})
	require.ErrorIs(t, err, storage.ErrHistoryNotRetained)
}

func TestHistoricalQueryDoesNotReadPendingWrites(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newHistoricalQueryStore(t), "history_pending"))
	ctx := t.Context()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Account {name:'pending'})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "CALL db.temporal.asOf($at,'MATCH (n:Account) RETURN n.name AS name') YIELD row RETURN row", map[string]interface{}{"at": time.Now().UTC()})
	require.NoError(t, err)
	require.Empty(t, result.Rows)
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
}

func TestHistoricalQueryElementIDsMatchCurrentDatabaseIdentity(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newHistoricalQueryStore(t), "history_ids"))
	ctx := t.Context()
	created, err := exec.Execute(ctx, "CREATE (n:Account {name:'before'}) RETURN elementId(n) AS id", nil)
	require.NoError(t, err)
	require.Len(t, created.Rows, 1)
	id := created.Rows[0][0]
	at := time.Now().UTC()
	_, err = exec.Execute(ctx, "MATCH (n:Account) SET n.name='after'", nil)
	require.NoError(t, err)
	for _, query := range []string{"MATCH (n) WHERE elementId(n)=$id RETURN n.name AS name", "MATCH (n) WHERE elementId(n) IN $ids RETURN n.name AS name"} {
		result, err := exec.Execute(ctx, "CALL db.temporal.asOf($at,$query,{id:$id,ids:[$id]}) YIELD row RETURN row.name AS name", map[string]interface{}{"at": at, "query": query, "id": id})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"before"}}, result.Rows, query)
	}
}

func TestHistoricalQueryRejectsDatabaseSelectionAtExecutionBoundary(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newHistoricalQueryStore(t), "history_use"))
	for _, query := range []string{"USE history_use RETURN 1 AS value", "CALL { USE history_use RETURN 1 AS value } RETURN value", "CALL apoc.cypher.run('USE history_use RETURN 1 AS value',{}) YIELD value RETURN value"} {
		_, err := exec.Execute(t.Context(), "CALL db.temporal.asOf($at,$query) YIELD row RETURN row", map[string]interface{}{"at": time.Now().UTC(), "query": query})
		require.ErrorContains(t, err, "selected database", query)
	}
}
