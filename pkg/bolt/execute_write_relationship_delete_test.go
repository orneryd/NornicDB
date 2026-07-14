package bolt

import (
	"context"
	"errors"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDatabaseManagerBoltExecuteWriteDeletesUnwindMatchedRelationship(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": storage.NewNamespacedEngine(base, "nornic"),
		},
		defaultDB: "nornic",
	}
	server := NewWithDatabaseManager(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	port := startBoltTestServer(t, server)

	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, driver.Close(context.Background()))
	})
	require.NoError(t, driver.VerifyConnectivity(ctx))

	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: "nornic",
	})
	defer func() {
		require.NoError(t, session.Close(ctx))
	}()

	for _, query := range []string{
		"CREATE (:Function {uid: 'source-delete-a'}), (:Function {uid: 'target-delete-a'}), (:Function {uid: 'source-delete-b'}), (:Function {uid: 'target-delete-b'}), (:Function {uid: 'source-keep'}), (:Function {uid: 'target-keep'}), (:Function {uid: 'source-rollback'}), (:Function {uid: 'target-rollback'})",
		"MATCH (s:Function {uid: 'source-delete-a'}), (t:Function {uid: 'target-delete-a'}) CREATE (s)-[:TAINT_FLOWS_TO {evidence_source: 'wanted'}]->(t)",
		"MATCH (s:Function {uid: 'source-delete-b'}), (t:Function {uid: 'target-delete-b'}) CREATE (s)-[:TAINT_FLOWS_TO {evidence_source: 'wanted'}]->(t)",
		"MATCH (s:Function {uid: 'source-delete-a'}), (t:Function {uid: 'target-delete-b'}) CREATE (s)-[:TAINT_FLOWS_TO {evidence_source: 'other'}]->(t)",
		"MATCH (s:Function {uid: 'source-keep'}), (t:Function {uid: 'target-keep'}) CREATE (s)-[:TAINT_FLOWS_TO {evidence_source: 'wanted'}]->(t)",
		"MATCH (s:Function {uid: 'source-rollback'}), (t:Function {uid: 'target-rollback'}) CREATE (s)-[:TAINT_FLOWS_TO {evidence_source: 'wanted'}]->(t)",
	} {
		result, runErr := session.Run(ctx, query, nil)
		require.NoError(t, runErr)
		_, consumeErr := result.Consume(ctx)
		require.NoError(t, consumeErr)
	}

	executeDelete := func(uids []string) neo4jdriver.ResultSummary {
		t.Helper()
		summaryValue, executeErr := session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
			result, runErr := tx.Run(ctx, `UNWIND $uids AS suid
MATCH (s:Function {uid: suid})-[rel:TAINT_FLOWS_TO]->()
WHERE rel.evidence_source = $evidence_source
DELETE rel`, map[string]any{
				"uids":            uids,
				"evidence_source": "wanted",
			})
			if runErr != nil {
				return nil, runErr
			}
			return result.Consume(ctx)
		})
		require.NoError(t, executeErr)
		summary, ok := summaryValue.(neo4jdriver.ResultSummary)
		require.True(t, ok, "ExecuteWrite result type = %T, want neo4j.ResultSummary", summaryValue)
		return summary
	}

	rollbackErr := errors.New("force rollback")
	_, err = session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, runErr := tx.Run(ctx, `UNWIND $uids AS suid
MATCH (s:Function {uid: suid})-[rel:TAINT_FLOWS_TO]->()
WHERE rel.evidence_source = $evidence_source
DELETE rel`, map[string]any{
			"uids":            []string{"source-rollback"},
			"evidence_source": "wanted",
		})
		if runErr != nil {
			return nil, runErr
		}
		summary, consumeErr := result.Consume(ctx)
		if consumeErr != nil {
			return nil, consumeErr
		}
		require.Equal(t, 1, summary.Counters().RelationshipsDeleted())
		return nil, rollbackErr
	})
	require.ErrorIs(t, err, rollbackErr)

	// Verify rollback with a fresh autocommit query. Autocommit runs outside
	// the (now rolled-back) explicit transaction, so it observes committed
	// state only — the relationship must still exist.
	result, err := session.Run(ctx, `MATCH (:Function {uid: 'source-rollback'})-[rel:TAINT_FLOWS_TO]->(:Function {uid: 'target-rollback'}) RETURN count(rel)`, nil)
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), record.Values[0], "callback error must roll back the relationship deletion")

	summary := executeDelete(nil)
	require.Zero(t, summary.Counters().RelationshipsDeleted(), "empty UNWIND input must be a no-op")

	summary = executeDelete([]string{"source-delete-a", "missing-source", "source-delete-a", "source-delete-b"})
	require.Equal(t, 2, summary.Counters().RelationshipsDeleted())

	assertSurvivors := func() {
		t.Helper()
		result, runErr := session.Run(ctx, `MATCH (s:Function)-[rel:TAINT_FLOWS_TO]->(t:Function)
RETURN s.uid, rel.evidence_source, t.uid
ORDER BY s.uid, rel.evidence_source, t.uid`, nil)
		require.NoError(t, runErr)
		records, collectErr := result.Collect(ctx)
		require.NoError(t, collectErr)
		require.Len(t, records, 3)
		require.Equal(t, []any{"source-delete-a", "other", "target-delete-b"}, records[0].Values)
		require.Equal(t, []any{"source-keep", "wanted", "target-keep"}, records[1].Values)
		require.Equal(t, []any{"source-rollback", "wanted", "target-rollback"}, records[2].Values)
	}
	assertSurvivors()

	result, err = session.Run(ctx, "MATCH (n:Function) RETURN n", nil)
	require.NoError(t, err)
	records, err := result.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, records, 8, "relationship deletion must preserve all endpoint nodes")

	summary = executeDelete([]string{"source-delete-a", "source-delete-b"})
	require.Zero(t, summary.Counters().RelationshipsDeleted(), "repeated delete must be idempotent")

	assertSurvivors()

	result, err = session.Run(ctx, "CREATE (:MutationNode {uid: 'mutation-a', legacy: true}), (:MutationNode {uid: 'mutation-b', legacy: true})", nil)
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)

	executeMutation := func(query string) neo4jdriver.ResultSummary {
		t.Helper()
		summaryValue, executeErr := session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
			result, runErr := tx.Run(ctx, query, map[string]any{
				"uids": []string{"mutation-a", "mutation-b"},
			})
			if runErr != nil {
				return nil, runErr
			}
			return result.Consume(ctx)
		})
		require.NoError(t, executeErr)
		summary, ok := summaryValue.(neo4jdriver.ResultSummary)
		require.True(t, ok, "ExecuteWrite result type = %T, want neo4j.ResultSummary", summaryValue)
		return summary
	}

	summary = executeMutation("UNWIND $uids AS uid CREATE (n:UnwindCreated {uid: uid}) SET n.marked = true")
	require.Equal(t, 2, summary.Counters().NodesCreated(), "UNWIND CREATE must aggregate per-row node counters")
	require.Equal(t, 2, summary.Counters().PropertiesSet(), "UNWIND CREATE SET must aggregate per-row property counters")

	summary = executeMutation("UNWIND $uids AS uid MATCH (n:MutationNode {uid: uid}) WITH n SET n.marked = true")
	require.Equal(t, 2, summary.Counters().PropertiesSet(), "UNWIND MATCH WITH SET must aggregate per-row counters")
	result, err = session.Run(ctx, "MATCH (n:MutationNode {marked: true}) RETURN count(n)", nil)
	require.NoError(t, err)
	record, err = result.Single(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), record.Values[0], "UNWIND MATCH WITH SET must update every matched node")

	summary = executeMutation("UNWIND $uids AS uid MATCH (n:MutationNode {uid: uid}) WITH n REMOVE n.legacy")
	require.Equal(t, 2, summary.Counters().PropertiesSet(), "UNWIND MATCH WITH REMOVE must aggregate per-row counters")
	result, err = session.Run(ctx, "MATCH (n:MutationNode {legacy: true}) RETURN count(n)", nil)
	require.NoError(t, err)
	record, err = result.Single(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), record.Values[0], "UNWIND MATCH WITH REMOVE must update every matched node")

	summary = executeMutation("UNWIND $uids AS uid MATCH (n:MutationNode {uid: uid}) WITH n DETACH DELETE n")
	require.Equal(t, 2, summary.Counters().NodesDeleted(), "UNWIND MATCH WITH DELETE must aggregate per-row counters")

	result, err = session.Run(ctx, "MATCH (n:MutationNode) RETURN count(n)", nil)
	require.NoError(t, err)
	record, err = result.Single(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), record.Values[0])
}
