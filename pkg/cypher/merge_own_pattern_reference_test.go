package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A relationship MERGE whose end node or relationship map reads the
// pattern's own start (or end) node reads it once the node is bound, when
// matching and when creating, as Neo4j 5.26 does. It stored the text "a.foo"
// and never matched an existing pattern.
func TestMergeMapReadsOwnPattern(t *testing.T) {
	for _, tc := range []struct {
		statements []string
		want       [][][]interface{}
	}{
		{[]string{"MERGE (a:MgW {foo: 1})-[:MgT]->(b:MgW {foo: a.foo}) RETURN b.foo AS v"}, [][][]interface{}{{{int64(1)}}}},
		{[]string{
			"MERGE (a:MgW {foo: 1})-[:MgT]->(b:MgW {foo: a.foo}) RETURN b.foo AS v",
			"MERGE (a:MgW {foo: 1})-[:MgT]->(b:MgW {foo: a.foo}) RETURN b.foo AS v",
			"MATCH (n:MgW) RETURN count(n) AS c",
		}, [][][]interface{}{{{int64(1)}}, {{int64(1)}}, {{int64(2)}}}},
		{[]string{
			"CREATE (:MgW {foo: 5})-[:MgT]->(:MgW {foo: 5})",
			"MERGE (a:MgW {foo: 5})-[:MgT]->(b:MgW {foo: a.foo}) RETURN b.foo AS v",
			"MATCH (n:MgW) RETURN count(n) AS c",
		}, [][][]interface{}{{}, {{int64(5)}}, {{int64(2)}}}},
		{[]string{"MERGE (a:MgW {x: 1})-[r:MgT {w: a.x}]->(b:MgW) RETURN r.w AS v"}, [][][]interface{}{{{int64(1)}}}},
		{[]string{"MERGE (a:MgW {x: 2})-[:MgT]->(b:MgW {y: a.x * 10}) RETURN b.y AS v"}, [][][]interface{}{{{int64(20)}}}},
		{[]string{"MERGE (a:MgW {x: 3})-[r:MgT {w: a.x + b.y}]->(b:MgW {y: 4}) RETURN r.w AS v"}, [][][]interface{}{{{int64(7)}}}},
		{[]string{"MERGE (a:MgW {x: 1})<-[:MgT]-(b:MgW {y: a.x}) RETURN b.y AS v"}, [][][]interface{}{{{int64(1)}}}},
		{[]string{
			"CREATE (:MgW {x: 1})-[:MgT {w: 1}]->(:MgW)",
			"MERGE (a:MgW {x: 1})-[r:MgT {w: a.x}]->(b:MgW) RETURN r.w AS v",
			"MATCH (n:MgW) RETURN count(n) AS c",
		}, [][][]interface{}{{}, {{int64(1)}}, {{int64(2)}}}},
		{[]string{"MERGE (a:MgW {x: 1})-[:MgT {}]->(b:MgW {y: a.x}) RETURN b.y AS v"}, [][][]interface{}{{{int64(1)}}}},
		{[]string{"MERGE (a:MgW {x: 1})-[:MgT]->(b:`Mg{W`) RETURN labels(b) AS v"}, [][][]interface{}{{{[]interface{}{"Mg{W"}}}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_own_pattern"))
		for index, statement := range tc.statements {
			result, err := exec.Execute(context.Background(), statement, nil)
			require.NoError(t, err, statement)
			want := tc.want[index]
			if len(want) == 0 {
				require.Empty(t, result.Rows, statement)
				continue
			}
			require.Equal(t, want, result.Rows, statement)
		}
	}
}

// A store read failure while a MERGE's match reads a label is the statement's
// error and the MERGE writes nothing. COMPONENT proof: the fixture's store
// exposes only storage.Engine, so the pipeline's label read is answered by
// GetNodesByLabel with the arbitrary injected error. The public native
// counterpart below proves the same pipeline read owner on the real
// transaction path, where the read is a transaction label stream and the error
// is the caller's real cancellation.
func TestComponentMergeMapReadsOwnPatternStoreError(t *testing.T) {
	ctx := context.Background()
	// The match reads the failing label where its pattern starts; a pattern
	// whose traversal never reaches it (no relationship from a) needn't.
	for _, statement := range []string{
		"MERGE (b:MgE {y: 1})-[:MgT]->(a:MgS {x: b.y}) RETURN a.x AS v",
		"MERGE (b:MgE)-[:MgT]->(a:MgS {x: 1}) RETURN b AS v",
	} {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_own_pattern_error")
		_, err := NewStorageExecutor(store).Execute(ctx, "CREATE (:MgS {x: 1})", nil)
		require.NoError(t, err)
		lookupErr := errors.New("end lookup failed")
		failing := &createMatchErrEngine{Engine: store, labelErrs: map[string]error{"MgE": lookupErr}}
		_, err = NewStorageExecutor(failing).executeRequiredPipeline(ctx, statement)
		require.ErrorIs(t, err, lookupErr, statement)
		require.Zero(t, failing.writes, "the failed MERGE reached no write")

		// Sensitivity control: the same statement writes when the label read
		// succeeds, so the assertions above are not satisfied by a statement
		// that can never write.
		healthy := &createMatchErrEngine{Engine: store}
		_, err = NewStorageExecutor(healthy).executeRequiredPipeline(ctx, statement)
		require.NoError(t, err, statement)
		require.Positive(t, healthy.writes, statement)
	}
}

// The public native counterpart: the same MERGE read, executed by public
// Execute in its implicit transaction, with the caller's real cancellation
// scheduled from the first observation of the statement's context made inside
// the transaction's label stream. The recorded stack proves the pipeline read
// owner, the graph proves the MERGE wrote nothing, and the healthy retry
// proves the same statement and owner merge and store their result.
func TestPublicMergeMapReadFailureRollsBackOnCancellation(t *testing.T) {
	const statement = "MERGE (b:MgE {y: 1})-[:MgT]->(a:MgS {x: b.y}) RETURN a.x AS v"
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_public_read_rollback")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(context.Background(), "CREATE (:MgS {x: 1})", nil)
	require.NoError(t, err)

	count := func(executor *StorageExecutor, label string) int64 {
		rows, err := executor.Execute(context.Background(), "MATCH (n:"+label+") RETURN count(n) AS c", nil)
		require.NoError(t, err)
		return rows.Rows[0][0].(int64)
	}
	require.Zero(t, count(exec, "MgE"))

	observer, cancel := newReaderEntryCancelContext(".(*BadgerTransaction).streamNodesByLabelProjected")
	defer cancel()
	_, err = exec.Execute(observer, statement, nil)
	require.True(t, observer.reached, "the MERGE must read its pattern label inside the transaction")
	require.ErrorIs(t, err, context.Canceled)
	requireFrameContaining(t, observer.frames, ".(*StorageExecutor).visitNodesWithStreamingProjection")
	requireFrameContaining(t, observer.frames, ".(*BadgerTransaction).streamNodesByLabelProjected")
	require.Zero(t, count(exec, "MgE"), "the canceled MERGE committed no node")
	edges, err := store.AllEdges(context.Background())
	require.NoError(t, err)
	require.Empty(t, edges, "the canceled MERGE committed no relationship")

	retried, err := exec.Execute(context.Background(), statement, nil)
	require.NoError(t, err)

	// Same statement, same initial graph, no cancellation: the retry's rows
	// and graph must be the ones the statement produces on its own.
	control := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_public_control"))
	_, err = control.Execute(context.Background(), "CREATE (:MgS {x: 1})", nil)
	require.NoError(t, err)
	controlResult, err := control.Execute(context.Background(), statement, nil)
	require.NoError(t, err)
	require.Equal(t, controlResult.Rows, retried.Rows)
	for _, label := range []string{"MgE", "MgS"} {
		require.Equal(t, count(control, label), count(exec, label), label)
	}
	controlEdges, err := control.Execute(context.Background(), "MATCH ()-[r:MgT]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, controlEdges.Rows, "the control must write the MERGE relationship")
	edges, err = store.AllEdges(context.Background())
	require.NoError(t, err)
	require.Len(t, edges, 1)
}
