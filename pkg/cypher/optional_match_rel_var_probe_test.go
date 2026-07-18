package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// BUG: an OPTIONAL MATCH-bound relationship variable resolves to nil in the
// raw executeMatch path, even when the OPTIONAL MATCH genuinely matched.
// ====================================================================================
// Discovered: eshu #5147 (surfaced while proving the DELETE residual-
// relationship guard above).
// Impact: executeDelete/executeSet/executeRemove all build an internal probe
//         shaped "MATCH ... OPTIONAL MATCH (a)-[r:TYPE]->(b) RETURN <vars>"
//         and execute it via the low-level e.executeMatch (pkg/cypher/match.go)
//         instead of the dispatcher (executor_query_routing.go
//         executeWithoutTransaction, which detects "OPTIONAL MATCH" and calls
//         executeCompoundMatchOptionalMatch). A bound relationship variable
//         from the embedded OPTIONAL MATCH projected to nil instead of the
//         real *storage.Edge, so a plain (non-DETACH) "DELETE r, chunk" could
//         not recognize r as one of the edges the statement itself removes,
//         and the new residual-relationship DELETE guard rejected it. The
//         same probe shape is used by SET/REMOVE, so "SET r.prop = ..." and
//         "REMOVE r.prop" on an OPTIONAL MATCH-bound relationship silently
//         no-op'd instead of mutating the real edge.
// Root Cause: executeMatch's relationship-pattern branch concatenates the
//         literal "OPTIONAL MATCH ..." text into one pattern string and hands
//         it to executeMatchWithRelationshipsWithPath ->
//         parseTraversalPatternStateMachine, which locates the relationship
//         bracket by scanning forward from the character right after the
//         FIRST node group's closing paren -- it never explicitly searches
//         for "-[" or "<-[". The embedded "OPTIONAL MATCH (n)" text between
//         the two node groups gets folded into the substring handed to
//         parseRelationshipPattern, which requires that substring (after
//         stripping a leading arrow) to start with "[". Since the corrupted
//         substring starts with whitespace instead, that check never fires
//         and the relationship variable/type/properties are silently
//         dropped.
// Fix: executeMatch now detects an embedded "OPTIONAL MATCH" inside a
//         relationship-pattern matchPart and routes the whole query through
//         executeCompoundMatchOptionalMatch, the same handler the top-level
//         dispatcher already uses for "MATCH ... OPTIONAL MATCH ..." queries.
// ====================================================================================

func setupOptionalMatchRelVarFixture(t *testing.T) (*StorageExecutor, storage.Engine) {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (n:Node {id: 'parent-node-id'})
		CREATE (c:NodeChunk:Node {id: 'chunk-1'})
		CREATE (o:Node {id: 'no-chunk-node'})
		CREATE (n)-[:HAS_CHUNK {index: 0}]->(c)
	`, nil)
	require.NoError(t, err)

	return exec, store
}

// TestBug_OptionalMatchRelVarResolvesToRealEdge exercises the raw internal
// probe path directly (exec.executeMatch), bypassing the top-level
// dispatcher, to prove the relationship variable bound by an embedded
// OPTIONAL MATCH resolves to a real *storage.Edge -- nil only when the
// OPTIONAL MATCH genuinely found no match.
func TestBug_OptionalMatchRelVarResolvesToRealEdge(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantEdgeNil bool
		wantNodeNil bool
	}{
		{
			name:        "OPTIONAL MATCH finds the relationship: r resolves to the real edge",
			query:       `MATCH (n:Node {id: 'parent-node-id'}) OPTIONAL MATCH (n)-[r:HAS_CHUNK]->(chunk:NodeChunk) RETURN r, chunk`,
			wantEdgeNil: false,
			wantNodeNil: false,
		},
		{
			name:        "OPTIONAL MATCH finds nothing: r and chunk both resolve to nil",
			query:       `MATCH (n:Node {id: 'no-chunk-node'}) OPTIONAL MATCH (n)-[r:HAS_CHUNK]->(chunk:NodeChunk) RETURN r, chunk`,
			wantEdgeNil: true,
			wantNodeNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec, _ := setupOptionalMatchRelVarFixture(t)
			ctx := context.Background()

			result, err := exec.executeMatch(ctx, tt.query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Len(t, result.Rows[0], 2)

			if tt.wantEdgeNil {
				assert.Nil(t, result.Rows[0][0], "r must be nil when OPTIONAL MATCH found nothing")
			} else {
				edge, ok := result.Rows[0][0].(*storage.Edge)
				require.True(t, ok, "r must resolve to a *storage.Edge, got %T", result.Rows[0][0])
				require.NotNil(t, edge)
				assert.Equal(t, "HAS_CHUNK", edge.Type)
			}

			if tt.wantNodeNil {
				assert.Nil(t, result.Rows[0][1], "chunk must be nil when OPTIONAL MATCH found nothing")
			} else {
				node, ok := result.Rows[0][1].(*storage.Node)
				require.True(t, ok, "chunk must resolve to a *storage.Node, got %T", result.Rows[0][1])
				require.NotNil(t, node)
				assert.Equal(t, "chunk-1", node.Properties["id"])
			}
		})
	}
}

// TestBug_DeletePlainOptionalMatchRelVar proves the root-cause fix, not a
// workaround: a plain (non-DETACH) "DELETE r, chunk" against an OPTIONAL
// MATCH-bound relationship now succeeds, because r resolves to the real edge
// being deleted and the residual-relationship guard recognizes it as part of
// this statement's own deletion set.
func TestBug_DeletePlainOptionalMatchRelVar(t *testing.T) {
	exec, _ := setupOptionalMatchRelVarFixture(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MATCH (n:Node {id: 'parent-node-id'})
		OPTIONAL MATCH (n)-[r:HAS_CHUNK]->(chunk:NodeChunk)
		DELETE r, chunk
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)
	assert.Equal(t, 1, result.Stats.RelationshipsDeleted)

	countResult, err := exec.Execute(ctx, `MATCH (c:NodeChunk {id: 'chunk-1'}) RETURN count(c)`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	assert.Equal(t, int64(0), countResult.Rows[0][0].(int64), "chunk node must be deleted")

	edgeCountResult, err := exec.Execute(ctx, `MATCH (:Node {id: 'parent-node-id'})-[r:HAS_CHUNK]->() RETURN count(r)`, nil)
	require.NoError(t, err)
	require.Len(t, edgeCountResult.Rows, 1)
	assert.Equal(t, int64(0), edgeCountResult.Rows[0][0].(int64), "HAS_CHUNK edge must be deleted")

	parentResult, err := exec.Execute(ctx, `MATCH (n:Node {id: 'parent-node-id'}) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Len(t, parentResult.Rows, 1)
	assert.Equal(t, int64(1), parentResult.Rows[0][0].(int64), "parent node itself must survive")
}

// TestBug_DeletePlainOptionalMatchRelVar_NoMatch proves a real OPTIONAL MATCH
// miss still leaves nothing to delete (r and chunk are genuinely nil, not a
// resolution failure), so the DELETE is a harmless no-op rather than an
// error.
func TestBug_DeletePlainOptionalMatchRelVar_NoMatch(t *testing.T) {
	exec, _ := setupOptionalMatchRelVarFixture(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MATCH (n:Node {id: 'no-chunk-node'})
		OPTIONAL MATCH (n)-[r:HAS_CHUNK]->(chunk:NodeChunk)
		DELETE r, chunk
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Stats.NodesDeleted)
	assert.Equal(t, 0, result.Stats.RelationshipsDeleted)

	parentResult, err := exec.Execute(ctx, `MATCH (n:Node {id: 'no-chunk-node'}) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Len(t, parentResult.Rows, 1)
	assert.Equal(t, int64(1), parentResult.Rows[0][0].(int64), "node with no chunk must survive")
}

// TestBug_SetOnOptionalMatchRelVar proves the same probe shape resolves
// correctly for SET: "MATCH ... OPTIONAL MATCH (a)-[r]->(b) SET r.prop = ..."
// must mutate the real edge, not silently no-op on a nil relationship
// variable.
func TestBug_SetOnOptionalMatchRelVar(t *testing.T) {
	exec, _ := setupOptionalMatchRelVarFixture(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MATCH (n:Node {id: 'parent-node-id'})
		OPTIONAL MATCH (n)-[r:HAS_CHUNK]->(chunk:NodeChunk)
		SET r.reviewed = true
		RETURN r.reviewed
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, true, result.Rows[0][0], "SET on an OPTIONAL MATCH-bound relationship variable must apply")

	verify, err := exec.Execute(ctx, `MATCH (:Node {id: 'parent-node-id'})-[r:HAS_CHUNK]->() RETURN r.reviewed`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	assert.Equal(t, true, verify.Rows[0][0], "the mutation must persist on the real stored edge")
}

// TestBug_RemoveOnOptionalMatchRelVar proves the same probe shape resolves
// correctly for REMOVE: "MATCH ... OPTIONAL MATCH (a)-[r]->(b) REMOVE
// r.prop" must remove the property from the real edge.
func TestBug_RemoveOnOptionalMatchRelVar(t *testing.T) {
	exec, _ := setupOptionalMatchRelVarFixture(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		MATCH (:Node {id: 'parent-node-id'})-[r:HAS_CHUNK]->()
		SET r.temp_flag = true
	`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH (n:Node {id: 'parent-node-id'})
		OPTIONAL MATCH (n)-[r:HAS_CHUNK]->(chunk:NodeChunk)
		REMOVE r.temp_flag
		RETURN r.temp_flag
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Nil(t, result.Rows[0][0], "REMOVE on an OPTIONAL MATCH-bound relationship variable must apply")

	verify, err := exec.Execute(ctx, `MATCH (:Node {id: 'parent-node-id'})-[r:HAS_CHUNK]->() RETURN r.temp_flag`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	assert.Nil(t, verify.Rows[0][0], "the property removal must persist on the real stored edge")
}

// TestBug_ChainedSetRemoveOnRelationshipVar exercises applyRemoveToMatchedRows
// (the helper backing a chained "SET ... REMOVE ..." clause in a single
// statement) with a relationship variable, proving its *storage.Edge branch
// removes the property from the real edge rather than silently skipping
// every non-node value in the row.
func TestBug_ChainedSetRemoveOnRelationshipVar(t *testing.T) {
	exec, _ := setupOptionalMatchRelVarFixture(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MATCH (n:Node {id: 'parent-node-id'})-[r:HAS_CHUNK]->(chunk:NodeChunk)
		SET chunk.reviewed = true
		REMOVE r.index
		RETURN r.index, chunk.reviewed
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Nil(t, result.Rows[0][0], "chained REMOVE must clear the relationship property")
	assert.Equal(t, true, result.Rows[0][1], "the chained SET must still apply to the node")

	verify, err := exec.Execute(ctx, `MATCH (:Node {id: 'parent-node-id'})-[r:HAS_CHUNK]->() RETURN r.index`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	assert.Nil(t, verify.Rows[0][0], "the property removal must persist on the real stored edge")
}
