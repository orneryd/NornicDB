package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The transaction owner refuses a node update that breaks a unique constraint,
// and the SET statement reports that refusal as its own error, naming the
// assignment it came from. An explicit transaction validates node constraints
// per statement, so the refusal is raised at the write the SET performs, and
// nothing it staged reaches storage.
//
// The refusal is the schema owner's own (CREATE CONSTRAINT ... IS UNIQUE): a
// local write statement stages its writes in one atomic owner, so the former
// fake engine's UpdateNode fault - which the statement's transaction never
// calls - cannot be observed truthfully any more.
func TestSetPropagatesUniqueConstraintRefusalForMergeAssignment(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT set_merge_assignment FOR (n:Person) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Person {id: 'a', k: 'taken'}), (:Person {id: 'b', k: 'free'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (n:Person {id: 'b'}) SET n += {k: 'taken'} RETURN n", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SET n +=")
	require.Contains(t, err.Error(), "UNIQUE on Person.[k]")
	_, _ = exec.Execute(ctx, "ROLLBACK", nil)

	// The refused assignment was never applied: the stored node is untouched.
	stored, err := NewStorageExecutor(store).Execute(ctx, "MATCH (n:Person {id: 'b'}) RETURN n.k AS k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"free"}}, stored.Rows)

	// Sensitivity control: the same assignment writes when its value does not
	// break the constraint, so "untouched" above is not the state of a
	// statement that can never write this property.
	_, err = NewStorageExecutor(store).Execute(ctx, "MATCH (n:Person {id: 'b'}) SET n += {k: 'only'} RETURN n.k AS k", nil)
	require.NoError(t, err)
	applied, err := NewStorageExecutor(store).Execute(ctx, "MATCH (n:Person {id: 'b'}) RETURN n.k AS k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"only"}}, applied.Rows)
}

// The whole-entity replacement counterpart of the assignment case above: the
// refusal is the same schema owner's, the reported clause is SET n =, and the
// rejected replacement leaves the stored node untouched.
func TestSetPropagatesUniqueConstraintRefusalForWholeEntityReplacement(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT set_replacement FOR (n:Person) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Person {id: 'a', k: 'taken'}), (:Person {id: 'b', k: 'free'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (n:Person {id: 'b'}) SET n = {k: 'taken'} RETURN n", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SET n =")
	require.Contains(t, err.Error(), "UNIQUE on Person.[k]")
	_, _ = exec.Execute(ctx, "ROLLBACK", nil)

	// The refused replacement was never applied: the stored node is untouched.
	stored, err := NewStorageExecutor(store).Execute(ctx, "MATCH (n:Person {id: 'b'}) RETURN n.k AS k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"free"}}, stored.Rows)

	// Sensitivity control: the same replacement writes when its value does not
	// break the constraint.
	_, err = NewStorageExecutor(store).Execute(ctx, "MATCH (n:Person {id: 'b'}) SET n = {id: 'b', k: 'only'} RETURN n.k AS k", nil)
	require.NoError(t, err)
	applied, err := NewStorageExecutor(store).Execute(ctx, "MATCH (n:Person {id: 'b'}) RETURN n.k AS k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"only"}}, applied.Rows)
}

// A storage-update error the pipeline's SET performs is reported with the
// assignment it came from: the clause's own error context ("SET r = ...")
// wraps the store's error unchanged. This is the COMPONENT half of the
// relationship-whole-entity contract.
//
// It is deliberately a component proof: the store double supplies the update
// error, and a public statement's update is staged inside the transaction, so
// the public binding cannot inject one. The public native test below states
// what the published contract does instead - the transaction validates
// constraints before commit - and observes that real refusal.
func TestComponentSetAttributesStorageUpdateErrorToWholeEntityReplacement(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}})
	require.NoError(t, err)
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: "r1", StartNode: "a", EndNode: "b", Type: "KNOWS", Properties: map[string]any{"since": int64(2020)}}))

	wantErr := errors.New("replace edge failed")
	failing := NewStorageExecutor(&updateErrorEngine{Engine: store, edgeErr: wantErr})
	_, err = failing.executeRequiredPipeline(ctx, "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) SET r = {weight:2} RETURN r")
	require.ErrorIs(t, err, wantErr, "the storage update error is the statement's error")
	require.Contains(t, err.Error(), "SET r =", "the error keeps the assignment it came from")

	// Sensitivity control: the same pipeline replaces the relationship when
	// the store accepts the update, so the assertions above are not satisfied
	// by a statement that never reaches its update.
	healthy := NewStorageExecutor(store)
	_, err = healthy.executeRequiredPipeline(ctx, "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) SET r = {weight:2} RETURN r")
	require.NoError(t, err)
	edges, err := store.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, map[string]any{"weight": int64(2)}, edges[0].Properties)
}

// The public native relationship whole-ENTITY replacement contract, as
// published: constraints are enforced on all write paths (CREATE, MERGE, SET,
// REMOVE), including transactions, and constraint validation happens before
// commit (docs/neo4j-migration/feature-parity.md, "Constraints & Schema" and
// "Consistency"). The transaction owner therefore validates a relationship
// replacement at commit, not at the statement: with SET r = the statement
// itself succeeds and the refusal arrives as the transaction's commit failure.
// Nothing of the refused replacement is stored, and the same replacement with
// a free value is stored. This is an explicitly stated difference from the
// node cases above, whose per-statement refusals name their assignment.
func TestPublicSetWholeEntityReplacementIsRefusedAtCommit(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "set_public_commit")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT set_edge_replacement FOR ()-[r:KNOWS]-() REQUIRE r.k IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Person {name:'Alice'})-[:KNOWS {k:'taken'}]->(:Person {name:'Bob'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Person {name:'Carol'})-[:KNOWS {k:'free'}]->(:Person {name:'Dave'})", nil)
	require.NoError(t, err)

	const refusal = "MATCH (:Person {name:'Carol'})-[r:KNOWS]->(:Person {name:'Dave'}) SET r = {k:'taken'} RETURN r"
	const accepted = "MATCH (:Person {name:'Carol'})-[r:KNOWS]->(:Person {name:'Dave'}) SET r = {k:'fresh'} RETURN r"

	// Auto-commit: the statement's transaction reports the refusal at commit.
	_, err = exec.Execute(ctx, refusal, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed")
	require.Contains(t, err.Error(), "UNIQUE on KNOWS.[k]")
	require.NotContains(t, err.Error(), "SET r =", "the refusal is the transaction's, not the statement's")
	require.Equal(t, [][]interface{}{{"free"}}, relK(t, exec, "Carol", "Dave"),
		"the refused replacement was not stored")

	// Explicit transaction: the SET statement succeeds; COMMIT reports the
	// same refusal and nothing of the transaction is stored.
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	rolledBack := false
	defer func() {
		if !rolledBack {
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
		}
	}()
	_, err = exec.Execute(ctx, refusal, nil)
	require.NoError(t, err, "the replacement is staged; the transaction validates before commit")
	_, err = exec.Execute(ctx, "COMMIT", nil)
	rolledBack = true
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed")
	require.Contains(t, err.Error(), "UNIQUE on KNOWS.[k]")
	require.Equal(t, [][]interface{}{{"free"}}, relK(t, exec, "Carol", "Dave"),
		"the refused transaction stored nothing")

	// Sensitivity control: the same whole-entity replacement with a free value
	// is stored, so the refusals above are not the state of a statement that
	// can never replace this relationship.
	_, err = exec.Execute(ctx, accepted, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"fresh"}}, relK(t, exec, "Carol", "Dave"))
	edges, err := store.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 2, "the accepted replacement did not add or remove a relationship")
}

// relK reads the stored k of the :KNOWS relationship between two named
// people, as one row, so a test can observe a replacement's effect.
func relK(t *testing.T, exec *StorageExecutor, from, to string) [][]interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(),
		"MATCH (:Person {name:'"+from+"'})-[r:KNOWS]->(:Person {name:'"+to+"'}) RETURN r.k AS k", nil)
	require.NoError(t, err)
	return result.Rows
}
