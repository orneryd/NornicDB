package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A uniqueness constraint created over existing nodes fills the property index
// it owns, so seeks use it (#875); uniqueness is still enforced, SHOW INDEXES
// lists the index once, and DROP CONSTRAINT drops it.
func TestGh875ConstraintIndexFilledAndUsed(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "UNWIND range(1, 5) AS i CREATE (:Rec {uid: 'u' + toString(i)})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT rec_uid FOR (n:Rec) REQUIRE n.uid IS UNIQUE", nil)
	require.NoError(t, err)

	schema := exec.storage.GetSchema()
	require.True(t, schema.HasPropertyIndex("Rec", "uid"))
	require.Len(t, schema.PropertyIndexLookup("Rec", "uid", "u3"), 1)

	result, err := exec.Execute(ctx, "MATCH (n:Rec) WHERE n.uid IN ['u2', 'u4', 'none'] RETURN n.uid ORDER BY n.uid", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"u2"}, {"u4"}}, result.Rows)

	_, err = exec.Execute(ctx, "CREATE (:Rec {uid: 'u1'})", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Schema.ConstraintValidationFailed")

	result, err = exec.Execute(ctx, "SHOW INDEXES YIELD name WHERE name = 'rec_uid' RETURN count(*)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Rows[0][0])

	_, err = exec.Execute(ctx, "DROP CONSTRAINT rec_uid", nil)
	require.NoError(t, err)
	require.False(t, schema.MaintainsPropertyIndex("Rec", "uid"))
	result, err = exec.Execute(ctx, "MATCH (n:Rec {uid: 'u3'}) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Rows[0][0])
}

// Inside an explicit transaction a seek over the constraint's index sees the
// transaction's own writes, and MERGE matches instead of creating.
func TestGh875ConstraintIndexSeekInExplicitTransaction(t *testing.T) {
	exec := newAsyncStackExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT rec_uid FOR (n:Rec) REQUIRE n.uid IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Rec {uid: 'stored', v: 0})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Rec {uid: 'own', v: 1})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (n:Rec) WHERE n.uid IN ['own', 'stored'] RETURN n.uid ORDER BY n.uid", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"own"}, {"stored"}}, result.Rows)
	_, err = exec.Execute(ctx, "MERGE (n:Rec {uid: 'own'}) SET n.v = 2", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:Rec {uid: 'own'}) RETURN count(n), max(n.v)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}}, result.Rows)
}

// Neo4j's rules for an index and a uniqueness constraint on one property
// (#884).
func TestGh884IndexAndConstraintOverlap(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT c884a FOR (n:T884) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE INDEX i884a FOR (n:T884) ON (n.id)", nil)
	require.Equal(t, "Neo.ClientError.Schema.ConstraintAlreadyExists: There is a uniqueness constraint on (:T884 {id}), so an index is already created that matches this.", statusText(err))
	_, err = exec.Execute(ctx, "CREATE INDEX i884c IF NOT EXISTS FOR (n:T884) ON (n.id)", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE INDEX i884b FOR (n:U884) ON (n.id)", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"CREATE CONSTRAINT c884b FOR (n:U884) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT c884c IF NOT EXISTS FOR (n:U884) REQUIRE n.id IS UNIQUE",
	} {
		_, err = exec.Execute(ctx, query, nil)
		require.Equal(t, "Neo.ClientError.Schema.IndexAlreadyExists: There already exists an index (:U884 {id}). A constraint cannot be created until the index has been dropped.", statusText(err), query)
	}

	_, err = exec.Execute(ctx, "DROP INDEX c884a", nil)
	require.Equal(t, "Neo.DatabaseError.Schema.IndexDropFailed: Unable to drop index: Index belongs to constraint: `c884a`", statusText(err))

	result, err := exec.Execute(ctx, "SHOW INDEXES YIELD name WHERE name STARTS WITH 'c884' OR name STARTS WITH 'i884' RETURN name ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"c884a"}, {"i884b"}}, result.Rows)
}

// A constraint whose index can't be filled isn't created.
func TestGh875ConstraintIndexFillFailureDropsConstraint(t *testing.T) {
	base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(&getNodesByLabelErrEngine{Engine: base, label: "Rec", err: errors.New("label scan failed")})
	_, err := exec.Execute(context.Background(), "CREATE CONSTRAINT rec_uid FOR (n:Rec) REQUIRE n.uid IS UNIQUE", nil)
	require.ErrorContains(t, err, "label scan failed")
	require.Empty(t, base.GetSchema().GetAllConstraints())
	require.False(t, base.GetSchema().MaintainsPropertyIndex("Rec", "uid"))
}
