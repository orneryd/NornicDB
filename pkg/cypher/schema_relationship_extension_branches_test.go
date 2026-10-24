package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateConstraint_RelationshipCardinalityAndPolicyBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_rel_ext_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []string{
		"CREATE CONSTRAINT rel_out_named IF NOT EXISTS FOR ()-[r:KNOWS]->() REQUIRE MAX COUNT 2",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:LIKES]->() REQUIRE MAX COUNT 3",
		"CREATE CONSTRAINT rel_in_named IF NOT EXISTS FOR ()<-[r:FOLLOWS]-() REQUIRE MAX COUNT 4",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()<-[r:MENTORS]-() REQUIRE MAX COUNT 5",
		"CREATE CONSTRAINT pol_allow_named IF NOT EXISTS FOR (:User)-[r:CAN_ACCESS]->(:Doc) REQUIRE ALLOWED",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (:Team)-[r:CAN_EDIT]->(:Doc) REQUIRE ALLOWED",
		"CREATE CONSTRAINT pol_deny_named IF NOT EXISTS FOR (:User)-[r:CANNOT_ACCESS]->(:Secret) REQUIRE DISALLOWED",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (:Team)-[r:CANNOT_EDIT]->(:Secret) REQUIRE DISALLOWED",
	}
	for _, q := range queries {
		fixtureAdmission1430Context, fixtureAdmission1430 := statementTestContext(ctx, q)
		_ = fixtureAdmission1430Context
		_, err := exec.executeCreateConstraint(fixtureAdmission1430, fixtureAdmission1430Context, q)
		require.NoError(t, err, q)
	}

	// Deterministically assert persisted constraint shapes.
	all := store.GetSchema().GetAllConstraints()
	byName := map[string]storage.Constraint{}
	for _, c := range all {
		byName[c.Name] = c
	}

	c := byName["rel_out_named"]
	require.Equal(t, storage.ConstraintCardinality, c.Type)
	require.Equal(t, storage.ConstraintEntityRelationship, c.EffectiveEntityType())
	require.Equal(t, "KNOWS", c.Label)
	require.Equal(t, "OUTGOING", c.Direction)
	require.Equal(t, 2, c.MaxCount)

	c = byName["constraint_LIKES_max_outgoing_3"]
	require.Equal(t, storage.ConstraintCardinality, c.Type)
	require.Equal(t, "LIKES", c.Label)
	require.Equal(t, "OUTGOING", c.Direction)
	require.Equal(t, 3, c.MaxCount)

	c = byName["rel_in_named"]
	require.Equal(t, storage.ConstraintCardinality, c.Type)
	require.Equal(t, "INCOMING", c.Direction)
	require.Equal(t, 4, c.MaxCount)

	c = byName["constraint_MENTORS_max_incoming_5"]
	require.Equal(t, storage.ConstraintCardinality, c.Type)
	require.Equal(t, "INCOMING", c.Direction)
	require.Equal(t, 5, c.MaxCount)

	c = byName["pol_allow_named"]
	require.Equal(t, storage.ConstraintPolicy, c.Type)
	require.Equal(t, "CAN_ACCESS", c.Label)
	require.Equal(t, "User", c.SourceLabel)
	require.Equal(t, "Doc", c.TargetLabel)
	require.Equal(t, "ALLOWED", c.PolicyMode)

	c = byName["constraint_Team_CAN_EDIT_Doc_allowed"]
	require.Equal(t, storage.ConstraintPolicy, c.Type)
	require.Equal(t, "ALLOWED", c.PolicyMode)
	require.Equal(t, "Team", c.SourceLabel)
	require.Equal(t, "Doc", c.TargetLabel)

	c = byName["pol_deny_named"]
	require.Equal(t, storage.ConstraintPolicy, c.Type)
	require.Equal(t, "DISALLOWED", c.PolicyMode)
	require.Equal(t, "User", c.SourceLabel)
	require.Equal(t, "Secret", c.TargetLabel)

	c = byName["constraint_Team_CANNOT_EDIT_Secret_disallowed"]
	require.Equal(t, storage.ConstraintPolicy, c.Type)
	require.Equal(t, "DISALLOWED", c.PolicyMode)
}

func TestCreateConstraint_RelationshipCardinalityInvalidCount(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_rel_ext_err"))
	ctx := context.Background()

	fixtureAdmission1431Context, fixtureAdmission1431 := statementTestContext(ctx, "CREATE CONSTRAINT bad_out IF NOT EXISTS FOR ()-[r:KNOWS]->() REQUIRE MAX COUNT 0")
	_ = fixtureAdmission1431Context
	_, err := exec.executeCreateConstraint(fixtureAdmission1431, fixtureAdmission1431Context, "CREATE CONSTRAINT bad_out IF NOT EXISTS FOR ()-[r:KNOWS]->() REQUIRE MAX COUNT 0")
	require.Error(t, err)
	require.Contains(t, err.Error(), "MAX COUNT must be a positive integer")

	fixtureAdmission1432Context, fixtureAdmission1432 := statementTestContext(ctx, "CREATE CONSTRAINT bad_in IF NOT EXISTS FOR ()<-[r:KNOWS]-() REQUIRE MAX COUNT -1")
	_ = fixtureAdmission1432Context
	_, err = exec.executeCreateConstraint(fixtureAdmission1432, fixtureAdmission1432Context, "CREATE CONSTRAINT bad_in IF NOT EXISTS FOR ()<-[r:KNOWS]-() REQUIRE MAX COUNT -1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cardinality require clause")
}
