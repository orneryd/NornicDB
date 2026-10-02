package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResidualRepeatedUniqueConstraintProperties(t *testing.T) {
	for _, query := range []string{
		"CREATE CONSTRAINT cu FOR (n:P) REQUIRE (n.a, n.a) IS UNIQUE",
		"CREATE CONSTRAINT cu FOR (n:P) REQUIRE (n.a, n.`a`) IS UNIQUE",
	} {
		t.Run(query, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Schema.RepeatedPropertyInCompositeSchema", code)
			result, err := exec.Execute(ctx, "SHOW CONSTRAINTS", nil)
			require.NoError(t, err)
			require.Empty(t, result.Rows)
		})
	}
}

func TestCreateConstraint_BranchMatrix_NodeAndRelationshipVariants(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_constraint_matrix_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []string{
		// Node domain + temporal
		"CREATE CONSTRAINT c_node_domain_named IF NOT EXISTS FOR (n:NodeDomA) REQUIRE n.status IN ['A','B']",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NodeDomB) REQUIRE n.status IN ['C','D']",
		"CREATE CONSTRAINT c_node_temp_named IF NOT EXISTS FOR (n:NodeTempA) REQUIRE (n.k, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NodeTempB) REQUIRE (n.k, n.valid_from, n.valid_to) IS TEMPORAL",

		// Node exists/not-null + type variants
		"CREATE CONSTRAINT c_node_exists_named IF NOT EXISTS FOR (n:NodeExistsA) REQUIRE n.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NodeExistsB) REQUIRE n.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NodeExistsC) ASSERT exists(n.email)",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NodeExistsD) ASSERT n.email IS NOT NULL",
		"CREATE CONSTRAINT c_node_type_named IF NOT EXISTS FOR (n:NodeTypeA) REQUIRE n.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NodeTypeB) REQUIRE n.age IS TYPED INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NodeTypeC) ASSERT n.age IS :: INTEGER",

		// Relationship domain + temporal
		"CREATE CONSTRAINT c_rel_domain_named IF NOT EXISTS FOR ()-[r:RELDOMA]-() REQUIRE r.state IN ['hot','cold']",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELDOMB]-() REQUIRE r.state IN ['warm','cool']",
		"CREATE CONSTRAINT c_rel_temp_named IF NOT EXISTS FOR ()-[r:RELTEMPA]-() REQUIRE (r.k, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELTEMPB]-() REQUIRE (r.k, r.valid_from, r.valid_to) IS TEMPORAL",

		// Relationship key + unique + exists + type
		"CREATE CONSTRAINT c_rel_key_named IF NOT EXISTS FOR ()-[r:RELKEYA]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELKEYB]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT c_rel_key_single_named IF NOT EXISTS FOR ()-[r:RELKEYC]-() REQUIRE r.k IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELKEYD]-() REQUIRE r.k IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT c_rel_unique_composite_named IF NOT EXISTS FOR ()-[r:RELUNQA]-() REQUIRE (r.a, r.b) IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELUNQB]-() REQUIRE (r.a, r.b) IS UNIQUE",
		"CREATE CONSTRAINT c_rel_unique_single_named IF NOT EXISTS FOR ()-[r:RELUNQC]-() REQUIRE r.k IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELUNQD]-() REQUIRE r.k IS UNIQUE",
		"CREATE CONSTRAINT c_rel_exists_named IF NOT EXISTS FOR ()-[r:RELEXA]-() REQUIRE r.k IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELEXB]-() REQUIRE r.k IS NOT NULL",
		"CREATE CONSTRAINT c_rel_type_named IF NOT EXISTS FOR ()-[r:RELTYPEA]-() REQUIRE r.ts IS :: ZONED DATETIME",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELTYPEB]-() REQUIRE r.ts IS :: ZONED DATETIME",
	}

	for _, q := range queries {
		_, err := exec.executeCreateConstraint(ctx, q)
		require.NoError(t, err, q)
	}

	constraints := store.GetSchema().GetAllConstraints()
	require.NotEmpty(t, constraints)
}

func TestCreateConstraint_BranchMatrix_DuplicateAndErrorPaths(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_constraint_matrix_err")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Duplicate name with different shape must surface conflict.
	_, err := exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT dup_name_collision FOR ()-[r:DUPREL]-() REQUIRE r.k IS UNIQUE")
	require.NoError(t, err)
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT dup_name_collision FOR ()-[r:DUPREL]-() REQUIRE r.other IS UNIQUE")
	require.Error(t, err)

	// Temporal arity failures.
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_temporal_node FOR (n:BadTemporal) REQUIRE (n.k, n.valid_from) IS TEMPORAL")
	require.Error(t, err)
	require.Contains(t, err.Error(), "TEMPORAL constraint")

	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_temporal_rel FOR ()-[r:BadTemporalRel]-() REQUIRE (r.k, r.valid_from) IS TEMPORAL")
	require.Error(t, err)
	require.Contains(t, err.Error(), "TEMPORAL constraint")

	// Relationship KEY with empty property tuple should fail syntax matching and error deterministically.
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_rel_key FOR ()-[r:BadRelKey]-() REQUIRE () IS RELATIONSHIP KEY")
	require.Error(t, err)
}
