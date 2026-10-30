package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestUniqueConstraints_DoNotRegisterAsConstraintContracts asserts a load-bearing
// invariant of the constraint-contract validation path: a schema that declares
// only single-property `IS UNIQUE` constraints via `CREATE CONSTRAINT` DDL must
// NOT show up as having any `ConstraintContract` registered.
//
// `validateConstraintContracts` in `pkg/storage/constraint_contracts.go` has an
// early-out at the top of the function: if `transactionTouchesConstraintContracts`
// reports zero contracts across every namespace the transaction touches, the
// expensive per-affected-node adjacency walk is skipped. That early-out reads
// `SchemaManager.HasAnyConstraintContract()` per namespace. If standard
// uniqueness DDL leaked into the contracts map, every commit would pay the
// adjacency-walk cost on workloads that have no boolean predicates to validate
// at all.
//
// Uniqueness constraints belong in `sm.constraints` and `sm.uniqueConstraints`;
// `sm.constraintContracts` is reserved for the block-form `REQUIRE { ... }`
// syntax handled by `executeCreateConstraintContract` in `schema_contracts.go`.
// This test pins that separation. If the assertion fires, somewhere in the
// `CREATE CONSTRAINT ... REQUIRE n.X IS UNIQUE` execution path a contract is
// being registered when it should not be, and the adjacency-walk early-out
// will fail to fire on uniqueness-only workloads.
func TestUniqueConstraints_DoNotRegisterAsConstraintContracts(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Register a representative set of single-property uniqueness constraints
	// matching a typed Neo4j-canonical Cypher client's stable-key schema.
	uniqueConstraints := []string{
		"CREATE CONSTRAINT function_uid_unique IF NOT EXISTS FOR (n:Function) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE",
		"CREATE CONSTRAINT variable_uid_unique IF NOT EXISTS FOR (n:Variable) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT struct_uid_unique IF NOT EXISTS FOR (n:Struct) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT interface_uid_unique IF NOT EXISTS FOR (n:Interface) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT annotation_uid_unique IF NOT EXISTS FOR (n:Annotation) REQUIRE n.uid IS UNIQUE",
	}
	for _, stmt := range uniqueConstraints {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err, "constraint registration must succeed: %s", stmt)
	}

	schema := baseStore.GetSchemaForNamespace("test")
	require.NotNil(t, schema, "schema for namespace 'test' must exist after constraint registration")

	require.False(t, schema.HasAnyConstraintContract(),
		"single-property `IS UNIQUE` constraints must NOT register as `ConstraintContract`s; "+
			"otherwise `transactionTouchesConstraintContracts` returns true and "+
			"`validateConstraintContracts` activates the per-affected-node adjacency walk on every commit")
}

// TestUnwindMergeCanonicalWrite_DoesNotLeakConstraintContracts is the
// integration form of the above. It runs the full sequence a canonical
// entity-write client performs against the executor — register uniqueness
// constraints, seed a `File` anchor, execute a representative UNWIND-MERGE
// batch — and asserts no `ConstraintContract` has been registered at any
// point along the way.
//
// If the canonical-write batch itself registers contracts (for example
// through some implicit DDL inside the batch, or through a property-type
// inference path on the cypher execution), this is where it shows up. The
// test is intentionally a behavior-level integration test rather than a
// `SchemaManager`-only unit test so that it catches regressions in any
// layer between cypher execution and schema mutation.
func TestUnwindMergeCanonicalWrite_DoesNotLeakConstraintContracts(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, stmt := range []string{
		"CREATE CONSTRAINT function_uid_unique IF NOT EXISTS FOR (n:Function) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}

	schema := baseStore.GetSchemaForNamespace("test")
	require.False(t, schema.HasAnyConstraintContract(),
		"after constraint registration only, no `ConstraintContract`s should exist")

	_, err := exec.Execute(ctx, "CREATE (:File {path: 'src/a.go'})", nil)
	require.NoError(t, err)

	require.False(t, schema.HasAnyConstraintContract(),
		"after seeding a `File` node, no `ConstraintContract`s should exist")

	_, err = exec.Execute(ctx,
		`UNWIND $rows AS row
		 MATCH (f:File {path: row.file_path})
		 MERGE (n:Function {uid: row.entity_id})
		 SET n += row.props
		 MERGE (f)-[:CONTAINS]->(n)`,
		map[string]interface{}{
			"rows": []map[string]interface{}{
				{
					"entity_id": "function:src/a.go:foo",
					"file_path": "src/a.go",
					"props":     map[string]interface{}{"name": "foo"},
				},
			},
		})
	require.NoError(t, err)

	require.False(t, schema.HasAnyConstraintContract(),
		"after a canonical entity-write UNWIND-MERGE batch, no `ConstraintContract`s should exist; "+
			"if this fires, the canonical-write cypher itself is registering contracts implicitly")
}
