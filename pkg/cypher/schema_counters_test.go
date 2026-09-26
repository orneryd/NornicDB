package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSchemaCommandCounters pins the summary counters of schema commands to
// Neo4j 5.26.30 (#507): a CREATE / DROP reports the index or constraint it
// created or dropped, a constraint's own index isn't counted as an index, and
// an IF [NOT] EXISTS that did nothing reports nothing.
func TestSchemaCommandCounters(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema507"))
	ctx := context.Background()
	steps := []struct {
		query string
		want  QueryStats
	}{
		{"CREATE INDEX sc_ix FOR (n:SC507) ON (n.a)", QueryStats{IndexesAdded: 1}},
		{"CREATE INDEX sc_ix IF NOT EXISTS FOR (n:SC507) ON (n.a)", QueryStats{}},
		{"CREATE INDEX sc_ix2 FOR (n:SC507) ON (n.c, n.d)", QueryStats{IndexesAdded: 1}},
		{"CREATE RANGE INDEX sc_rx FOR (n:SC507) ON (n.r)", QueryStats{IndexesAdded: 1}},
		{"CREATE INDEX sc_rel FOR ()-[r:SCR507]-() ON (r.w)", QueryStats{IndexesAdded: 1}},
		{"CREATE FULLTEXT INDEX sc_ft FOR (n:SC507) ON EACH [n.b]", QueryStats{IndexesAdded: 1}},
		{"CREATE VECTOR INDEX sc_vec FOR (n:SC507) ON (n.v) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", QueryStats{IndexesAdded: 1}},
		{"CREATE CONSTRAINT sc_uq FOR (n:SC507) REQUIRE n.k IS UNIQUE", QueryStats{ConstraintsAdded: 1}},
		{"CREATE CONSTRAINT sc_uq IF NOT EXISTS FOR (n:SC507) REQUIRE n.k IS UNIQUE", QueryStats{}},
		{"CREATE CONSTRAINT sc_ex FOR (n:SC507) REQUIRE n.e IS NOT NULL", QueryStats{ConstraintsAdded: 1}},
		{"CREATE CONSTRAINT sc_ty FOR (n:SC507) REQUIRE n.t IS :: INTEGER", QueryStats{ConstraintsAdded: 1}},
		{"DROP CONSTRAINT sc_uq", QueryStats{ConstraintsRemoved: 1}},
		{"DROP CONSTRAINT sc_ex", QueryStats{ConstraintsRemoved: 1}},
		{"DROP CONSTRAINT sc_ty", QueryStats{ConstraintsRemoved: 1}},
		{"DROP CONSTRAINT sc_uq IF EXISTS", QueryStats{}},
		{"DROP INDEX sc_ix", QueryStats{IndexesRemoved: 1}},
		{"DROP INDEX sc_ix2", QueryStats{IndexesRemoved: 1}},
		{"DROP INDEX sc_rx", QueryStats{IndexesRemoved: 1}},
		{"DROP INDEX sc_rel", QueryStats{IndexesRemoved: 1}},
		{"DROP INDEX sc_ft", QueryStats{IndexesRemoved: 1}},
		{"DROP INDEX sc_vec", QueryStats{IndexesRemoved: 1}},
		{"DROP INDEX sc_ix IF EXISTS", QueryStats{}},
	}
	for _, step := range steps {
		result, err := executor.Execute(ctx, step.query, nil)
		require.NoError(t, err, step.query)
		got := QueryStats{}
		if result.Stats != nil {
			got = *result.Stats
		}
		require.Equal(t, step.want, got, step.query)
	}
}

// TestRemoveLabelsCounted verifies REMOVE reports the labels it removed as
// labels_removed, as Neo4j does, on the MATCH ... REMOVE route and the
// pipeline route alike (#507).
func TestRemoveLabelsCounted(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:RL507) REMOVE n:A:B:Missing",
		"MATCH (n:RL507) WITH n REMOVE n:A:B:Missing",
	} {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "remove507"))
		ctx := context.Background()
		_, err := executor.Execute(ctx, "CREATE (:RL507:A:B {p: 1}), (:RL507:A)", nil)
		require.NoError(t, err)
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, 3, result.Stats.LabelsRemoved, query)
		require.Equal(t, 0, result.Stats.PropertiesSet, query)
	}
}
