package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestOptionalMatchFiltersBoundRelationshipAndPreservesMiss(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "optional_relationship_filter"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:A {num: 1})-[:REL {name: 'r1'}]->(b:B {num: 2})-[:REL {name: 'r2'}]->(c:C {num: 3})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a)-[r {name: 'r1'}]-(b) OPTIONAL MATCH (b)-[r2]-(c) WHERE r <> r2 RETURN a, b, c", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	matched := 0
	for _, row := range result.Rows {
		if row[2] != nil {
			matched++
		}
	}
	require.Equal(t, 1, matched)
}

func TestMandatoryMatchDropsCoalescedNullNodeBinding(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "mandatory_coalesced_null"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Single)-[:REL]->(:A)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:Single) OPTIONAL MATCH (a)-->(b:Missing) OPTIONAL MATCH (a)-->(c:Missing) WITH coalesce(b, c) AS x MATCH (x)-->(d) RETURN d", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}

func TestOptionalMatchReturnsUndirectedSelfRelationshipOnce(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "optional_self_relationship"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:B)", nil)
	require.NoError(t, err)
	nodes, err := exec.storage.GetNodesByLabel("B")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.NoError(t, exec.storage.CreateEdge(&storage.Edge{ID: "loop", Type: "LOOP", StartNode: nodes[0].ID, EndNode: nodes[0].ID}))

	result, err := exec.Execute(ctx, "MATCH (a:B) OPTIONAL MATCH (a)-[r]-(a) RETURN r", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.NotNil(t, result.Rows[0][0])
}

func TestGH447_UnwindOptionalMatchCorrelationMatrix(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		columns  []string
		expected [][]interface{}
	}{
		{
			name:     "gh-447-inline-property-expression",
			query:    "UNWIND [1, 2, 3] AS k OPTIONAL MATCH (t:OM {k: k}) RETURN k, t.k AS tk",
			columns:  []string{"k", "tk"},
			expected: [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(2)}, {int64(3), nil}},
		},
		{
			name:     "gh-447-where-predicate",
			query:    "UNWIND [1, 2, 3] AS k OPTIONAL MATCH (t:OM) WHERE t.k = k RETURN k, t.k AS tk",
			columns:  []string{"k", "tk"},
			expected: [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(2)}, {int64(3), nil}},
		},
		{
			name:     "gh-447-null-projection",
			query:    "UNWIND [1, 2, 3] AS k OPTIONAL MATCH (t:OM {k: k}) RETURN k, t IS NULL AS missing",
			columns:  []string{"k", "missing"},
			expected: [][]interface{}{{int64(1), false}, {int64(2), false}, {int64(3), true}},
		},
		{
			name:     "gh-447-null-anti-join",
			query:    "UNWIND [1, 2, 3] AS k OPTIONAL MATCH (t:OM {k: k}) WITH k, t WHERE t IS NULL RETURN k",
			columns:  []string{"k"},
			expected: [][]interface{}{{int64(3)}},
		},
		{
			name:     "gh-447-grouped-optional-count",
			query:    "UNWIND [1, 2, 3] AS k OPTIONAL MATCH (t:OM {k: k}) WITH k, count(t) AS c RETURN k, c",
			columns:  []string{"k", "c"},
			expected: [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}, {int64(3), int64(0)}},
		},
		{
			name:     "gh-447-standalone-optional-miss",
			query:    "OPTIONAL MATCH (t:OM {k: 3}) RETURN t IS NULL AS missing",
			columns:  []string{"missing"},
			expected: [][]interface{}{{true}},
		},
		{
			name:     "gh-447-matched-node-is-not-null",
			query:    "MATCH (t:OM) RETURN t.k AS k, t.k IS NULL AS isnull ORDER BY k",
			columns:  []string{"k", "isnull"},
			expected: [][]interface{}{{int64(1), false}, {int64(2), false}},
		},
	}

	for _, mode := range []struct {
		name     string
		explicit bool
	}{
		{name: "autocommit"},
		{name: "explicit transaction", explicit: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "optional_unwind_correlation"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:OM {k: 1}), (:OM {k: 2})", nil)
			require.NoError(t, err)
			if mode.explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, test.query, nil)
					require.NoError(t, err)
					require.Equal(t, test.columns, result.Columns)
					require.Equal(t, test.expected, result.Rows)
				})
			}
		})
	}
}
