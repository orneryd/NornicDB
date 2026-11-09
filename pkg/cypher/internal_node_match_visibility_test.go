package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchExcludesInternalNodes(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "visibility"))
	for _, q := range []string{
		"CREATE (:WrapperRecord:_internal {id: 1}), (:WrapperRecord {id: 2})",
		"CREATE INDEX wrapper_id FOR (n:WrapperRecord) ON (n.id)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
	for _, q := range []string{
		"MATCH (n:WrapperRecord) RETURN n.id AS id ORDER BY n.id",
		"MATCH (n:WrapperRecord) WHERE n.id >= 0 RETURN n.id AS id ORDER BY n.id LIMIT 1",
		"MATCH (n:WrapperRecord {id: 1}) WITH n MATCH (n:WrapperRecord) RETURN n.id AS id",
	} {
		t.Run(q, func(t *testing.T) {
			r, err := exec.Execute(ctx, q, nil)
			require.NoError(t, err)
			if q == "MATCH (n:WrapperRecord {id: 1}) WITH n MATCH (n:WrapperRecord) RETURN n.id AS id" {
				require.Empty(t, r.Rows)
			} else {
				require.Equal(t, [][]interface{}{{int64(2)}}, r.Rows)
			}
		})
	}
}

// internalMatchFixture is the population every MATCH route below reads: one
// label whose internal nodes (labels starting with "_") sort around the visible
// ones, and the id index the indexed seeds need. Ordered by id the population
// is 1 (internal), 2, 3, 5 (internal), 6, so a page that counted hidden nodes
// returns the wrong rows rather than only fewer.
func internalMatchFixture(t *testing.T, ctx context.Context, exec *StorageExecutor) {
	t.Helper()
	for _, statement := range []string{
		`CREATE (:Nor177Record:_internal {id: 1}), (:Nor177Record {id: 2}),
			(:Nor177Record {id: 3}), (:Nor177Record:_internal {id: 5}),
			(:Nor177Record {id: 6})`,
		"CREATE INDEX nor177_record_id FOR (n:Nor177Record) ON (n.id)",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
}

// TestMatchExcludesInternalNodesAcrossParserConfigurations covers the routes
// that admit a node into a MATCH row: the label scan, the indexed seeds (an
// inline pattern property, an equality, an IN list, a range, and the ordered
// index window LIMIT/SKIP read from), the fused indexed count, and a row bound
// earlier in the statement. Each parser configuration answers every route, so
// neither grammar can reintroduce a hidden row.
func TestMatchExcludesInternalNodesAcrossParserConfigurations(t *testing.T) {
	ctx := context.Background()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor177"))
			internalMatchFixture(t, ctx, exec)

			for _, tc := range []struct {
				query string
				rows  [][]interface{}
			}{
				// The label scan.
				{"MATCH (n:Nor177Record) RETURN n.id AS id ORDER BY n.id",
					[][]interface{}{{int64(2)}, {int64(3)}, {int64(6)}}},
				// The ordered index window: LIMIT keeps the first visible
				// rows, not the first indexed ones.
				{"MATCH (n:Nor177Record) WHERE n.id >= 0 RETURN n.id AS id ORDER BY n.id LIMIT 1",
					[][]interface{}{{int64(2)}}},
				{"MATCH (n:Nor177Record) WHERE n.id >= 0 RETURN n.id AS id ORDER BY n.id LIMIT 2",
					[][]interface{}{{int64(2)}, {int64(3)}}},
				{"MATCH (n:Nor177Record) WHERE n.id >= 0 RETURN n.id AS id ORDER BY n.id SKIP 1 LIMIT 1",
					[][]interface{}{{int64(3)}}},
				{"MATCH (n:Nor177Record) WHERE n.id >= 0 RETURN n.id AS id ORDER BY n.id SKIP 2 LIMIT 2",
					[][]interface{}{{int64(6)}}},
				{"MATCH (n:Nor177Record) WHERE n.id >= 0 RETURN n.id AS id ORDER BY n.id DESC LIMIT 1",
					[][]interface{}{{int64(6)}}},
				{"MATCH (n:Nor177Record) WHERE n.id >= 0 RETURN n.id AS id ORDER BY n.id DESC LIMIT 2",
					[][]interface{}{{int64(6)}, {int64(3)}}},
				// The indexed equality and range seeds.
				{"MATCH (n:Nor177Record {id: 1}) RETURN n.id AS id", [][]interface{}{}},
				{"MATCH (n:Nor177Record {id: 2}) RETURN n.id AS id",
					[][]interface{}{{int64(2)}}},
				{"MATCH (n:Nor177Record {id: 5}) RETURN n.id AS id", [][]interface{}{}},
				{"MATCH (n:Nor177Record) WHERE n.id = 1 RETURN n.id AS id", [][]interface{}{}},
				{"MATCH (n:Nor177Record) WHERE n.id IN [1, 5, 6] RETURN n.id AS id",
					[][]interface{}{{int64(6)}}},
				// The fused indexed count reads the same seed as the rows.
				{"MATCH (n:Nor177Record) WHERE n.id >= 1 RETURN count(n) AS c",
					[][]interface{}{{int64(3)}}},
				// A row bound earlier carries the same visibility as the seed
				// that produced it.
				{"MATCH (n:Nor177Record {id: 1}) WITH n MATCH (n:Nor177Record) RETURN n.id AS id", [][]interface{}{}},
				{"MATCH (n:Nor177Record {id: 2}) WITH n MATCH (n:Nor177Record) RETURN n.id AS id",
					[][]interface{}{{int64(2)}}},
				{"MATCH (n:Nor177Record {id: 5}) WITH n MATCH (n:Nor177Record) RETURN n.id AS id", [][]interface{}{}},
			} {
				t.Run(tc.query, func(t *testing.T) {
					result, err := exec.Execute(ctx, tc.query, nil)
					require.NoError(t, err, tc.query)
					require.Equal(t, tc.rows, result.Rows, tc.query)
				})
			}
		})
	}
}

// TestInternalNodeVisibilityKeepsSystemNamespacePrivileged pins the one
// visibility decision behind every MATCH admission: internal nodes are hidden
// on a user database and shown on the system database, which holds NornicDB's
// own records. Statements cannot read a graph on the system database
// (systemDatabaseStatementError), so the privileged side is asserted on the
// decision and on the admission test the internal read routes use.
func TestInternalNodeVisibilityKeepsSystemNamespacePrivileged(t *testing.T) {
	ctx := context.Background()
	require.False(t, shouldHideSystemNodes(storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")))
	require.True(t, shouldHideSystemNodes(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor177")))
	require.True(t, shouldHideSystemNodes(newTestMemoryEngine(t)))

	internal := &storage.Node{
		ID:         "nor177:hidden",
		Labels:     []string{"Nor177Record", "_internal"},
		Properties: map[string]interface{}{"id": int64(1)},
	}
	pattern := nodePatternInfo{variable: "n", labels: []string{"Nor177Record"}}
	system := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "system"))
	require.True(t, system.pipelineNodeMatchesPattern(ctx, internal, pattern))

	user := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor177"))
	require.False(t, user.pipelineNodeMatchesPattern(ctx, internal, pattern))
}

// TestQueryNodeReadKeepsInternalNodesForWriters pins the boundary the MATCH
// admission relies on: hiding happens when a MATCH row is admitted, never in
// the shared node read. The internal writers that own internal nodes (schema,
// user records, database metadata) read them through that read and MERGE-style
// lookups, which must keep returning them.
func TestQueryNodeReadKeepsInternalNodesForWriters(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor177")
	exec := NewStorageExecutor(store)
	internal := &storage.Node{
		ID:         "nor177:hidden",
		Labels:     []string{"Nor177Record", "_internal"},
		Properties: map[string]interface{}{"id": int64(1)},
	}
	_, err := store.CreateNode(ctx, internal)
	require.NoError(t, err)

	read, err := exec.getQueryNode(ctx, exec.storage, internal.ID)
	require.NoError(t, err)
	require.NotNil(t, read)
	require.Equal(t, int64(1), read.Properties["id"])
	require.True(t, isSystemNode(read))
}

func TestOrderedIndexRequiresEveryPatternLabel(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "multilabel"))
	for _, q := range []string{
		"CREATE INDEX FOR (n:GraphNode) ON (n.id)",
		"CREATE (:GraphNode:Document {id:'d0'}), (:GraphNode:Document {id:'d1'}), (:GraphNode:Entity {id:'m0'}), (:GraphNode:Entity {id:'m1'})",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
	result, err := exec.Execute(ctx, "MATCH (m:GraphNode:Entity) WHERE m.id > '' RETURN m.id ORDER BY m.id LIMIT 2", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"m0"}, {"m1"}}, result.Rows)
}
