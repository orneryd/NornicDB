package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type getNodesByLabelErrEngine struct {
	storage.Engine
	label string
	err   error
}

func (e *getNodesByLabelErrEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if label == e.label {
		return nil, e.err
	}
	return e.Engine.GetNodesByLabel(label)
}

type createEdgeErrEngine struct {
	storage.Engine
	err error
}

func (e *createEdgeErrEngine) CreateEdge(edge *storage.Edge) error {
	if e.err != nil {
		return e.err
	}
	return e.Engine.CreateEdge(edge)
}

func TestExecuteUnwindFixedChainLinkBatch_ErrorBranches(t *testing.T) {
	ctx := context.Background()
	rest := `
MATCH (o:OriginalText {textKey: row.textKey})
MATCH (h1:Hop {hopId: row.textKey + ':1'})
MATCH (h2:Hop {hopId: row.textKey + ':2'})
MERGE (o)-[:NEXT]->(h1)
MERGE (h1)-[:NEXT]->(h2)
RETURN count(o) AS linked
`
	items := []interface{}{map[string]interface{}{"textKey": "a"}}

	t.Run("root_label_lookup_error", func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "fixed_chain_root_err")
		exec := NewStorageExecutor(&getNodesByLabelErrEngine{Engine: base, label: "OriginalText", err: errors.New("root label boom")})
		_, supported, err := exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, rest)
		require.Error(t, err)
		require.True(t, supported)
		require.Contains(t, err.Error(), "root label boom")
	})

	t.Run("hop_label_lookup_error", func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "fixed_chain_hop_err")
		_, err := base.CreateNode(&storage.Node{ID: storage.NodeID("orig-a"), Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"textKey": "a"}})
		require.NoError(t, err)
		exec := NewStorageExecutor(&getNodesByLabelErrEngine{Engine: base, label: "Hop", err: errors.New("hop label boom")})
		_, supported, err := exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, rest)
		require.Error(t, err)
		require.True(t, supported)
		require.Contains(t, err.Error(), "hop label boom")
	})

	t.Run("merge_hop_create_edge_error", func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "fixed_chain_edge_err")
		_, err := base.CreateNode(&storage.Node{ID: storage.NodeID("orig-a"), Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"textKey": "a"}})
		require.NoError(t, err)
		for depth := 1; depth <= 2; depth++ {
			_, err = base.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("hop-a-%d", depth)), Labels: []string{"Hop"}, Properties: map[string]interface{}{"hopId": fmt.Sprintf("a:%d", depth)}})
			require.NoError(t, err)
		}
		exec := NewStorageExecutor(&createEdgeErrEngine{Engine: base, err: errors.New("edge create boom")})
		_, supported, err := exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, rest)
		require.Error(t, err)
		require.True(t, supported)
		require.Contains(t, err.Error(), "UNWIND fixed-chain merge failed")
	})
}

func TestExecuteUnwindFixedChainLinkBatch_MalformedShapesMoreBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "fixed_chain_malformed_more"))
	ctx := context.Background()
	items := []interface{}{map[string]interface{}{"textKey": "a"}}

	cases := []string{
		`MATCH (o:OriginalText {textKey: row.textKey}) MATCH (h1:Hop {hopId: row.textKey + ':1'}) MERGE (o)-[:NEXT]->(h1)`,
		`MATCH (o:OriginalText {textKey: row.textKey}) MATCH (h1:Hop {hopId: row.textKey + ':1'}) MERGE (o)-[:NEXT]->(h1) RETURN count(o)`,
		`MATCH (o:OriginalText) WHERE id(o) = row.root_id MATCH (h1:Hop {hopId: row.textKey + ':1'}) MERGE (o)-[:NEXT]->(h1) RETURN count(o) AS linked`,
		`MATCH (o:OriginalText {textKey: row.textKey}) MATCH (h1:Hop {hopId: row.textKey + ':x'}) MERGE (o)-[:NEXT]->(h1) RETURN count(o) AS linked`,
		`MATCH (o:OriginalText {textKey: row.textKey}) MATCH (h1:Hop {hopId: row.textKey + ':1'}) MERGE (o)-[:NEXT]->(missing) RETURN count(o) AS linked`,
		`MATCH (o:OriginalText {textKey: row.textKey}) MATCH (h1:Hop {hopId: row.textKey + ':1'}) MERGE (o)-[:NEXT]->(h1) MERGE (h1)-[:OTHER]->(h2) RETURN count(o) AS linked`,
		`MATCH (o:OriginalText {textKey: row.textKey}) MATCH (h1:Hop {hopId: row.textKey + ':1'}) MERGE (o)-[:NEXT]->(h1) MERGE (h1)-[r:NEXT]->(h2) RETURN count(o) AS linked`,
	}

	for i, rest := range cases {
		result, supported, err := exec.executeUnwindFixedChainLinkBatch(ctx, "row", items, rest)
		require.NoError(t, err, "case %d", i)
		if i == 1 {
			require.True(t, supported)
			require.Equal(t, []string{"count(o)"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			continue
		}
		require.False(t, supported, "case %d", i)
	}

	// Non-map rows are ignored; supported shape still returns count 0.
	validRest := `
MATCH (o:OriginalText {textKey: row.textKey})
MATCH (h1:Hop {hopId: row.textKey + ':1'})
MERGE (o)-[:NEXT]->(h1)
RETURN count(o) AS linked
`
	res, supported, err := exec.executeUnwindFixedChainLinkBatch(ctx, "row", []interface{}{int64(1), "bad"}, validRest)
	require.NoError(t, err)
	require.True(t, supported)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 0, res.Rows[0][0])
}
