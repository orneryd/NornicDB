package storage

// A filtered scan of a label reads that label's nodes, whether or not it
// returns a projection, on the engine and at a transaction's snapshot
// (NOR-407). With a projection the engine read every node in scope, and a
// transaction did so for every filtered scan: the label was a hint the scan
// ignored.

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// documentsByIDOptions is the scan MATCH (n:Doc {id: $id}) RETURN n.id asks
// for: the label, the filter, and the one property the row reads.
func documentsByIDOptions(id string, storedVectors bool) StreamNodesOptions {
	return StreamNodesOptions{
		Prefix:               "a:",
		Label:                "Doc",
		QueryNodes:           !storedVectors,
		WithEmbeddings:       storedVectors,
		ApplyDecayFilter:     true,
		Projection:           []string{"id"},
		PropertyFilter:       func(properties map[string]interface{}) bool { return properties["id"] == id },
		PropertyStringEquals: map[string]string{"id": id},
	}
}

func TestProjectedFilteredLabelScanReadsTheLabel(t *testing.T) {
	for _, storedVectors := range []bool{false, true} {
		name := "query rows"
		if storedVectors {
			name = "rows with stored vectors"
		}
		t.Run(name, func(t *testing.T) {
			engine, _ := filteredLabelScanFixture(t, BadgerOptions{}, 40)
			// The fixture's Other nodes carry the same ids: a scan of every
			// node in scope returns them too. The scan repeats, so it runs on
			// the scan decoder, then keeps the label's nodes, then reads them
			// from the cache.
			for _, id := range []string{"d7", "d8", "d9", "d0"} {
				var rows []*Node
				require.NoError(t, engine.StreamNodesWithOptions(context.Background(), documentsByIDOptions(id, storedVectors), func(node *Node) error {
					rows = append(rows, node)
					return nil
				}))
				require.Len(t, rows, 1, id)
				require.Equal(t, NodeID("a:"+id), rows[0].ID)
				require.Equal(t, map[string]interface{}{"id": id}, rows[0].Properties, "the row is the projection")
			}
			require.Equal(t, 40, cachedDocuments(engine))
		})
	}
}

// A transaction's filtered scan of a label that is a small part of its scope
// reads the label's nodes: the fixture's Other nodes carry the same ids, and
// a scan of every node in scope returns them too. Its rows are the rows the
// scan of every node gives for the label's nodes, in the same order, and the
// transaction's own writes are part of it as before.
func TestTransactionFilteredLabelScanReadsTheLabel(t *testing.T) {
	engine, _ := filteredLabelScanFixture(t, BadgerOptions{}, 40)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	scan := func(options StreamNodesOptions) []*Node {
		t.Helper()
		var rows []*Node
		require.NoError(t, tx.StreamNodesWithOptions(context.Background(), options, func(node *Node) error {
			rows = append(rows, node)
			return nil
		}))
		return rows
	}
	ids := func(rows []*Node) []string {
		var ids []string
		for _, row := range rows {
			ids = append(ids, string(row.ID))
		}
		sort.Strings(ids)
		return ids
	}
	whole := func(id string) StreamNodesOptions {
		options := documentsByIDOptions(id, false)
		options.Projection, options.PropertyFilterProjection = nil, []string{"id"}
		return options
	}
	for _, id := range []string{"d7", "d8", "d0"} {
		for name, options := range map[string]StreamNodesOptions{"projected": documentsByIDOptions(id, false), "whole rows": whole(id)} {
			rows := scan(options)
			require.Equal(t, []string{"a:" + id}, ids(rows), "%s %s", name, id)
			// The scan of every node in scope, with the label tested here.
			everyNode := options
			everyNode.Label = ""
			var labelled []*Node
			for _, row := range scan(everyNode) {
				if node, err := engine.GetNode(t.Context(), row.ID); err == nil && hasLabel(node.Labels, "Doc") {
					labelled = append(labelled, row)
				}
			}
			require.Len(t, rows, len(labelled), "%s %s", name, id)
			for index, row := range rows {
				require.Equal(t, labelled[index].ID, row.ID, "%s %s: the same rows as the scan of every node", name, id)
				require.Equal(t, labelled[index].Labels, row.Labels, "%s %s", name, id)
				require.Equal(t, labelled[index].Properties, row.Properties, "%s %s", name, id)
			}
		}
	}
	require.Empty(t, scan(whole("none")))

	// A label on most of the scope's nodes is read in one pass over them.
	other := documentsByIDOptions("d7", false)
	other.Label = "Other"
	require.Contains(t, ids(scan(other)), "a:o7")

	// The transaction's own writes: a deleted node is gone, and a pending
	// node is returned for the caller to test, as before.
	_, err = tx.CreateNode(t.Context(), &Node{ID: "a:new", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "d7", "title": "t"}})
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode(t.Context(), "a:d8", CascadeEdges))
	require.Equal(t, []string{"a:d7", "a:new"}, ids(scan(whole("d7"))))
	require.Equal(t, []string{"a:d7", "a:new"}, ids(scan(documentsByIDOptions("d7", false))))
	require.NotContains(t, ids(scan(whole("d8"))), "a:d8")
}

// A transaction's filtered scan returns the projection it asked for and
// nothing else, however often the scan repeats.
func TestTransactionFilteredScanReturnsItsProjection(t *testing.T) {
	engine, _ := filteredLabelScanFixture(t, BadgerOptions{}, 40)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for round := 0; round < 3; round++ {
		found := false
		require.NoError(t, tx.StreamNodesWithOptions(context.Background(), documentsByIDOptions("d7", false), func(node *Node) error {
			require.Equal(t, map[string]interface{}{"id": "d7"}, node.Properties, "round %d: the row of %s is the projection", round, node.ID)
			found = found || node.ID == "a:d7"
			return nil
		}))
		require.True(t, found, "round %d", round)
	}
}
