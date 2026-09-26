package cypher

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestTransactionCountsEqualScan: inside an explicit transaction, the
// counter-served counts (committed counter plus the transaction's staged
// delta) equal a scan of the transaction-visible nodes and relationships
// after every kind of write: creates, relabels (including a node whose
// relationships are untouched), node deletes, relationship creates and
// deletes. After COMMIT the counters equal a scan of the committed data;
// after ROLLBACK they are unchanged. Deleting a node that had a relationship
// created in the same transaction is left out (#741): only nodes no
// relationship ever touches ({iso: true}) are deleted.
func TestTransactionCountsEqualScan(t *testing.T) {
	statements := []string{
		"CREATE (:A {iso: true})",
		"CREATE (:A:B {i: 2})",
		"CREATE (:A)-[:T]->(:B)",
		"CREATE (:B)-[:U]->(:C)",
		"MATCH (n:A) WITH n LIMIT 1 SET n:B",
		"MATCH (n:B) WITH n LIMIT 1 REMOVE n:B",
		"MATCH (n)-[:T]->() WITH n LIMIT 1 REMOVE n:A SET n:C",
		"MATCH (n)<-[:T]-() WITH n LIMIT 1 SET n:A",
		"CREATE (:C {iso: true})",
		"MATCH (n:C {iso: true}) WITH n LIMIT 1 DELETE n",
		"MATCH (n:A {iso: true}) WITH n LIMIT 1 DELETE n",
		"MATCH ()-[r:T]->() WITH r LIMIT 1 DELETE r",
		"MATCH (a:A), (b:B) WHERE a.iso IS NULL AND b.iso IS NULL WITH a, b LIMIT 1 CREATE (a)-[:U]->(b)",
		"MATCH (a:B), (b:C) WHERE a.iso IS NULL AND b.iso IS NULL WITH a, b LIMIT 1 CREATE (a)-[:T]->(b)",
		"MATCH ()-[r:U]->() WITH r LIMIT 1 DELETE r",
	}
	labels := []string{"A", "B", "C"}
	types := []string{"T", "U"}

	requireCountsEqualScan := func(t *testing.T, w *transactionStorageWrapper, step string) {
		t.Helper()
		for _, label := range labels {
			fast, err := w.NodeCountByLabel(label)
			require.NoError(t, err)
			scan, err := w.nodeCountByLabelScan(label)
			require.NoError(t, err)
			require.Equal(t, scan, fast, "%s: nodes :%s", step, label)
		}
		for _, edgeType := range types {
			fast, err := w.EdgeCountByType(edgeType)
			require.NoError(t, err)
			scan, err := w.edgeCountByTypeScan(edgeType)
			require.NoError(t, err)
			require.Equal(t, scan, fast, "%s: relationships :%s", step, edgeType)
			for _, label := range labels {
				for _, start := range []bool{true, false} {
					fast, err := w.edgeCountByEndpointLabel(label, edgeType, start)
					require.NoError(t, err)
					scan, err := w.edgeCountByEndpointLabelScan(label, edgeType, start)
					require.NoError(t, err)
					require.Equal(t, scan, fast, "%s: relationships :%s with %s node :%s", step, edgeType, map[bool]string{true: "start", false: "end"}[start], label)
				}
			}
		}
	}

	for seed := int64(1); seed <= 12; seed++ {
		end := "COMMIT"
		if seed%3 == 0 {
			end = "ROLLBACK"
		}
		t.Run(fmt.Sprintf("seed=%d/%s", seed, end), func(t *testing.T) {
			base, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "pending")
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(seed))

			// Committed data the transaction starts from.
			for i := 0; i < 6; i++ {
				_, err := exec.Execute(ctx, statements[rng.Intn(4)], nil)
				require.NoError(t, err)
			}

			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			// The transaction's storage wrapper exists from its first statement.
			_, err = exec.Execute(ctx, "RETURN 1", nil)
			require.NoError(t, err)
			wrapper := exec.txContext.storageWrapper
			require.NotNil(t, wrapper)
			requireCountsEqualScan(t, wrapper, "begin")
			for step := 0; step < 40; step++ {
				statement := statements[rng.Intn(len(statements))]
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
				requireCountsEqualScan(t, wrapper, fmt.Sprintf("step %d %s", step, statement))
			}
			before := map[string]int64{}
			if end == "ROLLBACK" {
				for _, label := range labels {
					before[label], err = store.NodeCountByLabel(label)
					require.NoError(t, err)
				}
			}
			_, err = exec.Execute(ctx, end, nil)
			require.NoError(t, err)

			// Committed state: the counters equal a scan outside any transaction.
			for _, label := range labels {
				counted, err := store.NodeCountByLabel(label)
				require.NoError(t, err)
				nodes, err := store.GetNodesByLabel(label)
				require.NoError(t, err)
				require.EqualValues(t, len(nodes), counted, "committed nodes :%s", label)
				if end == "ROLLBACK" {
					require.Equal(t, before[label], counted, "rollback nodes :%s", label)
				}
			}
			for _, edgeType := range types {
				counted, err := store.EdgeCountByType(edgeType)
				require.NoError(t, err)
				edges, err := store.GetEdgesByType(edgeType)
				require.NoError(t, err)
				require.EqualValues(t, len(edges), counted, "committed relationships :%s", edgeType)
				for _, label := range labels {
					var starts, ends int64
					for _, edge := range edges {
						if node, err := store.GetNode(edge.StartNode); err == nil && nodeHasLabelFold(node, label) {
							starts++
						}
						if node, err := store.GetNode(edge.EndNode); err == nil && nodeHasLabelFold(node, label) {
							ends++
						}
					}
					counted, err := store.EdgeCountByStartLabel(label, edgeType)
					require.NoError(t, err)
					require.Equal(t, starts, counted, "committed relationships :%s with start node :%s", edgeType, label)
					counted, err = store.EdgeCountByEndLabel(label, edgeType)
					require.NoError(t, err)
					require.Equal(t, ends, counted, "committed relationships :%s with end node :%s", edgeType, label)
				}
			}
		})
	}
}

func nodeHasLabelFold(node *storage.Node, label string) bool {
	for _, l := range node.Labels {
		if strings.EqualFold(l, label) {
			return true
		}
	}
	return false
}
