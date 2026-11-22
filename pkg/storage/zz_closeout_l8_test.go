package storage

// Close-out lane L8, round 3 (2026-11-22): owner run for hunt lead L-H2-012.
// A node with managed chunk vectors is updated once, and twice, in one
// transaction; the staged operations' nodeAnchorEligible flags are logged
// together with the condition managedReferences applies to each operation
// (managed_vector_reference.go:133): an operation it skips is journaled with
// its full vector image. Test-only, not for merge.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestL8Owner_H2_012_SecondUpdateAnchorEligibility(t *testing.T) {
	for _, updates := range []int{1, 2} {
		t.Run(fmt.Sprintf("updates-%d", updates), func(t *testing.T) {
			engine := NewMemoryEngine()
			defer engine.Close()
			ctx := context.Background()
			chunks := make([][]float32, 4)
			for i := range chunks {
				chunks[i] = []float32{float32(i), 1, 2, 3}
			}
			if _, err := engine.CreateNode(ctx, &Node{ID: "l8:doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"text": "alpha"}, ChunkEmbeddings: chunks}); err != nil {
				t.Fatalf("create: %v", err)
			}
			tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			for i := 1; i <= updates; i++ {
				current, err := tx.GetNode(ctx, "l8:doc")
				if err != nil {
					t.Fatalf("read %d: %v", i, err)
				}
				next := CopyNode(current)
				next.Properties["k"] = int64(i)
				if err := tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: current, After: next}); err != nil {
					t.Fatalf("update %d: %v", i, err)
				}
			}
			ops := []map[string]any{}
			for index, operation := range tx.operations {
				skipped := operation.Type != OpUpdateNode || !operation.nodeAnchorEligible || operation.Node == nil || operation.OldNode == nil || len(operation.Node.ChunkEmbeddings) == 0
				ops = append(ops, map[string]any{"index": index, "type": fmt.Sprint(operation.Type), "nodeAnchorEligible": operation.nodeAnchorEligible,
					"node_chunks": l8ChunkCount(operation.Node), "old_chunks": l8ChunkCount(operation.OldNode), "managedReferences_skips_it": skipped})
			}
			data, _ := json.Marshal(map[string]any{"lead": "L-H2-012", "updates": updates, "operations": ops})
			t.Logf("L8_OWNER %s", data)
		})
	}
}

func l8ChunkCount(node *Node) int {
	if node == nil {
		return -1
	}
	return len(node.ChunkEmbeddings)
}
