package storage

import (
	"fmt"
	"testing"
)

var claimPreimageCountSink int

// BenchmarkNodeMutationPreimages measures only staged-image selection, not a
// storage commit. Repeated writes still require the ordered incarnation fold.
func BenchmarkNodeMutationPreimages(b *testing.B) {
	for _, size := range []int{1, 100, 1000} {
		for _, repeated := range []bool{false, true} {
			b.Run(fmt.Sprintf("nodes=%d/repeated=%t", size, repeated), func(b *testing.B) {
				tx := &BadgerTransaction{pendingNodes: make(map[NodeID]*Node, size)}
				for i := range size {
					id := NodeID(fmt.Sprintf("test:n%d", i))
					node := &Node{ID: id}
					tx.pendingNodes[id] = node
					tx.operations = append(tx.operations, Operation{Type: OpUpdateNode, NodeID: id, OldNode: node, Node: node})
				}
				if repeated {
					tx.operations = append(tx.operations, tx.operations[0])
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					folded, distinct := tx.nodeMutationPreimagesLocked()
					count := len(folded)
					if distinct {
						count = len(tx.operations)
					}
					claimPreimageCountSink = count
				}
			})
		}
	}
}
