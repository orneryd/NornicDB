package storage

import (
	"fmt"
	"testing"
)

// BenchmarkPublicationKeys measures dependency construction for the 100-node
// UNIQUE batches used by the concurrent public commit benchmark. The commit
// benchmark remains the qualification path; this isolates allocation ownership.
func BenchmarkPublicationKeys(b *testing.B) {
	schema := NewSchemaManager()
	if err := schema.AddUniqueConstraint("function_uid", "Function", "uid"); err != nil {
		b.Fatal(err)
	}
	for _, changed := range []bool{false, true} {
		b.Run(fmt.Sprintf("changed=%t", changed), func(b *testing.B) {
			ops := make([]Operation, badgerUniqueLockKeysPerWriter)
			for i := range ops {
				old := badgerUniqueFunctionNode(0, i, 1)
				written := badgerUniqueFunctionNode(0, i, 2)
				if changed {
					written.Properties["uid"] = fmt.Sprintf("replacement-%d", i)
				}
				ops[i] = Operation{Type: OpUpdateNode, Node: written, OldNode: old}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				constraints := capturePublicationConstraintView(schema)
				if len(appendOperationPublicationKeys(nil, &constraints, ops)) == 0 {
					b.Fatal("missing publication dependencies")
				}
			}
		})
	}
}

// BenchmarkConstraintLookup isolates the constraint list consumed repeatedly
// by the concurrent UNIQUE commit workload, including deterministic ordering.
func BenchmarkConstraintLookup(b *testing.B) {
	for _, count := range []int{1, 8} {
		b.Run(fmt.Sprintf("constraints=%d", count), func(b *testing.B) {
			schema := NewSchemaManager()
			for i := 0; i < count; i++ {
				if err := schema.AddUniqueConstraint(fmt.Sprintf("c-%d", i), "Function", fmt.Sprintf("p-%d", i)); err != nil {
					b.Fatal(err)
				}
			}
			labels := []string{"Function"}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if len(schema.GetConstraintsForLabels(labels)) != count {
					b.Fatal("missing constraints")
				}
			}
		})
	}
}

// BenchmarkPublicationStage measures repeated entity dependencies from an
// operation and its pending-embedding hint, using the same 100-node batch.
func BenchmarkPublicationStage(b *testing.B) {
	keys := make([]any, 0, 2*badgerUniqueLockKeysPerWriter)
	for i := range badgerUniqueLockKeysPerWriter {
		id := NodeID(fmt.Sprintf("test:n-%d", i))
		keys = append(keys, id, id)
	}
	var stage publicationStage
	publication := commitPublication{keys: keys, run: func() {}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		ts := uint64(i + 1)
		stage.register(ts, &publication)
		stage.apply(ts)
	}
}
