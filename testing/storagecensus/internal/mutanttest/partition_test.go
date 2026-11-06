package mutanttest

import (
	"testing"
)

// TestPartitionAssignmentCoversMatrixOnce proves the split of the shared matrix
// across partitionCount packages loses nothing: every case is assigned to
// exactly one partition, every partition runs at least one case, and the case
// names stay distinct, so each subtest name identifies exactly one mutant. The
// matrix literal is what the partitions are derived from; it is not split or
// copied per partition.
func TestPartitionAssignmentCoversMatrixOnce(t *testing.T) {
	byPartition := make([][]string, partitionCount)
	for i, m := range mutants {
		p := partitionOf(i)
		if p < 0 || p >= partitionCount {
			t.Fatalf("case %d assigned to partition %d, want 0..%d", i, p, partitionCount-1)
		}
		byPartition[p] = append(byPartition[p], m.name)
	}
	owner := make(map[string]int, len(mutants))
	assigned := 0
	for p, names := range byPartition {
		if len(names) == 0 {
			t.Fatalf("partition %d is empty", p)
		}
		for _, name := range names {
			if prev, dup := owner[name]; dup {
				t.Fatalf("case %q assigned to partition %d and again to partition %d", name, prev, p)
			}
			owner[name] = p
			assigned++
		}
		t.Logf("partition %d: %d cases", p, len(names))
	}
	if assigned != len(mutants) {
		t.Fatalf("assigned %d cases, the matrix has %d", assigned, len(mutants))
	}
	if len(owner) != len(mutants) {
		t.Fatalf("%d distinct case names for %d matrix cases: an omission or duplicate remains", len(owner), len(mutants))
	}
	t.Logf("matrix: %d cases over %d partitions", len(mutants), partitionCount)
}
