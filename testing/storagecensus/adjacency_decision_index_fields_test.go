package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

func TestAdjacencyDecisionIndexesKeepExactPublicationOwnership(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
 type entityWriteLocks struct { absenceIndex map[uint64]int; absenceCollisions map[uint64][]int; unreviewed int }
 type BadgerTransaction struct { entityLocks entityWriteLocks }
 func(tx *BadgerTransaction) observeEmptyAdjacencyLocked() int {
  tx.entityLocks.absenceIndex=make(map[uint64]int)
  tx.entityLocks.absenceIndex[1]=1
  tx.entityLocks.absenceCollisions=make(map[uint64][]int)
  tx.entityLocks.absenceCollisions[1]=[]int{1}
  tx.entityLocks.unreviewed++
  return tx.entityLocks.absenceIndex[1]+len(tx.entityLocks.absenceCollisions[1])
 }
 func(tx *BadgerTransaction) unauthorized() int {
  tx.entityLocks.absenceIndex[2]=2
  tx.entityLocks.absenceCollisions[2]=[]int{2}
  return tx.entityLocks.absenceIndex[2]+len(tx.entityLocks.absenceCollisions[2])
 }
 type entityWriteLocksLookalike struct { absenceIndex map[uint64]int; absenceCollisions map[uint64][]int }
 func(l *entityWriteLocksLookalike) change() { l.absenceIndex=make(map[uint64]int);l.absenceCollisions=make(map[uint64][]int) }
 type BadgerEngine struct { nodeCount int }
 func(b *BadgerEngine) unauthorized() { b.nodeCount++ }
 func CensusFixtureUses(tx *BadgerTransaction,look *entityWriteLocksLookalike,b *BadgerEngine) { tx.observeEmptyAdjacencyLocked();tx.unauthorized();look.change();b.unauthorized() }
 `, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
	report := &Report{}
	(&analysis{p: p}).checkFields(report)
	violations := strings.Join(report.Violations, "\n")
	for _, field := range []string{"absenceIndex", "absenceCollisions"} {
		name := "pkg/storage.entityWriteLocks." + field
		row, ok := classOf(name)
		require.True(t, ok)
		require.Equal(t, "publication-order", row.class)
		require.Equal(t, []string{"pkg/storage.BadgerTransaction.observeEmptyAdjacencyLocked"}, row.owners)
		require.Empty(t, row.readers, "no separate reader exceptions")
		require.NotContains(t, violations, "unclassified field: "+name+" ")
		require.Contains(t, violations, "unowned publication-order change: "+name+" ")
		require.Contains(t, violations, "unowned publication-order read: "+name+" ")
		require.Contains(t, violations, "unclassified field: pkg/storage.entityWriteLocksLookalike."+field+" ")
		allowed := 0
		for _, write := range report.FieldWrites {
			if write.Field == name && write.Func == "pkg/storage.BadgerTransaction.observeEmptyAdjacencyLocked" {
				allowed++
				require.Equal(t, "publication-order", write.Class)
			}
		}
		require.GreaterOrEqual(t, allowed, 2, "the approved owner's map initialization and insertion must actually be analyzed")
	}
	require.Contains(t, violations, "unclassified field: pkg/storage.entityWriteLocks.unreviewed ")
	require.Contains(t, violations, "unowned entity-count change: pkg/storage.BadgerEngine.nodeCount ")
}
