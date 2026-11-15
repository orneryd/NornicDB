package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// Exact scratch-field rows must not admit sibling state, lookalike types or
// writes to real engine counts outside their reviewed owner.
func TestCallerCopyIndexFieldsAreExactTraversalScratch(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
 type callerCopyIndex struct { count, current, unreviewed int; slots []uint64 }
 func (i *callerCopyIndex) insert() { i.count++; i.current++; i.slots = make([]uint64, 2); i.unreviewed++ }
 func (i *callerCopyIndex) grow() { i.slots = make([]uint64, 4); i.slots[0] = 1 }
 func (i *callerCopyIndex) clear() { i.count = 0; i.current = 0; clear(i.slots) }
 type callerCopyIndexLookalike struct { count, current int; slots []uint64 }
 func (i *callerCopyIndexLookalike) change() { i.count++; i.current++; i.slots = make([]uint64, 2) }
 type BadgerEngine struct { nodeCount int }
 func (b *BadgerEngine) unauthorized() { b.nodeCount++ }
 func CensusFixtureUses(i *callerCopyIndex, look *callerCopyIndexLookalike, b *BadgerEngine) { i.insert(); i.grow(); i.clear(); look.change(); b.unauthorized() }
 `, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
	a := &analysis{p: p}
	r := &Report{}
	a.checkFields(r)
	text := strings.Join(r.Violations, "\n")
	classes := map[string]string{
		"count":   "caller-copy traversal progress",
		"current": "caller-copy traversal progress",
		"slots":   "caller-copy identity lookup scratch",
	}
	counts := map[string]int{}
	for _, w := range r.FieldWrites {
		for field, class := range classes {
			if w.Field != "pkg/storage.callerCopyIndex."+field {
				continue
			}
			counts[field]++
			require.Equal(t, class, w.Class)
		}
	}
	require.Equal(t, map[string]int{"count": 2, "current": 2, "slots": 3}, counts, "all seven writes must actually be discovered")
	for field, class := range classes {
		name := "pkg/storage.callerCopyIndex." + field
		row, ok := classOf(name)
		require.True(t, ok, name)
		require.Equal(t, class, row.class)
		require.NotContains(t, text, "unclassified field: "+name+" ")
		look := "pkg/storage.callerCopyIndexLookalike." + field
		_, ok = classOf(look)
		require.False(t, ok, "same field name on another type must remain unreviewed")
		require.Contains(t, text, "unclassified field: "+look+" ")
	}
	require.Contains(t, text, "unclassified field: pkg/storage.callerCopyIndex.unreviewed ")
	require.Contains(t, text, "unowned entity-count change: pkg/storage.BadgerEngine.nodeCount ")
}
