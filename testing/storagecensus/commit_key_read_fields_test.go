package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

func TestCommitKeySizingRetainsExactCountReadersAndWriters(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
 type BadgerTransaction struct {
 pendingLabelCountDeltas map[string]int64
 pendingEdgeTypeCountDeltas map[string]int64
 pendingEdgeTypeLabelCountDeltas map[string]int64
 }
 func(t *BadgerTransaction)storeKeyLimitErrorLocked() int { return len(t.pendingLabelCountDeltas)+len(t.pendingEdgeTypeCountDeltas)+len(t.pendingEdgeTypeLabelCountDeltas) }
 func(t *BadgerTransaction)unreviewedReader() int { return len(t.pendingLabelCountDeltas)+len(t.pendingEdgeTypeCountDeltas)+len(t.pendingEdgeTypeLabelCountDeltas) }
 func(t *BadgerTransaction)unreviewedWriter() { t.pendingLabelCountDeltas["x"]=1;t.pendingEdgeTypeCountDeltas["x"]=1;t.pendingEdgeTypeLabelCountDeltas["x"]=1 }
 func CensusFixtureUses(t *BadgerTransaction) int { t.unreviewedWriter();return t.storeKeyLimitErrorLocked()+t.unreviewedReader() }
 `, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
	a := &analysis{p: p}
	r := &Report{}
	a.checkFields(r)
	text := strings.Join(r.Violations, "\n")
	require.NotContains(t, text, "in pkg/storage.BadgerTransaction.storeKeyLimitErrorLocked ")
	for _, field := range []string{"pendingLabelCountDeltas", "pendingEdgeTypeCountDeltas", "pendingEdgeTypeLabelCountDeltas"} {
		var sizingReads int
		for _, rd := range a.fieldReads() {
			if rd.field == "pkg/storage.BadgerTransaction."+field && rd.fn == "pkg/storage.BadgerTransaction.storeKeyLimitErrorLocked" {
				sizingReads++
			}
		}
		require.Positive(t, sizingReads, "approved field must actually be read: %s", field)
		require.Contains(t, text, "unowned derived-count read: pkg/storage.BadgerTransaction."+field)
		require.Contains(t, text, "unowned derived-count change: pkg/storage.BadgerTransaction."+field)
	}
	require.Contains(t, text, "in pkg/storage.BadgerTransaction.unreviewedReader ")
	require.Contains(t, text, "in pkg/storage.BadgerTransaction.unreviewedWriter ")
}
