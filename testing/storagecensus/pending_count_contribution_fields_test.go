package storagecensus

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// Each exact reader must read every approved field; unreviewed reads and writes
// to those same fields must remain rejected.
func TestApprovedDerivedCountOwnersRetainExactReadersAndWriters(t *testing.T) {
	cases := []struct {
		owner  string
		fields []string
	}{
		{"pendingCountStoreContributionLocked", []string{"pendingLabelCountDeltas", "pendingEdgeTypeCountDeltas"}},
		{"storeKeyLimitErrorLocked", []string{"pendingLabelCountDeltas", "pendingEdgeTypeCountDeltas", "pendingEdgeTypeLabelCountDeltas"}},
	}
	for _, c := range cases {
		t.Run(c.owner, func(t *testing.T) {
			expressions := make([]string, 0, len(c.fields))
			for _, field := range c.fields {
				expressions = append(expressions, "len(t."+field+")")
			}
			sizing := strings.Join(expressions, "+")

			source := fmt.Sprintf(`package storage
 type BadgerTransaction struct {
 pendingLabelCountDeltas map[string]int64
 pendingEdgeTypeCountDeltas map[string]int64
 pendingEdgeTypeLabelCountDeltas map[string]int64
 }
 func(t *BadgerTransaction)%s() int { return %s }
 func(t *BadgerTransaction)unreviewedReader() int { return %s }
 func(t *BadgerTransaction)unreviewedWriter() { t.pendingLabelCountDeltas["x"]=1;t.pendingEdgeTypeCountDeltas["x"]=1;t.pendingEdgeTypeLabelCountDeltas["x"]=1 }
 func CensusFixtureUses(t *BadgerTransaction) int { t.unreviewedWriter();return t.%s()+t.unreviewedReader() }
 `, c.owner, sizing, sizing, c.owner)
			p, _ := ssaFixture(t, source, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
			a := &analysis{p: p}
			r := &Report{}
			a.checkFields(r)
			text := strings.Join(r.Violations, "\n")
			require.NotContains(t, text, "in pkg/storage.BadgerTransaction."+c.owner+" ")
			for _, field := range c.fields {
				var sizingReads int
				for _, rd := range a.fieldReads() {
					if rd.field == "pkg/storage.BadgerTransaction."+field && rd.fn == "pkg/storage.BadgerTransaction."+c.owner {
						sizingReads++
					}
				}
				require.Positive(t, sizingReads, "approved field must actually be read: %s", field)
				require.Contains(t, text, "unowned derived-count read: pkg/storage.BadgerTransaction."+field)
				require.Contains(t, text, "unowned derived-count change: pkg/storage.BadgerTransaction."+field)
			}
			require.Contains(t, text, "in pkg/storage.BadgerTransaction.unreviewedReader ")
			require.Contains(t, text, "in pkg/storage.BadgerTransaction.unreviewedWriter ")
		})
	}
}
