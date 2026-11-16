package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

func TestTemporalRebuildPlanCursorClassificationIsExact(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
 type temporalRebuildPlan struct { next, unreviewed int }
 func (p *temporalRebuildPlan) nextEntry() { p.next++; p.unreviewed++ }
 type temporalRebuildPlanLookalike struct { next int }
 func (p *temporalRebuildPlanLookalike) nextEntry() { p.next++ }
 type BadgerEngine struct { nodeCount int }
 func (b *BadgerEngine) unauthorized() { b.nodeCount++ }
 func CensusFixtureUses(p *temporalRebuildPlan, other *temporalRebuildPlanLookalike, b *BadgerEngine) { p.nextEntry(); other.nextEntry(); b.unauthorized() }
 `, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
	report := &Report{}
	(&analysis{p: p}).checkFields(report)
	violations := strings.Join(report.Violations, "\n")
	var classes []string
	for _, write := range report.FieldWrites {
		if write.Field == "pkg/storage.temporalRebuildPlan.next" {
			classes = append(classes, write.Class)
		}
	}
	require.Equal(t, []string{"temporal backfill derivation cursor"}, classes)
	require.NotContains(t, violations, "unclassified field: pkg/storage.temporalRebuildPlan.next ")
	for _, field := range []string{
		"pkg/storage.temporalRebuildPlan.unreviewed",
		"pkg/storage.temporalRebuildPlanLookalike.next",
	} {
		require.Contains(t, violations, "unclassified field: "+field+" ")
	}
	require.Contains(t, violations, "unowned entity-count change: pkg/storage.BadgerEngine.nodeCount ")
}
