package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

func TestTemporalValidationReadCutClassificationIsFieldSpecific(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
type temporalValidationRead struct { readCut uint64; unreviewed uint64 }
func (r *temporalValidationRead) get(cut uint64) { r.readCut = cut; r.unreviewed = cut }
type lookalike struct { readCut uint64 }
func (r *lookalike) get(cut uint64) { r.readCut = cut }
func CensusFixtureUses(r *temporalValidationRead, other *lookalike) { r.get(1); other.get(2) }
`, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
	report := &Report{}
	(&analysis{p: p}).checkFields(report)
	violations := strings.Join(report.Violations, "\n")
	row, ok := classOf("pkg/storage.temporalValidationRead.readCut")
	require.True(t, ok)
	require.Equal(t, "read cache physical source cut", row.class)
	require.NotContains(t, violations, "unclassified field: pkg/storage.temporalValidationRead.readCut ")
	require.Contains(t, violations, "unclassified field: pkg/storage.temporalValidationRead.unreviewed ")
	require.Contains(t, violations, "unclassified field: pkg/storage.lookalike.readCut ")
}
