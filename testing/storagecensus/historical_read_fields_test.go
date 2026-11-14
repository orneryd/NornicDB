package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// The pinned historical read counts the reads in flight on it: withTxn
// increments historicalReadEngine.active, finishRead decrements it, and Close
// retires the snapshot once the last read has ended. The fixture proves the
// census accepts those two changes under the approved snapshot readers row,
// and still rejects an unreviewed sibling field and the same field name on a
// lookalike type.
func TestHistoricalReadEngineActiveIsSnapshotReaderBookkeeping(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
 type historicalReadEngine struct { active int; unreviewed int }
 func(h *historicalReadEngine)withTxn(){h.active++;h.unreviewed++}
 func(h *historicalReadEngine)finishRead(){h.active--}
 type historicalReadEngineLookalike struct{active int}
 func(h *historicalReadEngineLookalike)withTxn(){h.active++}
 func CensusFixtureUses(h *historicalReadEngine, look *historicalReadEngineLookalike){h.withTxn();h.finishRead();look.withTxn()}
 `, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
	analysis := &analysis{p: p}
	var fields []string
	for _, w := range analysis.fieldWrites() {
		fields = append(fields, w.field)
	}
	for _, field := range []string{"historicalReadEngine.active", "historicalReadEngine.unreviewed", "historicalReadEngineLookalike.active"} {
		require.Contains(t, fields, "pkg/storage."+field, "fixture must actually expose each field write")
		_, typ := analysis.lookupField("pkg/storage." + field)
		require.NotNil(t, typ, "fixture numeric field types must be available")
		require.True(t, numeric(typ.Type()))
	}
	r := &Report{}
	analysis.checkFields(r)
	text := strings.Join(r.Violations, "\n")
	row, ok := classOf("pkg/storage.historicalReadEngine.active")
	require.True(t, ok)
	require.Equal(t, "snapshot readers", row.class)
	require.NotContains(t, text, "unclassified field: pkg/storage.historicalReadEngine.active ")
	var accepted []FieldWriteRow
	for _, w := range r.FieldWrites {
		if w.Field == "pkg/storage.historicalReadEngine.active" {
			accepted = append(accepted, w)
		}
	}
	require.Len(t, accepted, 2, "the increment and the decrement are the field's only changes (%s)", text)
	var writers []string
	for _, w := range accepted {
		require.Equal(t, "snapshot readers", w.Class)
		writers = append(writers, w.Func)
	}
	require.ElementsMatch(t, []string{"pkg/storage.historicalReadEngine.withTxn", "pkg/storage.historicalReadEngine.finishRead"}, writers)
	require.NotEqual(t, accepted[0].Pos, accepted[1].Pos, "each change is its own row")
	require.Contains(t, text, "unclassified field: pkg/storage.historicalReadEngine.unreviewed")
	_, ok = classOf("pkg/storage.historicalReadEngineLookalike.active")
	require.False(t, ok)
	require.Contains(t, text, "unclassified field: pkg/storage.historicalReadEngineLookalike.active")
}
