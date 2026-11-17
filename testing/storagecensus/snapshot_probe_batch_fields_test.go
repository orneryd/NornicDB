package storagecensus

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"strings"
	"testing"
)

func TestSnapshotProbeAndBatchByteFieldsAreExactScratch(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
 type BatchWriter struct { bytes, unreviewed int }
 func (b *BatchWriter) append() { b.bytes += 4; b.unreviewed++ }
 func (b *BatchWriter) CommitWithSeq() { b.bytes = 0 }
 func (b *BatchWriter) Rollback() { b.bytes = 0 }
 type snapshotEmbeddingAbsence struct { reads, unreviewed uint64 }
 func (p *snapshotEmbeddingAbsence) recordsAbsent() { p.reads++; p.unreviewed++ }
 type BatchWriterLookalike struct { bytes int }
 func (b *BatchWriterLookalike) change() { b.bytes++ }
 type snapshotEmbeddingAbsenceLookalike struct { reads uint64 }
 func (p *snapshotEmbeddingAbsenceLookalike) change() { p.reads++ }
 type BadgerEngine struct { nodeCount int }
 func (b *BadgerEngine) unauthorized() { b.nodeCount++ }
 func CensusFixtureUses(b *BatchWriter, p *snapshotEmbeddingAbsence, bl *BatchWriterLookalike, pl *snapshotEmbeddingAbsenceLookalike, engine *BadgerEngine) {
  b.append(); b.CommitWithSeq(); b.Rollback(); p.recordsAbsent(); bl.change(); pl.change(); engine.unauthorized()
 }
 `, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
	a := &analysis{p: p}
	r := &Report{}
	a.checkFields(r)
	text := strings.Join(r.Violations, "\n")
	classes := map[string]string{
		"pkg/storage.BatchWriter.bytes":              "WAL batch encoded buffer bytes",
		"pkg/storage.snapshotEmbeddingAbsence.reads": "snapshot embedding-probe progress",
	}
	counts := map[string]int{}
	for _, write := range r.FieldWrites {
		if class, ok := classes[write.Field]; ok {
			counts[write.Field]++
			require.Equal(t, class, write.Class)
		}
	}
	require.Equal(t, map[string]int{"pkg/storage.BatchWriter.bytes": 3, "pkg/storage.snapshotEmbeddingAbsence.reads": 1}, counts)
	for field, class := range classes {
		row, ok := classOf(field)
		require.True(t, ok, field)
		require.Equal(t, class, row.class)
		require.NotContains(t, text, "unclassified field: "+field+" ")
	}
	for _, field := range []string{"pkg/storage.BatchWriter.unreviewed", "pkg/storage.snapshotEmbeddingAbsence.unreviewed", "pkg/storage.BatchWriterLookalike.bytes", "pkg/storage.snapshotEmbeddingAbsenceLookalike.reads"} {
		_, ok := classOf(field)
		require.False(t, ok, field)
		require.Contains(t, text, "unclassified field: "+field+" ")
	}
	require.Contains(t, text, "unowned entity-count change: pkg/storage.BadgerEngine.nodeCount ")
}
