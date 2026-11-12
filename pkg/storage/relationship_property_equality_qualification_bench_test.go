package storage

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// relationshipEqualityQualificationColdDeclarationCases is the bounded benchmark
// table: each representative population and arity once for the cold-declaration
// cost, then the declaration ladder at the small population for the retained-set
// size and the declaration that retires an entry.
var relationshipEqualityQualificationColdDeclarationCases = []relationshipEqualityQualificationCase{
	{edges: 128, defs: 1, arity: 1},
	{edges: 4096, defs: 1, arity: 2},
	{edges: 32768, defs: 1, arity: 1},
	{edges: 32768, defs: 1, arity: 4},
	{edges: 128, defs: relationshipUniqueIndexLimit, arity: 1},
	{edges: 128, defs: relationshipUniqueIndexLimit + 1, arity: 1},
}

// BenchmarkRelationshipPropertyEqualityQualificationColdDeclaration measures one
// cold declaration of a definition: nothing is retained for the measured
// definition, so the declaration builds its index from the stored relationships
// of the type and publishes it. ns/op, B/op and allocs/op describe that build.
//
// retained-heap-B reports the process heap a live engine keeps after declaring
// the case's definitions, collected and measured once per case. It has no pass
// threshold: a heap delta of one build is not a stable comparison, so the number
// is reported for review rather than asserted.
func BenchmarkRelationshipPropertyEqualityQualificationColdDeclaration(b *testing.B) {
	for _, testCase := range relationshipEqualityQualificationColdDeclarationCases {
		b.Run(testCase.name(), func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			fixture := newRelationshipEqualityQualificationFixture(b, testCase.arity, relationshipEqualityQualificationCasePopulations(testCase))
			engine := fixture.engine
			ctx := context.Background()
			for _, definition := range fixture.defs {
				require.NoError(b, engine.GetSchemaForNamespace(definition.namespace).AddConstraint(definition.constraint, false))
			}
			declare := func(definition relationshipEqualityQualificationDefinition) {
				require.NoError(b, PrepareRelationshipPropertyEqualityIndexForEngine(ctx, fixture.declarationEngine(definition.namespace), definition.constraint))
			}
			measured := fixture.defs[len(fixture.defs)-1]
			preceding := fixture.defs[:len(fixture.defs)-1]

			retained := relationshipEqualityQualificationRetainedHeapDelta(b, engine, fixture.defs, declare)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// The measured declaration always builds from the stored
				// relationships. The declarations before it are untimed
				// preparation restoring the case's retained-set size, so the
				// bound and its retirement are in force where the case asks.
				b.StopTimer()
				engine.resetRelationshipUniqueIndexes()
				for _, definition := range preceding {
					declare(definition)
				}
				b.StartTimer()
				declare(measured)
			}
			b.StopTimer()
			b.ReportMetric(float64(retained), "retained-heap-B")
		})
	}
}

// relationshipEqualityQualificationRetainedHeap returns the process heap the live
// fixture retains after two full collections, so unreachable work from the
// declarations is not part of the sample.
func relationshipEqualityQualificationRetainedHeap() int64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(stats.HeapAlloc)
}

// relationshipEqualityQualificationRetainedHeapDelta measures, without a pass
// threshold, the heap the case's declarations retain while the engine stays live.
// The baseline is the same engine with no retained definition, so the delta counts
// the derived indexes the declarations built — their postings, per-definition
// property slices and key maps — together with whatever the declaration's type
// read retains. The fixture's stored relationships, the engine and badger's own
// tables are in the baseline and are therefore excluded, and collection noise is
// included. runtime.KeepAlive keeps the engine and its retained indexes live
// across the measurement.
func relationshipEqualityQualificationRetainedHeapDelta(b *testing.B, engine *BadgerEngine, definitions []relationshipEqualityQualificationDefinition, declare func(relationshipEqualityQualificationDefinition)) int64 {
	b.Helper()
	engine.resetRelationshipUniqueIndexes()
	baseline := relationshipEqualityQualificationRetainedHeap()
	for _, definition := range definitions {
		declare(definition)
	}
	retained := relationshipEqualityQualificationRetainedHeap() - baseline
	runtime.KeepAlive(engine)
	b.Logf("retained heap delta %d B for %d declared definitions (%d retained by the %d-entry cap): the delta covers the derived relationship equality indexes and the declaration's type read, excludes the fixture's stored relationships, the engine and badger tables, includes collection noise, and asserts no pass threshold",
		retained, len(definitions), min(len(definitions), relationshipUniqueIndexLimit), relationshipUniqueIndexLimit)
	return retained
}
