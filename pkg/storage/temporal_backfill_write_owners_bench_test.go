package storage

// The temporal backfill runs through one of two write owners: the engine's
// maintenance transaction (backfillTemporalIndexForLabel, as the constraint's
// creation and the start-up rebuild drive it) or a schema statement's buffered
// writes (stageTemporalBackfillForLabel). Each sub-benchmark below measures one
// bounded pass of one owner over the same stored population and the same
// constraint arity, so the comparison is between the writers, not the workload.
// The deliberately overlapping pre-constraint versions exercise winner selection;
// this is a derived-index kernel benchmark, not a valid NO OVERLAP DDL workload.
//
// The population is fixed at 200 nodes spread over 25 keys, several versions
// per key, all covering the pass's as-of time: every node derives a history
// entry per constraint and takes part in the current-version comparison. The
// index is cleared outside the timed region (the staged writer never publishes,
// so it needs none), and the acquisition and transaction boundaries stay
// outside it too: what is timed is the derivation and its writes.

import (
	"context"
	"fmt"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// temporalBackfillBenchPopulation bounds the benchmark population.
const temporalBackfillBenchPopulation = 200

// temporalBackfillBenchNodes returns the stored population: count nodes of one
// label over 25 keys, 8 versions per key with increasing start times, each
// version covering the pass's as-of time.
func temporalBackfillBenchNodes(count int) []*Node {
	now := time.Now().UTC()
	nodes := make([]*Node, 0, count)
	for i := 0; i < count; i++ {
		key := i % 25
		from := now.Add(time.Duration(i/25)*time.Minute - time.Hour)
		nodes = append(nodes, &Node{ID: NodeID(prefixTestID(fmt.Sprintf("bench-%d", i))), Labels: []string{"Fact"}, Properties: map[string]interface{}{
			"record_id":  int64(key),
			"group":      fmt.Sprintf("g%d", key%5),
			"valid_from": from,
			"valid_to":   from.Add(48 * time.Hour),
		}})
	}
	return nodes
}

// temporalBackfillBenchConstraints returns count constraints over the
// benchmark label: one scalar key, a second scalar key, and their composite.
func temporalBackfillBenchConstraints(count int) []Constraint {
	all := []Constraint{
		{Name: "bench_record", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Fact", Properties: []string{"record_id", "valid_from", "valid_to"}},
		{Name: "bench_group", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Fact", Properties: []string{"group", "valid_from", "valid_to"}},
		{Name: "bench_record_group", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Fact", Properties: []string{"record_id", "group", "valid_from", "valid_to"}},
	}
	return all[:count]
}

// commitTemporalBackfillBenchNodes stores the benchmark population in one
// commit, before any temporal constraint exists.
func commitTemporalBackfillBenchNodes(b *testing.B, engine *BadgerEngine, nodes []*Node) {
	b.Helper()
	ctx := context.Background()
	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(b, err)
	for _, node := range nodes {
		_, err := tx.CreateNode(ctx, node)
		require.NoError(b, err)
	}
	require.NoError(b, tx.Commit())
}

// BenchmarkTemporalBackfillWriteOwners measures one bounded backfill pass
// through each write owner at one and three constraints per node.
func BenchmarkTemporalBackfillWriteOwners(b *testing.B) {
	ctx := context.Background()
	for _, constraints := range []int{1, 3} {
		b.Run(fmt.Sprintf("maintenance/constraints%d", constraints), func(b *testing.B) {
			engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
			require.NoError(b, err)
			commitTemporalBackfillBenchNodes(b, engine, temporalBackfillBenchNodes(temporalBackfillBenchPopulation))
			sm := engine.GetSchemaForNamespace("test")
			for _, c := range temporalBackfillBenchConstraints(constraints) {
				require.NoError(b, sm.AddConstraint(c))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				require.NoError(b, engine.clearBadgerPrefix(ctx, prefixTemporalIndex))
				require.NoError(b, engine.clearBadgerPrefix(ctx, prefixTemporalHead))
				lifecycle, err := engine.beginMaintenanceWrite(ctx, nil)
				require.NoError(b, err)
				b.StartTimer()
				passErr := engine.backfillTemporalIndexForLabel(ctx, engine, "Fact")
				b.StopTimer()
				lifecycle.finish()
				require.NoError(b, passErr)
			}
		})
		b.Run(fmt.Sprintf("statement/constraints%d", constraints), func(b *testing.B) {
			engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
			require.NoError(b, err)
			commitTemporalBackfillBenchNodes(b, engine, temporalBackfillBenchNodes(temporalBackfillBenchPopulation))
			sm := engine.GetSchemaForNamespace("test")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tx, err := sm.BeginSchemaTransaction(ctx, engine.BeginTransaction)
				require.NoError(b, err)
				require.NoError(b, tx.SetNamespace("test"))
				_, err = tx.Schema(OrdinarySchemaChanges)
				require.NoError(b, err)
				view := newSchemaStatementView(tx, NewNamespacedEngine(engine, "test"), "test")
				for _, c := range temporalBackfillBenchConstraints(constraints) {
					require.NoError(b, view.GetSchema().AddConstraint(c))
				}
				b.StartTimer()
				passErr := stageTemporalBackfillForLabel(ctx, tx, view, "Fact")
				b.StopTimer()
				require.NoError(b, tx.Rollback())
				require.NoError(b, passErr)
			}
		})
	}
}
