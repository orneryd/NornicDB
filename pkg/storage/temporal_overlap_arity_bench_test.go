package storage

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// BenchmarkTemporalOverlapCheckByArity compares the same native overlap owner
// with its authoritative temporal index and its complete label-scan oracle.
// It measures preparation/validation, not a full query or durable write.
func BenchmarkTemporalOverlapCheckByArity(b *testing.B) {
	for _, width := range []int{1, 2, 4, 8} {
		for _, population := range []int{100, 1000} {
			for _, indexed := range []bool{true, false} {
				b.Run(fmt.Sprintf("width=%d/population=%d/indexed=%t", width, population, indexed), func(b *testing.B) {
					engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true}))
					require.NoError(b, err)
					b.Cleanup(func() { require.NoError(b, engine.Close()) })
					scoped := NewNamespacedEngine(engine, "test")
					names := make([]string, width)
					for i := range names {
						names[i] = fmt.Sprintf("k%d", i)
					}
					c := Constraint{Name: "intervals", Type: ConstraintTemporal, Label: "Period", EntityType: ConstraintEntityNode, Properties: append(append([]string{}, names...), "starts", "ends")}
					require.NoError(b, scoped.GetSchema().AddConstraint(c))
					require.NoError(b, MarkNewTemporalConstraintsForEngine(scoped, scoped.GetSchema(), []Constraint{c}))
					start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
					values := func(slot int) map[string]interface{} {
						p := map[string]interface{}{"starts": start.Add(time.Duration(slot) * time.Hour), "ends": start.Add(time.Duration(slot+1) * time.Hour)}
						for i, name := range names {
							p[name] = int64(i + 1)
						}
						return p
					}
					for i := 0; i < population; i++ {
						_, err := scoped.CreateNode(b.Context(), &Node{ID: NodeID(fmt.Sprintf("seed%d", i)), Labels: []string{"Period"}, Properties: values(i)})
						require.NoError(b, err)
					}
					spec, _ := splitTemporalKeySpec(c.Properties)
					key, _ := spec.keyValue(values(population))
					from, to := start.Add(time.Duration(population)*time.Hour), start.Add(time.Duration(population+1)*time.Hour)
					mode := temporalOverlapIndexed
					if !indexed {
						mode = temporalOverlapFullLabel
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						err := engine.withView(func(txn *badger.Txn) error {
							return engine.scanForTemporalOverlapInTxn(b.Context(), txn, "test", c, key, from, to, true, "", mode)
						})
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
