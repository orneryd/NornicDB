package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkTemporalConstraintCreate measures a direct node create that is
// checked against a temporal no-overlap constraint with existing history.
// Each write appends a non-overlapping interval to an existing key.
func BenchmarkTemporalConstraintCreate(b *testing.B) {
	day := 24 * time.Hour
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, composite := range []bool{false, true} {
		for _, existing := range []int{1000, 8000} {
			name := fmt.Sprintf("scalar/existing%d", existing)
			keyProps := []string{"key"}
			if composite {
				name = fmt.Sprintf("composite/existing%d", existing)
				keyProps = []string{"key", "group"}
			}
			b.Run(name, func(b *testing.B) {
				b.StopTimer()
				inner, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
				require.NoError(b, err)
				b.Cleanup(func() { require.NoError(b, inner.Close()) })
				engine := NewNamespacedEngine(inner, "test")
				c := Constraint{Name: "bench_temporal", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Period", Properties: append(append([]string(nil), keyProps...), "from", "to")}
				require.NoError(b, ValidateConstraintOnCreationForEngine(engine, c))
				require.NoError(b, engine.GetSchema().AddConstraint(c))
				require.NoError(b, MarkNewTemporalConstraintsForEngine(engine, engine.GetSchema(), []Constraint{c}))
				properties := func(key, slot int) map[string]interface{} {
					p := map[string]interface{}{"key": int64(key), "from": start.Add(time.Duration(slot) * day), "to": start.Add(time.Duration(slot+1) * day)}
					if composite {
						p["group"] = fmt.Sprintf("g%d", key%7)
					}
					return p
				}
				for i := 0; i < existing; i++ {
					_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("seed%d", i)), Labels: []string{"Period"}, Properties: properties(i, 0)})
					require.NoError(b, err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					node := &Node{ID: NodeID(fmt.Sprintf("new%d", i)), Labels: []string{"Period"}, Properties: properties(i%existing, 1+i/existing)}
					b.StartTimer()
					_, err := engine.CreateNode(node)
					b.StopTimer()
					require.NoError(b, err)
				}
			})
		}
	}
}
