package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The public ID-only route must bound candidate work before the namespace
// callback, including when every foreign ID precedes its first local match.
// AllocsPerRun observes that work without a production counter or timing noise.
func TestScopedLabelIDReadCostIgnoresOtherDatabases(t *testing.T) {
	makeStore := func(foreign int) *BadgerEngine {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })
		for _, population := range []struct {
			namespace string
			count     int
		}{{"b", foreign}, {"a", 8}} {
			nodes := make([]*Node, 0, population.count)
			for index := 0; index < population.count; index++ {
				nodes = append(nodes, &Node{ID: NodeID(fmt.Sprintf("%s:n%d", population.namespace, index)), Labels: []string{"Person"}})
			}
			require.NoError(t, engine.BulkCreateNodes(nodes))
		}
		return engine
	}
	alone, shared := makeStore(0), makeStore(10_000)
	for _, wrapper := range []string{"badger", "wal", "traced_wal"} {
		t.Run(wrapper, func(t *testing.T) {
			wrap := func(engine *BadgerEngine) Engine {
				if wrapper == "badger" {
					return engine
				}
				wal, err := NewWAL(t.TempDir(), nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = wal.Close() })
				var wrapped Engine = NewWALEngine(engine, wal)
				if wrapper == "traced_wal" {
					wrapped = NewTracedEngine(wrapped)
				}
				return wrapped
			}
			measure := func(engine Engine) float64 {
				namespace := NewNamespacedEngine(engine, "a")
				return testing.AllocsPerRun(3, func() {
					seen := 0
					err := namespace.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
						seen++
						if id != "n0" {
							t.Fatalf("first ID = %q", id)
						}
						return false
					})
					if err != nil || seen != 1 {
						t.Fatalf("ID scan: seen=%d err=%v", seen, err)
					}
				})
			}
			localCost, sharedCost := measure(wrap(alone)), measure(wrap(shared))
			t.Logf("ID-only allocation cost: alone=%.0f foreign-first10000=%.0f", localCost, sharedCost)
			require.LessOrEqual(t, sharedCost, 3*localCost+64, "foreign candidates must not scale ID-only read work")
		})
	}
}
