package knowledgepolicy

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestBinding returns a CompiledBinding with all fields populated so that
// individual tests can confirm identity by pointer comparison.
func newTestBinding(halfLife int64, floor float64, fn DecayFunction) *CompiledBinding {
	threshold := 0.10
	return &CompiledBinding{
		DecayProfile: &DecayProfileBundle{
			Name:                "test-profile",
			HalfLifeSeconds:     halfLife / int64(1e9),
			VisibilityThreshold: threshold,
			ScoreFloor:          floor,
			Function:            fn,
			Scope:               ScopeNode,
			DecayEnabled:        true,
			ScoreFrom:           ScoreFromCreated,
			Enabled:             true,
		},
		DecayBinding: &DecayProfileBinding{
			Name:       "test-binding",
			ProfileRef: "test-profile",
			IsWildcard: false,
			IsEdge:     false,
		},
		PromotionPolicy:     nil,
		VisibilityThreshold: threshold,
		ScoreFrom:           ScoreFromCreated,
		ScoreFromProperty:   "",
		Function:            fn,
		HalfLifeNanos:       halfLife,
		ThresholdAgeNanos:   halfLife * 10,
		DecayFloor:          floor,
		NoDecay:             false,
	}
}

func TestNewBindingTable(t *testing.T) {
	t.Run("returns empty table", func(t *testing.T) {
		bt := NewBindingTable()
		require.NotNil(t, bt)
		assert.Equal(t, 0, bt.NodeCount())
		assert.Equal(t, 0, bt.EdgeCount())
		assert.False(t, bt.HasWildNode())
		assert.False(t, bt.HasWildEdge())
	})
}

func TestSetAndLookupNode(t *testing.T) {
	t.Run("set a node binding and look it up", func(t *testing.T) {
		bt := NewBindingTable()
		cb := newTestBinding(3_600_000_000_000, 0.05, DecayFunctionExponential)
		bt.SetNode("KnowledgeFact", cb)

		got := bt.LookupNode("KnowledgeFact")
		require.NotNil(t, got)
		assert.Same(t, cb, got)
	})
}

func TestLookupNodeNoMatchNoWildcard(t *testing.T) {
	t.Run("returns nil when no match and no wildcard", func(t *testing.T) {
		bt := NewBindingTable()
		got := bt.LookupNode("NonExistentLabel")
		assert.Nil(t, got)
	})
}

func TestLookupNodeNoMatchWithWildcard(t *testing.T) {
	t.Run("returns wildcard when key is absent but wildcard is set", func(t *testing.T) {
		bt := NewBindingTable()
		wild := newTestBinding(7_200_000_000_000, 0.01, DecayFunctionLinear)
		bt.SetWildNode(wild)

		got := bt.LookupNode("UnknownLabel")
		require.NotNil(t, got)
		assert.Same(t, wild, got)
	})

	t.Run("specific binding takes precedence over wildcard", func(t *testing.T) {
		bt := NewBindingTable()
		specific := newTestBinding(3_600_000_000_000, 0.05, DecayFunctionExponential)
		wild := newTestBinding(7_200_000_000_000, 0.01, DecayFunctionLinear)
		bt.SetNode("MemoryEpisode", specific)
		bt.SetWildNode(wild)

		got := bt.LookupNode("MemoryEpisode")
		require.NotNil(t, got)
		assert.Same(t, specific, got, "specific binding should shadow wildcard")
	})
}

func TestSetAndLookupEdge(t *testing.T) {
	t.Run("set an edge binding and look it up", func(t *testing.T) {
		bt := NewBindingTable()
		cb := newTestBinding(1_800_000_000_000, 0.02, DecayFunctionStep)
		bt.SetEdge("RELATES_TO", cb)

		got := bt.LookupEdge("RELATES_TO")
		require.NotNil(t, got)
		assert.Same(t, cb, got)
	})
}

func TestLookupEdgeWithWildcard(t *testing.T) {
	t.Run("returns nil when no match and no wildcard", func(t *testing.T) {
		bt := NewBindingTable()
		got := bt.LookupEdge("UNKNOWN_EDGE")
		assert.Nil(t, got)
	})

	t.Run("returns wildcard when edge type is absent", func(t *testing.T) {
		bt := NewBindingTable()
		wild := newTestBinding(900_000_000_000, 0.03, DecayFunctionNone)
		bt.SetWildEdge(wild)

		got := bt.LookupEdge("UNKNOWN_EDGE")
		require.NotNil(t, got)
		assert.Same(t, wild, got)
	})

	t.Run("specific edge binding takes precedence over wildcard", func(t *testing.T) {
		bt := NewBindingTable()
		specific := newTestBinding(1_800_000_000_000, 0.02, DecayFunctionStep)
		wild := newTestBinding(900_000_000_000, 0.03, DecayFunctionNone)
		bt.SetEdge("SUPERSEDES", specific)
		bt.SetWildEdge(wild)

		got := bt.LookupEdge("SUPERSEDES")
		require.NotNil(t, got)
		assert.Same(t, specific, got)
	})
}

func TestNodeCountAndEdgeCount(t *testing.T) {
	t.Run("counts reflect number of specific bindings only", func(t *testing.T) {
		bt := NewBindingTable()
		assert.Equal(t, 0, bt.NodeCount())
		assert.Equal(t, 0, bt.EdgeCount())

		bt.SetNode("LabelA", newTestBinding(1_000_000_000, 0.1, DecayFunctionExponential))
		assert.Equal(t, 1, bt.NodeCount())
		assert.Equal(t, 0, bt.EdgeCount())

		bt.SetNode("LabelB", newTestBinding(2_000_000_000, 0.2, DecayFunctionLinear))
		assert.Equal(t, 2, bt.NodeCount())

		bt.SetEdge("EDGE_X", newTestBinding(500_000_000, 0.05, DecayFunctionStep))
		assert.Equal(t, 1, bt.EdgeCount())

		// Wildcard must not affect counts.
		bt.SetWildNode(newTestBinding(3_000_000_000, 0.0, DecayFunctionNone))
		bt.SetWildEdge(newTestBinding(3_000_000_000, 0.0, DecayFunctionNone))
		assert.Equal(t, 2, bt.NodeCount(), "wildcard node must not increment NodeCount")
		assert.Equal(t, 1, bt.EdgeCount(), "wildcard edge must not increment EdgeCount")
	})
}

func TestHasWildNodeAndHasWildEdge(t *testing.T) {
	t.Run("HasWildNode and HasWildEdge reflect wildcard presence", func(t *testing.T) {
		bt := NewBindingTable()
		assert.False(t, bt.HasWildNode())
		assert.False(t, bt.HasWildEdge())

		bt.SetWildNode(newTestBinding(1_000_000_000, 0.1, DecayFunctionExponential))
		assert.True(t, bt.HasWildNode())
		assert.False(t, bt.HasWildEdge())

		bt.SetWildEdge(newTestBinding(2_000_000_000, 0.2, DecayFunctionLinear))
		assert.True(t, bt.HasWildNode())
		assert.True(t, bt.HasWildEdge())
	})

	t.Run("HasWildNode is false when only node bindings are set", func(t *testing.T) {
		bt := NewBindingTable()
		bt.SetNode("SomeLabel", newTestBinding(1_000_000_000, 0.1, DecayFunctionExponential))
		assert.False(t, bt.HasWildNode())
	})

	t.Run("HasWildEdge is false when only edge bindings are set", func(t *testing.T) {
		bt := NewBindingTable()
		bt.SetEdge("SOME_EDGE", newTestBinding(1_000_000_000, 0.1, DecayFunctionExponential))
		assert.False(t, bt.HasWildEdge())
	})
}

func TestMultipleNodeBindingsWithDifferentLabelKeys(t *testing.T) {
	t.Run("distinct label keys resolve to their own bindings independently", func(t *testing.T) {
		bt := NewBindingTable()

		bindings := map[string]*CompiledBinding{
			"KnowledgeFact":                  newTestBinding(3_600_000_000_000, 0.10, DecayFunctionExponential),
			"MemoryEpisode":                  newTestBinding(7_200_000_000_000, 0.10, DecayFunctionLinear),
			"KnowledgeFact\x00MemoryEpisode": newTestBinding(1_800_000_000_000, 0.05, DecayFunctionStep),
			"ProceduralMemory":               newTestBinding(86_400_000_000_000, 0.01, DecayFunctionNone),
		}

		for key, cb := range bindings {
			bt.SetNode(key, cb)
		}

		require.Equal(t, len(bindings), bt.NodeCount())

		for key, expected := range bindings {
			t.Run(fmt.Sprintf("key=%q", key), func(t *testing.T) {
				got := bt.LookupNode(key)
				require.NotNil(t, got)
				assert.Same(t, expected, got)
			})
		}
	})
}

func TestConcurrentReadWriteSafety(t *testing.T) {
	t.Run("concurrent SetNode and LookupNode do not race", func(t *testing.T) {
		bt := NewBindingTable()
		const goroutines = 20
		const iterations = 50

		var wg sync.WaitGroup
		wg.Add(goroutines * 2)

		// Writers: each goroutine writes to a unique key to avoid overwrite
		// contention that would complicate result verification.
		for i := 0; i < goroutines; i++ {
			i := i
			go func() {
				defer wg.Done()
				key := fmt.Sprintf("Label%d", i)
				cb := newTestBinding(int64(i+1)*1_000_000_000, 0.01*float64(i+1), DecayFunctionExponential)
				for j := 0; j < iterations; j++ {
					bt.SetNode(key, cb)
				}
			}()
		}

		// Readers: each goroutine reads arbitrary keys (may be nil, that is fine).
		for i := 0; i < goroutines; i++ {
			i := i
			go func() {
				defer wg.Done()
				key := fmt.Sprintf("Label%d", i%goroutines)
				for j := 0; j < iterations; j++ {
					_ = bt.LookupNode(key)
				}
			}()
		}

		wg.Wait()
		// After all goroutines finish, each key must resolve to a non-nil binding.
		for i := 0; i < goroutines; i++ {
			key := fmt.Sprintf("Label%d", i)
			assert.NotNil(t, bt.LookupNode(key), "key %s should be set after concurrent writes", key)
		}
	})

	t.Run("concurrent SetEdge and LookupEdge do not race", func(t *testing.T) {
		bt := NewBindingTable()
		const goroutines = 20
		const iterations = 50

		var wg sync.WaitGroup
		wg.Add(goroutines * 2)

		for i := 0; i < goroutines; i++ {
			i := i
			go func() {
				defer wg.Done()
				edgeType := fmt.Sprintf("EDGE_%d", i)
				cb := newTestBinding(int64(i+1)*500_000_000, 0.02*float64(i+1), DecayFunctionLinear)
				for j := 0; j < iterations; j++ {
					bt.SetEdge(edgeType, cb)
				}
			}()
		}

		for i := 0; i < goroutines; i++ {
			i := i
			go func() {
				defer wg.Done()
				edgeType := fmt.Sprintf("EDGE_%d", i%goroutines)
				for j := 0; j < iterations; j++ {
					_ = bt.LookupEdge(edgeType)
				}
			}()
		}

		wg.Wait()
		for i := 0; i < goroutines; i++ {
			edgeType := fmt.Sprintf("EDGE_%d", i)
			assert.NotNil(t, bt.LookupEdge(edgeType), "edge %s should be set after concurrent writes", edgeType)
		}
	})

	t.Run("concurrent wildcard set and lookup do not race", func(t *testing.T) {
		bt := NewBindingTable()
		var wg sync.WaitGroup
		const goroutines = 10
		wg.Add(goroutines * 2)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				bt.SetWildNode(newTestBinding(1_000_000_000, 0.1, DecayFunctionExponential))
				bt.SetWildEdge(newTestBinding(2_000_000_000, 0.2, DecayFunctionLinear))
			}()
		}
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				_ = bt.LookupNode("AnyLabel")
				_ = bt.LookupEdge("ANY_EDGE")
				_ = bt.HasWildNode()
				_ = bt.HasWildEdge()
			}()
		}

		wg.Wait()
	})
}
