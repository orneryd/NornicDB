package storage_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type conditionalReadEngine struct {
	*storage.MemoryEngine
	available, transparent bool
}

func (e *conditionalReadEngine) ReadCapabilityAvailable(reflect.Type) bool { return e.available }
func (e *conditionalReadEngine) ReadCapabilitySource() (storage.Engine, bool) {
	return e.MemoryEngine, e.transparent
}

type projectedReadAlias interface {
	StreamNodesByLabelProjected(string, []string, func(*storage.Node) error) error
}
type combinedReadAlias interface {
	projectedReadAlias
	storage.QueryNodeReader
}
type narrowHistoricalRead interface {
	GetNodesByLabelVisibleAt(string, storage.MVCCVersion) ([]*storage.Node, error)
}
type temporalCurrentRead interface {
	IsCurrentTemporalNode(*storage.Node, time.Time) (bool, error)
}
type materializedRead interface{ RecordMaterializedAccess(string) }

func capabilityAvailable[T any](engine storage.Engine) bool {
	_, ok := capabilityForTest[T](engine)
	return ok
}

// Availability is a live operation contract, including structural aliases and
// combined interfaces. The method sets never change during these comparisons.
func TestReadCapabilityDifferentialConditionalViews(t *testing.T) {
	core := storage.NewMemoryEngineWithMVCCHistory()
	t.Cleanup(func() { require.NoError(t, core.Close()) })
	view := storage.NewNamespacedEngine(core, "tenant")
	_, err := view.CreateNode(&storage.Node{ID: "n", Labels: []string{"Conditional"}, Properties: map[string]interface{}{"v": int64(1)}})
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "hidden", Labels: []string{"Conditional"}, Properties: map[string]interface{}{"v": int64(66)}})
	require.NoError(t, err)
	require.NoError(t, view.SetManualSuppression(storage.StoredEntityNodeKind, "hidden", true))
	conditional := &conditionalReadEngine{MemoryEngine: core}
	requests := []struct {
		name      string
		available func(storage.Engine) bool
	}{
		{"projected", capabilityAvailable[storage.ProjectedLabelNodeReader]},
		{"projected-alias", capabilityAvailable[projectedReadAlias]},
		{"combined", capabilityAvailable[combinedReadAlias]},
		{"batch", capabilityAvailable[storage.BatchNodeWithoutEmbeddingsReader]},
		{"historical", capabilityAvailable[storage.MVCCIndexedVisibilityEngine]},
		{"historical-alias", capabilityAvailable[narrowHistoricalRead]},
		{"snapshot", capabilityAvailable[storage.MVCCVisibilityEngine]},
		{"heads", capabilityAvailable[storage.MVCCHeadEngine]},
		{"temporal", capabilityAvailable[storage.TemporalLookupEngine]},
		{"temporal-current", capabilityAvailable[temporalCurrentRead]},
		{"materialized", capabilityAvailable[materializedRead]},
		{"property-keys", capabilityAvailable[storage.PropertyKeyLookup]},
		{"read-snapshot", capabilityAvailable[storage.ReadSnapshotter]},
	}
	for _, transparent := range []bool{false, true} {
		conditional.transparent = transparent
		for _, available := range []bool{true, false, true} {
			conditional.available = available
			namespace := storage.NewNamespacedEngine(conditional, "tenant")
			for name, engine := range map[string]storage.Engine{
				"namespace":    namespace,
				"TW-namespace": storage.NewTracedEngine(storage.NewWALEngine(namespace, nil)),
				"namespace-WT": storage.NewNamespacedEngine(storage.NewWALEngine(storage.NewTracedEngine(conditional), nil), "tenant"),
			} {
				identity, found := storage.ViewMetadataForEngine[interface{ Namespace() string }](engine)
				require.True(t, found)
				require.Equal(t, "tenant", identity.Namespace())
				visibility, found := storage.ViewMetadataForEngine[interface{ HasManualSuppressions() bool }](engine)
				require.True(t, found)
				require.True(t, visibility.HasManualSuppressions(), "data unavailability cannot hide read visibility policy")
				revision, found := storage.ViewMetadataForEngine[storage.GraphMutationVersionProvider](engine)
				require.True(t, found)
				actualRevision, actualSupported := revision.GraphMutationVersion()
				expectedRevision, expectedSupported := view.GraphMutationVersion()
				require.Equal(t, expectedSupported, actualSupported)
				require.Equal(t, expectedRevision, actualRevision)
				for _, request := range requests {
					want := available
					if request.name == "projected" || request.name == "projected-alias" || request.name == "combined" {
						want = true
					}
					require.Equal(t, want, request.available(engine), "%s/%s transparent=%v", name, request.name, transparent)
				}
				// Complete owner-side core fallbacks remain useful even when
				// the underlying optional reads are unavailable.
				projected, ok := capabilityForTest[storage.ProjectedNodeReader](engine)
				require.True(t, ok)
				node, err := projected.GetNodeProjected("n", []string{"v"})
				require.NoError(t, err)
				require.Equal(t, int64(1), node.Properties["v"])
				query, ok := capabilityForTest[storage.QueryNodeReader](engine)
				require.True(t, ok)
				node, err = query.GetQueryNode("n")
				require.NoError(t, err)
				require.Equal(t, int64(1), node.Properties["v"])
				adjacency, ok := capabilityForTest[storage.AdjacentEdgesEngine](engine)
				require.True(t, ok)
				out, in, err := adjacency.GetAdjacentEdges("n")
				require.NoError(t, err)
				require.Empty(t, out)
				require.Empty(t, in)
			}
		}
	}
}

type unavailableBatchEngine struct {
	*storage.MemoryEngine
	transparent bool
}

func (*unavailableBatchEngine) BatchGetNodesWithoutEmbeddingsSupported() bool { return false }
func (e *unavailableBatchEngine) ReadCapabilitySource() (storage.Engine, bool) {
	return e.MemoryEngine, e.transparent
}

func TestReadCapabilityDifferentialLegacyBatchAvailability(t *testing.T) {
	core := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, core.Close()) })
	conditional := &unavailableBatchEngine{MemoryEngine: core}
	view := storage.NewNamespacedEngine(conditional, "tenant")
	_, err := view.CreateNode(&storage.Node{ID: "n", Labels: []string{"Batch"}, Properties: map[string]interface{}{"v": int64(1)}})
	require.NoError(t, err)
	for _, transparent := range []bool{false, true} {
		conditional.transparent = transparent
		for _, engine := range []storage.Engine{view, storage.NewWALEngine(storage.NewTracedEngine(view), nil), storage.NewTracedEngine(storage.NewWALEngine(view, nil))} {
			require.False(t, capabilityAvailable[storage.BatchNodeWithoutEmbeddingsReader](engine), "legacy support=false at transparent=%v must remain authoritative", transparent)
			nodes, err := engine.BatchGetNodes([]storage.NodeID{"n"})
			require.NoError(t, err)
			require.Equal(t, int64(1), nodes["n"].Properties["v"])
		}
	}
}

// A later constituent without a projected reader must be detected before an
// earlier constituent delivers rows. Core aggregation is the differential
// oracle; namespace and composite identity stay intact during discovery.
func TestReadCapabilityDifferentialCompositeAvailability(t *testing.T) {
	base := storage.NewMemoryEngineWithMVCCHistory()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	conditional := &conditionalReadEngine{MemoryEngine: base, available: true}
	a := storage.NewNamespacedEngine(base, "a")
	b := storage.NewNamespacedEngine(conditional, "b")
	for _, seed := range []struct {
		engine storage.Engine
		id     storage.NodeID
		v      int64
	}{{a, "first", 1}, {b, "second", 2}} {
		_, err := seed.engine.CreateNode(&storage.Node{ID: seed.id, Labels: []string{"Composite"}, Properties: map[string]interface{}{"v": seed.v}})
		require.NoError(t, err)
	}
	composite := storage.NewCompositeEngine(map[string]storage.Engine{"a": a, "b": b}, map[string]string{"a": "a", "b": "b"}, map[string]string{"a": "read_write", "b": "read_write"})
	for _, available := range []bool{true, false, true} {
		conditional.available = available
		for _, engine := range []storage.Engine{composite, storage.NewTracedEngine(storage.NewWALEngine(composite, nil)), storage.NewWALEngine(storage.NewTracedEngine(composite), nil)} {
			owner, ok := capabilityForTest[*storage.CompositeEngine](engine)
			require.True(t, ok)
			require.Same(t, composite, owner)
			require.True(t, capabilityAvailable[storage.ProjectedLabelNodeReader](engine), "the composite owns an ordinary per-child projected fallback")
			for _, request := range []func(storage.Engine) bool{capabilityAvailable[storage.MVCCIndexedVisibilityEngine], capabilityAvailable[storage.MVCCVisibilityEngine], capabilityAvailable[storage.MVCCHeadEngine], capabilityAvailable[narrowHistoricalRead]} {
				require.Equal(t, available, request(engine))
			}
			expected, err := engine.GetNodesByLabel("Composite")
			require.NoError(t, err)
			var actual []*storage.Node
			if reader, ok := capabilityForTest[storage.ProjectedLabelNodeReader](engine); ok {
				err = reader.StreamNodesByLabelProjected("Composite", []string{"v"}, func(node *storage.Node) error { actual = append(actual, node); return nil })
			} else {
				actual, err = engine.GetNodesByLabel("Composite")
			}
			require.NoError(t, err)
			values := func(nodes []*storage.Node) map[storage.NodeID]interface{} {
				result := map[storage.NodeID]interface{}{}
				for _, n := range nodes {
					result[n.ID] = n.Properties["v"]
				}
				return result
			}
			require.Equal(t, values(expected), values(actual))
			require.Len(t, actual, 2)
		}
	}
	// Ordinary core streams remain available in the same view.
	var count int
	require.NoError(t, composite.StreamNodes(context.Background(), func(*storage.Node) error { count++; return nil }))
	require.Equal(t, 2, count)
}
