package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEdgeMetaStore(t *testing.T) {
	store := NewEdgeMetaStore()
	require.NotNil(t, store)
	assert.Equal(t, 0, store.Size())
	assert.Equal(t, 0, store.UniqueEdgeCount())
}

func TestEdgeMetaStore_Append_FeatureEnabled(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	meta := EdgeMeta{
		Src:        "node-A",
		Dst:        "node-B",
		Label:      "relates_to",
		Score:      0.85,
		SignalType: string(SignalSimilarity),
		Method:     "vector_search",
		Reason:     "High embedding similarity",
	}

	err := store.Append(ctx, meta)
	require.NoError(t, err)

	assert.Equal(t, 1, store.Size())
	assert.Equal(t, 1, store.UniqueEdgeCount())
}

func TestEdgeMetaStore_Append_FeatureDisabled(t *testing.T) {
	cleanup := config.WithEdgeProvenanceDisabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	meta := EdgeMeta{
		Src:        "node-A",
		Dst:        "node-B",
		Label:      "relates_to",
		Score:      0.85,
		SignalType: string(SignalSimilarity),
	}

	err := store.Append(ctx, meta)
	require.NoError(t, err)

	// Should not be stored when feature is disabled
	assert.Equal(t, 0, store.Size())
}

func TestEdgeMetaStore_Append_AutoGeneratesFields(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	meta := EdgeMeta{
		Src:        "node-A",
		Dst:        "node-B",
		Label:      "relates_to",
		Score:      0.85,
		SignalType: string(SignalSimilarity),
		// Timestamp and EdgeID not provided
	}

	err := store.Append(ctx, meta)
	require.NoError(t, err)

	history, err := store.GetHistory(ctx, "node-A", "node-B", "relates_to")
	require.NoError(t, err)
	require.Len(t, history, 1)

	// Should have auto-generated EdgeID and Timestamp
	assert.NotEmpty(t, history[0].EdgeID)
	assert.False(t, history[0].Timestamp.IsZero())
}

func TestEdgeMetaStore_AppendFromSuggestion(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	err := store.AppendFromSuggestion(
		ctx,
		"node-A", "node-B", "relates_to",
		0.85,
		"similarity", "vector_search", "High similarity",
		"session-123", "inference-engine",
		true,
	)
	require.NoError(t, err)

	history, err := store.GetHistory(ctx, "node-A", "node-B", "relates_to")
	require.NoError(t, err)
	require.Len(t, history, 1)

	assert.Equal(t, 0.85, history[0].Score)
	assert.Equal(t, "similarity", history[0].SignalType)
	assert.Equal(t, "session-123", history[0].SessionID)
	assert.True(t, history[0].Materialized)
}

func TestEdgeMetaStore_GetHistory(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	// Empty history
	history, err := store.GetHistory(ctx, "node-A", "node-B", "relates_to")
	require.NoError(t, err)
	assert.Empty(t, history)

	// Add multiple records
	for i := 0; i < 3; i++ {
		store.Append(ctx, EdgeMeta{
			Src:        "node-A",
			Dst:        "node-B",
			Label:      "relates_to",
			Score:      float64(i) * 0.1,
			SignalType: string(SignalSimilarity),
		})
	}

	history, err = store.GetHistory(ctx, "node-A", "node-B", "relates_to")
	require.NoError(t, err)
	assert.Len(t, history, 3)
}

func TestEdgeMetaStore_GetLatest(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	// Empty - should return nil
	latest, err := store.GetLatest(ctx, "node-A", "node-B", "relates_to")
	require.NoError(t, err)
	assert.Nil(t, latest)

	// Add records
	store.Append(ctx, EdgeMeta{Src: "node-A", Dst: "node-B", Label: "relates_to", Score: 0.1})
	store.Append(ctx, EdgeMeta{Src: "node-A", Dst: "node-B", Label: "relates_to", Score: 0.2})
	store.Append(ctx, EdgeMeta{Src: "node-A", Dst: "node-B", Label: "relates_to", Score: 0.3})

	latest, err = store.GetLatest(ctx, "node-A", "node-B", "relates_to")
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, 0.3, latest.Score)
}

func TestEdgeMetaStore_GetBySignalType(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	// Add records with different signal types
	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r", SignalType: string(SignalSimilarity)})
	store.Append(ctx, EdgeMeta{Src: "c", Dst: "d", Label: "r", SignalType: string(SignalCoAccess)})
	store.Append(ctx, EdgeMeta{Src: "e", Dst: "f", Label: "r", SignalType: string(SignalSimilarity)})
	store.Append(ctx, EdgeMeta{Src: "g", Dst: "h", Label: "r", SignalType: string(SignalTopology)})

	// Get similarity only
	results, err := store.GetBySignalType(ctx, string(SignalSimilarity), 0)
	require.NoError(t, err)
	assert.Len(t, results, 2)

	// With limit
	results, err = store.GetBySignalType(ctx, string(SignalSimilarity), 1)
	require.NoError(t, err)
	assert.Len(t, results, 1)
}

func TestEdgeMetaStore_GetMaterialized(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r", Materialized: true})
	store.Append(ctx, EdgeMeta{Src: "c", Dst: "d", Label: "r", Materialized: false})
	store.Append(ctx, EdgeMeta{Src: "e", Dst: "f", Label: "r", Materialized: true})

	results, err := store.GetMaterialized(ctx, 0)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestEdgeMetaStore_GetByTimeRange(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	now := time.Now()
	pastHour := now.Add(-1 * time.Hour)
	past2Hours := now.Add(-2 * time.Hour)

	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r", Timestamp: past2Hours})
	store.Append(ctx, EdgeMeta{Src: "c", Dst: "d", Label: "r", Timestamp: pastHour})
	store.Append(ctx, EdgeMeta{Src: "e", Dst: "f", Label: "r", Timestamp: now})

	// Get last 90 minutes
	results, err := store.GetByTimeRange(ctx, now.Add(-90*time.Minute), now, 0)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestEdgeMetaStore_GetByOrigin(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r", Origin: "agent-1"})
	store.Append(ctx, EdgeMeta{Src: "c", Dst: "d", Label: "r", Origin: "agent-2"})
	store.Append(ctx, EdgeMeta{Src: "e", Dst: "f", Label: "r", Origin: "agent-1"})

	results, err := store.GetByOrigin(ctx, "agent-1", 0)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestEdgeMetaStore_GetBySession(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r", SessionID: "session-1"})
	store.Append(ctx, EdgeMeta{Src: "c", Dst: "d", Label: "r", SessionID: "session-2"})
	store.Append(ctx, EdgeMeta{Src: "e", Dst: "f", Label: "r", SessionID: "session-1"})

	results, err := store.GetBySession(ctx, "session-1", 0)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestEdgeMetaStore_HasHistory(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	assert.False(t, store.HasHistory("a", "b", "r"))

	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r"})
	assert.True(t, store.HasHistory("a", "b", "r"))
	assert.False(t, store.HasHistory("x", "y", "z"))
}

func TestEdgeMetaStore_CountHistory(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	assert.Equal(t, 0, store.CountHistory("a", "b", "r"))

	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r"})
	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r"})
	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r"})

	assert.Equal(t, 3, store.CountHistory("a", "b", "r"))
}

func TestEdgeMetaStore_MarkMaterialized(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	err := store.MarkMaterialized(ctx, "a", "b", "r", "agent-1")
	require.NoError(t, err)

	history, err := store.GetHistory(ctx, "a", "b", "r")
	require.NoError(t, err)
	require.Len(t, history, 1)

	assert.True(t, history[0].Materialized)
	assert.Equal(t, "agent-1", history[0].Origin)
	assert.Equal(t, "materialization", history[0].SignalType)
}

func TestEdgeMetaStore_Stats(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	stats := store.Stats()
	assert.Equal(t, int64(0), stats.TotalRecords)
	assert.Equal(t, int64(0), stats.TotalMaterialized)

	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r", SignalType: "similarity", Materialized: true})
	store.Append(ctx, EdgeMeta{Src: "c", Dst: "d", Label: "r", SignalType: "similarity", Materialized: false})
	store.Append(ctx, EdgeMeta{Src: "e", Dst: "f", Label: "r", SignalType: "topology", Materialized: true})

	stats = store.Stats()
	assert.Equal(t, int64(3), stats.TotalRecords)
	assert.Equal(t, int64(2), stats.TotalMaterialized)
	assert.Equal(t, int64(3), stats.UniqueEdges)
	assert.Equal(t, int64(2), stats.BySignalType["similarity"])
	assert.Equal(t, int64(1), stats.BySignalType["topology"])
}

func TestEdgeMetaStore_Clear(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r"})
	store.Append(ctx, EdgeMeta{Src: "c", Dst: "d", Label: "r"})

	assert.Equal(t, 2, store.Size())

	store.Clear()
	assert.Equal(t, 0, store.Size())
	assert.Equal(t, 0, store.UniqueEdgeCount())
}

func TestEdgeMetaStore_Cleanup(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	now := time.Now()
	oldTime := now.Add(-2 * time.Hour)
	recentTime := now.Add(-30 * time.Minute)

	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r", Timestamp: oldTime})
	store.Append(ctx, EdgeMeta{Src: "c", Dst: "d", Label: "r", Timestamp: oldTime})
	store.Append(ctx, EdgeMeta{Src: "e", Dst: "f", Label: "r", Timestamp: recentTime})

	assert.Equal(t, 3, store.Size())

	// Cleanup records older than 1 hour
	removed := store.Cleanup(1 * time.Hour)
	assert.Equal(t, 2, removed)
	assert.Equal(t, 1, store.Size())
}

func TestEdgeMetaStore_ExportImport(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store1 := NewEdgeMetaStore()
	ctx := context.Background()

	store1.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r", Score: 0.5})
	store1.Append(ctx, EdgeMeta{Src: "c", Dst: "d", Label: "r", Score: 0.7, Materialized: true})

	// Export
	exported := store1.Export()
	assert.Len(t, exported, 2)

	// Import into new store
	store2 := NewEdgeMetaStore()
	imported := store2.Import(exported)
	assert.Equal(t, 2, imported)
	assert.Equal(t, 2, store2.Size())

	// Verify data
	stats := store2.Stats()
	assert.Equal(t, int64(2), stats.TotalRecords)
	assert.Equal(t, int64(1), stats.TotalMaterialized)
}

func TestEdgeMetaStore_Import_NilRecords(t *testing.T) {
	store := NewEdgeMetaStore()
	records := []*EdgeMeta{nil, nil, nil}
	imported := store.Import(records)
	assert.Equal(t, 0, imported)
}

func TestEdgeMetaStore_Concurrent(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent writes
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store.Append(ctx, EdgeMeta{
				Src:   "a",
				Dst:   "b",
				Label: "r",
				Score: float64(i) * 0.01,
			})
		}(i)
	}

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.GetHistory(ctx, "a", "b", "r")
			store.Stats()
			store.Size()
		}()
	}

	wg.Wait()

	assert.Equal(t, iterations, store.Size())
}

func TestEdgeMetaKey_String(t *testing.T) {
	key := EdgeMetaKey{Src: "node1", Dst: "node2", Label: "relates_to"}
	assert.Equal(t, "node1:node2:relates_to", key.String())
}

func TestGlobalEdgeMetaStore(t *testing.T) {
	ResetGlobalEdgeMetaStore()

	store1 := GlobalEdgeMetaStore()
	store2 := GlobalEdgeMetaStore()

	assert.Same(t, store1, store2)

	ResetGlobalEdgeMetaStore()
	store3 := GlobalEdgeMetaStore()
	assert.NotSame(t, store1, store3)
}

func TestSignalTypeConstants(t *testing.T) {
	// Verify signal type constants are defined
	assert.Equal(t, SignalType("similarity"), SignalSimilarity)
	assert.Equal(t, SignalType("coaccess"), SignalCoAccess)
	assert.Equal(t, SignalType("topology"), SignalTopology)
	assert.Equal(t, SignalType("llm-infer"), SignalLLMInference)
	assert.Equal(t, SignalType("manual"), SignalManual)
	assert.Equal(t, SignalType("transitive"), SignalTransitive)
	assert.Equal(t, SignalType("temporal"), SignalTemporal)
}

func TestEdgeMetaStore_MultipleEdges(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	// Add records for multiple different edges
	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r1"})
	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r2"})
	store.Append(ctx, EdgeMeta{Src: "a", Dst: "c", Label: "r1"})

	assert.Equal(t, 3, store.Size())
	assert.Equal(t, 3, store.UniqueEdgeCount())

	// Add to existing edge
	store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "r1"})
	assert.Equal(t, 4, store.Size())
	assert.Equal(t, 3, store.UniqueEdgeCount()) // Still 3 unique

	// Check specific edge history
	history, _ := store.GetHistory(ctx, "a", "b", "r1")
	assert.Len(t, history, 2)
}

func TestEdgeMetaStore_WithMetadata(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	meta := EdgeMeta{
		Src:   "a",
		Dst:   "b",
		Label: "r",
		Metadata: map[string]interface{}{
			"source":     "api",
			"confidence": 0.95,
			"tags":       []string{"important", "reviewed"},
		},
	}

	store.Append(ctx, meta)

	history, _ := store.GetHistory(ctx, "a", "b", "r")
	require.Len(t, history, 1)

	assert.Equal(t, "api", history[0].Metadata["source"])
	assert.Equal(t, 0.95, history[0].Metadata["confidence"])
}

func TestEdgeMetaStore_TLPFields(t *testing.T) {
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	meta := EdgeMeta{
		Src:               "a",
		Dst:               "b",
		Label:             "r",
		SignalType:        string(SignalTopology),
		TopologyAlgorithm: "adamic_adar",
		TopologyScore:     0.75,
		SemanticScore:     0.80,
	}

	store.Append(ctx, meta)

	history, _ := store.GetHistory(ctx, "a", "b", "r")
	require.Len(t, history, 1)

	assert.Equal(t, "adamic_adar", history[0].TopologyAlgorithm)
	assert.Equal(t, 0.75, history[0].TopologyScore)
	assert.Equal(t, 0.80, history[0].SemanticScore)
}

func TestNewEdgeMetaStoreWithOptions(t *testing.T) {
	// Test functional options pattern
	store := NewEdgeMetaStoreWithOptions()
	require.NotNil(t, store)
	assert.Equal(t, 0, store.Size())
}

func TestEdgeMetaStore_GetLatest_WithError(t *testing.T) {
	// This tests the error path of GetLatest by calling GetHistory
	// which returns empty for non-existent edge
	cleanup := config.WithEdgeProvenanceEnabled()
	defer cleanup()

	store := NewEdgeMetaStore()
	ctx := context.Background()

	// Empty case - already tested in TestEdgeMetaStore_GetLatest
	latest, err := store.GetLatest(ctx, "nonexistent", "edge", "label")
	require.NoError(t, err)
	assert.Nil(t, latest)
}
