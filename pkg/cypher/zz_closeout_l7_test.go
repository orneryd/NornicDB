package cypher

// Close-out lane L7 (scratch branch only; test-side, not product code).
// L-H7-013: under -race in the whole package, TestRelationshipIDPinnedEdgeFallbackShapes
// reports a data race in its own adjacencyReadSpy.count (relationship_id_pinned_edge_test.go:45)
// reached from traverseGraphParallel. Alone it does not traverse in parallel: the
// default ParallelConfig needs 1000 start nodes (parallel.go:60, traversal.go:1413).
// TestParallelFilterEdges (parallel_test.go:305) leaves MinBatchSize 1 and never
// restores it, so every later traversal with a start node runs in parallel workers
// that call the spy concurrently.
//
// Run with -race -run '^TestL7013':
//   A victim alone                                        expected PASS
//   B TestParallelFilterEdges then victim                 expected FAIL (race at the spy)
//   C TestParallelFilterEdges, default config restored, victim   expected PASS
// On scratch/v152-closeout-L7-bypass013 (spy counters under a mutex), B is expected to PASS.

import "testing"

func TestL7013AVictimAlone(t *testing.T) {
	t.Logf("L7_013 parallel config before victim: %+v", GetParallelConfig())
	t.Run("TestRelationshipIDPinnedEdgeFallbackShapes", TestRelationshipIDPinnedEdgeFallbackShapes)
}

func TestL7013BConfigLeftThenVictim(t *testing.T) {
	saved := GetParallelConfig()
	t.Cleanup(func() { SetParallelConfig(saved) })
	t.Run("TestParallelFilterEdges", TestParallelFilterEdges)
	t.Logf("L7_013 parallel config after TestParallelFilterEdges: %+v", GetParallelConfig())
	t.Run("TestRelationshipIDPinnedEdgeFallbackShapes", TestRelationshipIDPinnedEdgeFallbackShapes)
}

func TestL7013CConfigRestoredThenVictim(t *testing.T) {
	saved := GetParallelConfig()
	t.Cleanup(func() { SetParallelConfig(saved) })
	t.Run("TestParallelFilterEdges", TestParallelFilterEdges)
	SetParallelConfig(DefaultParallelConfig())
	t.Logf("L7_013 parallel config restored to default: %+v", GetParallelConfig())
	t.Run("TestRelationshipIDPinnedEdgeFallbackShapes", TestRelationshipIDPinnedEdgeFallbackShapes)
}
