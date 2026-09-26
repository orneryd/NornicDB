package storage

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type mvccBenchChain struct {
	engine   *MemoryEngine
	nodeID   NodeID
	versions []MVCCVersion
}

type mvccTombstoneBenchChain struct {
	engine            *MemoryEngine
	nodeID            NodeID
	liveVersions      []MVCCVersion
	tombstoneVersions []MVCCVersion
}

// mvccBenchRetainedVersions is the history the MVCC benchmarks keep per
// key: more than the longest chain they build (1,000 cycles of a live
// version and a tombstone), so every version they read or prune exists.
const mvccBenchRetainedVersions = 4096

// newMVCCHistoryBenchEngine is an in-memory engine that keeps
// mvccBenchRetainedVersions versions per key. The production default is
// head-only (MaxVersionsPerKey = 0), under which prior versions aren't
// archived, so the history and prune benchmarks would read or prune
// nothing (#731).
func newMVCCHistoryBenchEngine(b *testing.B) *MemoryEngine {
	b.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
		EngineOptions: EngineOptions{
			RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: mvccBenchRetainedVersions},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return &MemoryEngine{BadgerEngine: engine}
}

func seedConcurrentPruneBenchEngine(b *testing.B, nodeCount, initialVersions int) *MemoryEngine {
	b.Helper()
	engine := newMVCCHistoryBenchEngine(b)
	for i := 0; i < nodeCount; i++ {
		id := NodeID(prefixTestID(fmt.Sprintf("bench-prune-concurrent-%03d", i)))
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Bench"}, Properties: map[string]any{"version": 1}})
		if err != nil {
			_ = engine.Close()
			b.Fatal(err)
		}
		for version := 2; version <= initialVersions; version++ {
			if err := engine.UpdateNode(&Node{ID: id, Labels: []string{"Bench"}, Properties: map[string]any{"version": version, "node": i}}); err != nil {
				_ = engine.Close()
				b.Fatal(err)
			}
		}
	}
	return engine
}

func startConcurrentPruneBenchWriter(engine *MemoryEngine, nodeCount int) func() error {
	var stop int32
	var writerWG sync.WaitGroup
	errCh := make(chan error, 1)

	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		counter := 0
		for atomic.LoadInt32(&stop) == 0 {
			for i := 0; i < nodeCount; i++ {
				if atomic.LoadInt32(&stop) != 0 {
					return
				}
				id := NodeID(prefixTestID(fmt.Sprintf("bench-prune-concurrent-%03d", i)))
				err := engine.UpdateNode(&Node{ID: id, Labels: []string{"Bench"}, Properties: map[string]any{"version": counter + 1000, "writer": i}})
				if err != nil {
					if err == ErrStorageClosed {
						return
					}
					select {
					case errCh <- err:
					default:
					}
					atomic.StoreInt32(&stop, 1)
					return
				}
				counter++
			}
			runtime.Gosched()
		}
	}()

	return func() error {
		atomic.StoreInt32(&stop, 1)
		writerWG.Wait()
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	}
}

func buildMVCCBenchChain(b *testing.B, chainLength int) mvccBenchChain {
	b.Helper()
	engine := newMVCCHistoryBenchEngine(b)
	b.Cleanup(func() { _ = engine.Close() })

	nodeID := NodeID(prefixTestID(fmt.Sprintf("bench-chain-%d", chainLength)))
	versions := make([]MVCCVersion, 0, chainLength)
	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Bench"}, Properties: map[string]any{"version": 1}})
	if err != nil {
		b.Fatal(err)
	}
	head, err := engine.GetNodeCurrentHead(nodeID)
	if err != nil {
		b.Fatal(err)
	}
	versions = append(versions, head.Version)

	for version := 2; version <= chainLength; version++ {
		if err := engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Bench"}, Properties: map[string]any{"version": version}}); err != nil {
			b.Fatal(err)
		}
		head, err := engine.GetNodeCurrentHead(nodeID)
		if err != nil {
			b.Fatal(err)
		}
		versions = append(versions, head.Version)
	}

	return mvccBenchChain{engine: engine, nodeID: nodeID, versions: versions}
}

func benchmarkMVCCChainPosition(b *testing.B, chainLength int, selector func([]MVCCVersion) MVCCVersion) {
	chain := buildMVCCBenchChain(b, chainLength)
	target := selector(chain.versions)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node, err := chain.engine.GetNodeVisibleAt(chain.nodeID, target)
		if err != nil {
			b.Fatal(err)
		}
		if node == nil {
			b.Fatal("expected visible node")
		}
	}
}

func buildMVCCTombstoneBenchChain(b *testing.B, cycles int) mvccTombstoneBenchChain {
	b.Helper()
	engine := newMVCCHistoryBenchEngine(b)
	b.Cleanup(func() { _ = engine.Close() })

	nodeID := NodeID(prefixTestID(fmt.Sprintf("bench-tombstone-chain-%d", cycles)))
	liveVersions := make([]MVCCVersion, 0, cycles)
	tombstoneVersions := make([]MVCCVersion, 0, max(cycles-1, 0))

	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Bench"}, Properties: map[string]any{"cycle": 0, "state": "live"}})
	if err != nil {
		b.Fatal(err)
	}
	head, err := engine.GetNodeCurrentHead(nodeID)
	if err != nil {
		b.Fatal(err)
	}
	liveVersions = append(liveVersions, head.Version)

	for cycle := 1; cycle < cycles; cycle++ {
		if err := engine.DeleteNode(nodeID); err != nil {
			b.Fatal(err)
		}
		head, err = engine.GetNodeCurrentHead(nodeID)
		if err != nil {
			b.Fatal(err)
		}
		tombstoneVersions = append(tombstoneVersions, head.Version)

		_, err = engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Bench"}, Properties: map[string]any{"cycle": cycle, "state": "live"}})
		if err != nil {
			b.Fatal(err)
		}
		head, err = engine.GetNodeCurrentHead(nodeID)
		if err != nil {
			b.Fatal(err)
		}
		liveVersions = append(liveVersions, head.Version)
	}

	return mvccTombstoneBenchChain{
		engine:            engine,
		nodeID:            nodeID,
		liveVersions:      liveVersions,
		tombstoneVersions: tombstoneVersions,
	}
}

func benchmarkMVCCTombstonePosition(b *testing.B, cycles int, selector func(mvccTombstoneBenchChain) MVCCVersion, expectFound bool) {
	chain := buildMVCCTombstoneBenchChain(b, cycles)
	target := selector(chain)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node, err := chain.engine.GetNodeVisibleAt(chain.nodeID, target)
		if expectFound {
			if err != nil {
				b.Fatal(err)
			}
			if node == nil {
				b.Fatal("expected visible node")
			}
			continue
		}
		if err != ErrNotFound {
			b.Fatalf("expected ErrNotFound, got %v", err)
		}
		if node != nil {
			b.Fatal("expected nil node for tombstone snapshot")
		}
	}
}

func oldestRetainedLiveVersion(b *testing.B, chain mvccTombstoneBenchChain) MVCCVersion {
	b.Helper()
	for _, version := range chain.liveVersions {
		var retained bool
		err := chain.engine.withView(func(txn *badger.Txn) error {
			_, actualVersion, loadErr := chain.engine.loadNodeMVCCRecordAtOrBeforeInTxn(txn, chain.nodeID, version)
			if loadErr != nil {
				return loadErr
			}
			retained = actualVersion.Compare(version) == 0
			return nil
		})
		if err == nil && retained {
			return version
		}
		if err != nil && err != ErrNotFound {
			b.Fatal(err)
		}
	}
	b.Fatal("expected at least one retained live version after prune")
	return MVCCVersion{}
}

func benchmarkMVCCTombstonePositionAfterPrune(b *testing.B, cycles, maxVersionsPerKey int, flatten bool) {
	chain := buildMVCCTombstoneBenchChain(b, cycles)
	deleted, err := chain.engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: maxVersionsPerKey})
	if err != nil {
		b.Fatal(err)
	}
	totalVersions := len(chain.liveVersions) + len(chain.tombstoneVersions)
	if totalVersions > maxVersionsPerKey+1 && deleted == 0 {
		b.Fatal("expected prune to delete versions")
	}
	if flatten {
		if err := chain.engine.db.Flatten(1); err != nil {
			b.Fatal(err)
		}
	}
	target := oldestRetainedLiveVersion(b, chain)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node, err := chain.engine.GetNodeVisibleAt(chain.nodeID, target)
		if err != nil {
			b.Fatal(err)
		}
		if node == nil {
			b.Fatal("expected retained live node")
		}
	}
}

func BenchmarkBadgerEngine_CreateNode_MVCC(b *testing.B) {
	engine := NewMemoryEngine()
	b.Cleanup(func() { _ = engine.Close() })
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node := &Node{
			ID:         NodeID(prefixTestID(fmt.Sprintf("bench-mvcc-create-%06d", i))),
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}
		if _, err := engine.CreateNode(node); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadgerEngine_UpdateNode_MVCC(b *testing.B) {
	engine := NewMemoryEngine()
	b.Cleanup(func() { _ = engine.Close() })

	nodeID := NodeID(prefixTestID("bench-mvcc-update-target"))
	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Benchmark"}, Properties: map[string]any{"index": 0}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Benchmark"}, Properties: map[string]any{"index": i}}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadgerEngine_GetNodeVisibleAt(b *testing.B) {
	for _, chainLength := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("chain=%d/latest", chainLength), func(b *testing.B) {
			benchmarkMVCCChainPosition(b, chainLength, func(versions []MVCCVersion) MVCCVersion {
				return versions[len(versions)-1]
			})
		})
		b.Run(fmt.Sprintf("chain=%d/middle", chainLength), func(b *testing.B) {
			benchmarkMVCCChainPosition(b, chainLength, func(versions []MVCCVersion) MVCCVersion {
				return versions[len(versions)/2]
			})
		})
		b.Run(fmt.Sprintf("chain=%d/oldest", chainLength), func(b *testing.B) {
			benchmarkMVCCChainPosition(b, chainLength, func(versions []MVCCVersion) MVCCVersion {
				return versions[0]
			})
		})
	}
}

func BenchmarkBadgerEngine_GetNodeVisibleAt_CurrentHeadGraphitiVector(b *testing.B) {
	engine := NewMemoryEngine()
	b.Cleanup(func() { _ = engine.Close() })

	nodeID := NodeID(prefixTestID("bench-mvcc-current-graphiti-vector"))
	embedding := make([]float64, 1024)
	for i := range embedding {
		embedding[i] = float64(i%17) / 17
	}
	_, err := engine.CreateNode(&Node{
		ID:     nodeID,
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":      "entity",
			"embedding": embedding,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	head, err := engine.GetNodeCurrentHead(nodeID)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node, err := engine.GetNodeVisibleAt(nodeID, head.Version)
		if err != nil {
			b.Fatal(err)
		}
		if node.Properties["name"] != "entity" {
			b.Fatalf("unexpected node name: %v", node.Properties["name"])
		}
	}
}

func BenchmarkBadgerEngine_GetNodesByLabelVisibleAt(b *testing.B) {
	engine := NewMemoryEngine()
	b.Cleanup(func() { _ = engine.Close() })
	b.ReportAllocs()

	for i := 0; i < 200; i++ {
		id := NodeID(prefixTestID(fmt.Sprintf("bench-mvcc-label-%03d", i)))
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"BenchLabel"}, Properties: map[string]any{"index": i}})
		if err != nil {
			b.Fatal(err)
		}
		if err := engine.UpdateNode(&Node{ID: id, Labels: []string{"BenchLabel"}, Properties: map[string]any{"index": i, "updated": true}}); err != nil {
			b.Fatal(err)
		}
	}
	probeID := NodeID(prefixTestID("bench-mvcc-label-000"))
	head, err := engine.GetNodeCurrentHead(probeID)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := engine.GetNodesByLabelVisibleAt("BenchLabel", head.Version)
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) == 0 {
			b.Fatal("expected visible nodes")
		}
	}
}

func BenchmarkBadgerEngine_GetNodeVisibleAt_TombstoneChain(b *testing.B) {
	for _, cycles := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("cycles=%d/latest-live", cycles), func(b *testing.B) {
			benchmarkMVCCTombstonePosition(b, cycles, func(chain mvccTombstoneBenchChain) MVCCVersion {
				return chain.liveVersions[len(chain.liveVersions)-1]
			}, true)
		})
		b.Run(fmt.Sprintf("cycles=%d/oldest-live", cycles), func(b *testing.B) {
			benchmarkMVCCTombstonePosition(b, cycles, func(chain mvccTombstoneBenchChain) MVCCVersion {
				return chain.liveVersions[0]
			}, true)
		})
		b.Run(fmt.Sprintf("cycles=%d/latest-tombstone", cycles), func(b *testing.B) {
			benchmarkMVCCTombstonePosition(b, cycles, func(chain mvccTombstoneBenchChain) MVCCVersion {
				return chain.tombstoneVersions[len(chain.tombstoneVersions)-1]
			}, false)
		})
		b.Run(fmt.Sprintf("cycles=%d/pruned-keep=100/oldest-retained-live", cycles), func(b *testing.B) {
			benchmarkMVCCTombstonePositionAfterPrune(b, cycles, 100, false)
		})
		b.Run(fmt.Sprintf("cycles=%d/pruned-keep=100+flatten/oldest-retained-live", cycles), func(b *testing.B) {
			benchmarkMVCCTombstonePositionAfterPrune(b, cycles, 100, true)
		})
	}
}

func BenchmarkBadgerEngine_PruneMVCCVersions(b *testing.B) {
	for _, versionsPerKey := range []int{16, 128, 1024} {
		b.Run(fmt.Sprintf("versions=%d", versionsPerKey), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				engine := newMVCCHistoryBenchEngine(b)
				nodeID := NodeID(prefixTestID(fmt.Sprintf("bench-prune-%d-%03d", versionsPerKey, i)))
				_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Bench"}, Properties: map[string]any{"version": 1}})
				if err != nil {
					_ = engine.Close()
					b.Fatal(err)
				}
				for version := 2; version <= versionsPerKey; version++ {
					if err := engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Bench"}, Properties: map[string]any{"version": version}}); err != nil {
						_ = engine.Close()
						b.Fatal(err)
					}
				}
				deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 2})
				if err != nil {
					_ = engine.Close()
					b.Fatal(err)
				}
				if deleted == 0 {
					_ = engine.Close()
					b.Fatal("expected prune to delete versions")
				}
				if err := engine.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBadgerEngine_PruneMVCCVersions_WithConcurrentWrites(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		engine := seedConcurrentPruneBenchEngine(b, 64, 8)
		stopWriter := startConcurrentPruneBenchWriter(engine, 64)
		time.Sleep(20 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		b.StartTimer()
		deleted, err := engine.PruneMVCCVersions(ctx, MVCCPruneOptions{MaxVersionsPerKey: 4})
		b.StopTimer()
		cancel()
		writerErr := stopWriter()
		closeErr := engine.Close()
		if err != nil {
			b.Fatal(err)
		}
		if writerErr != nil {
			b.Fatal(writerErr)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
		b.ReportMetric(float64(deleted), "versions/prune")
	}
}
