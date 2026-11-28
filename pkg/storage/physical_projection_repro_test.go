package storage

import (
	"expvar"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPhysicalProjectionReadsOnlySelectedBytes is a fail-first public-path
// repro for the missing physical projection on a large encrypted node value.
func TestPhysicalProjectionReadsOnlySelectedBytes(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	large := make([]byte, 2<<20)
	var random uint64 = 0x9e3779b97f4a7c15
	for i := range large {
		random ^= random << 13
		random ^= random >> 7
		random ^= random << 17
		large[i] = byte(random)
	}
	for _, experimental := range []bool{false, true} {
		name := "projected"
		if experimental {
			name = "projected_experimental"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			open := func() *BadgerEngine {
				engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, EncryptionKey: key, ExperimentalLayout: experimental, Projected: true})
				require.NoError(t, err)
				return engine
			}
			engine := open()
			id, err := engine.CreateNode(t.Context(), &Node{ID: "ns:physical-projection", Labels: []string{"Doc"}, Properties: map[string]any{
				"small": int64(7), "large": large,
			}})
			require.NoError(t, err)
			require.NoError(t, engine.Close())

			engine = open()
			defer engine.Close()
			metric, ok := expvar.Get("badger_read_bytes_vlog").(*expvar.Int)
			require.True(t, ok)
			before := metric.Value()
			node, err := engine.GetNodeProjected(t.Context(), id, []string{"small"})
			require.NoError(t, err)
			require.Equal(t, map[string]any{"small": int64(7)}, node.Properties)
			require.Less(t, metric.Value()-before, int64(64<<10), "a projected scalar must not read a 2 MiB unrelated value")
			before = metric.Value()
			var scanned []*Node
			err = engine.StreamNodesWithOptions(t.Context(), StreamNodesOptions{Prefix: "ns:", Projection: []string{"small"}}, func(n *Node) error {
				scanned = append(scanned, n)
				return nil
			})
			require.NoError(t, err)
			require.Len(t, scanned, 1)
			require.Equal(t, map[string]any{"small": int64(7)}, scanned[0].Properties)
			require.Less(t, metric.Value()-before, int64(64<<10), "a projected scan must not read a 2 MiB unrelated value")
			before = metric.Value()
			scanned = nil
			err = engine.StreamNodesWithOptions(t.Context(), StreamNodesOptions{
				Prefix: "ns:", Projection: []string{"small"},
				PropertyFilter: func(properties map[string]any) bool { return properties["small"] == int64(7) },
			}, func(n *Node) error {
				scanned = append(scanned, n)
				return nil
			})
			require.NoError(t, err)
			require.Len(t, scanned, 1)
			require.Less(t, metric.Value()-before, int64(64<<10), "a filtered projected scan must not read an unrelated value")
			before = metric.Value()
			scanned = nil
			err = engine.StreamNodesWithOptions(t.Context(), StreamNodesOptions{
				Prefix: "ns:", Label: "Doc", Projection: []string{"small"},
				PropertyFilter: func(properties map[string]any) bool { return properties["small"] == int64(7) },
			}, func(n *Node) error { scanned = append(scanned, n); return nil })
			require.NoError(t, err)
			require.Len(t, scanned, 1)
			require.Less(t, metric.Value()-before, int64(64<<10), "a label-projected scan must not read an unrelated value")
			before = metric.Value()
			scanned = nil
			err = engine.StreamNodesWithOptions(t.Context(), StreamNodesOptions{
				Prefix: "ns:", Label: "Doc", PropertyFilterProjection: []string{"small"},
				PropertyFilter: func(properties map[string]any) bool { return properties["small"] == int64(999) },
			}, func(n *Node) error { scanned = append(scanned, n); return nil })
			require.NoError(t, err)
			require.Empty(t, scanned)
			require.Less(t, metric.Value()-before, int64(64<<10), "a rejected hinted label scan must not read an unrelated value")
			full, err := engine.GetNode(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, large, full.Properties["large"])
			reader, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
			require.NoError(t, err)
			defer reader.Rollback()
			pinned, err := reader.GetNodeProjected(t.Context(), id, []string{"small"})
			require.NoError(t, err)
			require.Equal(t, int64(7), pinned.Properties["small"])
			full.Properties["small"] = int64(8)
			require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: full, Policy: ClientNodeEdits}))
			engine.cacheDeleteNode(id, nodeCacheAllForms)
			before = metric.Value()
			updated, err := engine.GetNodeProjected(t.Context(), id, []string{"small"})
			require.NoError(t, err)
			require.Equal(t, int64(8), updated.Properties["small"])
			require.Less(t, metric.Value()-before, int64(64<<10))
			pinned, err = reader.GetNodeProjected(t.Context(), id, []string{"small"})
			require.NoError(t, err)
			require.Equal(t, int64(7), pinned.Properties["small"])
		})
	}
}

func TestPhysicalProjectionSurvivesTableCompaction(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(), Projected: true,
		EncryptionKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	require.NoError(t, err)
	defer engine.Close()
	large := make([]byte, 2<<20)
	for i := range large {
		large[i] = byte(i*131 + i/257)
	}
	id, err := engine.CreateNode(t.Context(), &Node{ID: "ns:compact", Properties: map[string]any{"small": int64(31), "large": large}})
	require.NoError(t, err)
	require.NoError(t, engine.db.DB.Flatten(1))
	engine.cacheDeleteNode(id, nodeCacheAllForms)
	projected, err := engine.GetNodeProjected(t.Context(), id, []string{"small"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"small": int64(31)}, projected.Properties)
	full, err := engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, large, full.Properties["large"])
}

func BenchmarkPhysicalProjectionWriteLargeCurrent(b *testing.B) {
	benchmarkPhysicalProjectionWrite(b, false, 2<<20)
}

func BenchmarkPhysicalProjectionWriteLargeProjected(b *testing.B) {
	benchmarkPhysicalProjectionWrite(b, true, 2<<20)
}

func BenchmarkPhysicalProjectionWriteSmallCurrent(b *testing.B) {
	benchmarkPhysicalProjectionWrite(b, false, 1<<10)
}

func BenchmarkPhysicalProjectionWriteSmallProjected(b *testing.B) {
	benchmarkPhysicalProjectionWrite(b, true, 1<<10)
}

func benchmarkPhysicalProjectionWrite(b *testing.B, projected bool, size int) {
	large := make([]byte, size)
	var random uint64 = 0x9e3779b97f4a7c15
	for i := range large {
		random ^= random << 13
		random ^= random >> 7
		random ^= random << 17
		large[i] = byte(random)
	}
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: b.TempDir(), EncryptionKey: []byte("0123456789abcdef0123456789abcdef"), Projected: projected,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.CreateNode(b.Context(), &Node{ID: NodeID(fmt.Sprintf("ns:%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{
			"small": int64(i), "large": large,
		}})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPhysicalProjectionCurrent records the warm-device cost and Badger
// value-log bytes of a narrow read from a large node. It deliberately evicts
// NornicDB's node cache before each iteration so the storage path is exercised.
func BenchmarkPhysicalProjectionCurrent(b *testing.B) {
	benchmarkPhysicalProjection(b, false)
}

func BenchmarkPhysicalProjectionProjected(b *testing.B) {
	benchmarkPhysicalProjection(b, true)
}

func benchmarkPhysicalProjection(b *testing.B, projected bool) {
	dir := b.TempDir()
	large := make([]byte, 2<<20)
	var random uint64 = 0x9e3779b97f4a7c15
	for i := range large {
		random ^= random << 13
		random ^= random >> 7
		random ^= random << 17
		large[i] = byte(random)
	}
	open := func() *BadgerEngine {
		engine, err := NewBadgerEngineWithOptions(BadgerOptions{
			DataDir: dir, ExperimentalLayout: true,
			Projected:     projected,
			EncryptionKey: []byte("0123456789abcdef0123456789abcdef"),
		})
		if err != nil {
			b.Fatal(err)
		}
		return engine
	}
	engine := open()
	id, err := engine.CreateNode(b.Context(), &Node{ID: "ns:bench-projection", Labels: []string{"Doc"}, Properties: map[string]any{
		"small": int64(7), "large": large,
	}})
	if err != nil {
		b.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		b.Fatal(err)
	}
	engine = open()
	b.Cleanup(func() { _ = engine.Close() })
	metric := expvar.Get("badger_read_bytes_vlog").(*expvar.Int)
	projection := []string{"small"}
	b.ReportAllocs()
	b.ResetTimer()
	before := metric.Value()
	for i := 0; i < b.N; i++ {
		engine.cacheDeleteNode(id, nodeCacheAllForms)
		node, err := engine.GetNodeProjected(b.Context(), id, projection)
		if err != nil || node == nil || node.Properties["small"] != int64(7) {
			b.Fatalf("projected read: node=%v err=%v", node, err)
		}
	}
	b.ReportMetric(float64(metric.Value()-before)/float64(b.N), "vlog-bytes/op")
}
