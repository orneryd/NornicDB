package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Independent storage-level probes for cache accounting, absent-field growth,
// and the synthetic has_embedding projection.
func TestPR1826VerifyStorageObservations(t *testing.T) {
	b := createTestBadgerEngine(t)
	vec := make([]float32, 1536)
	for i := range vec { vec[i] = float32(i) }
	bytes := estimateNodeBodyCacheBytes(&Node{Properties: map[string]any{"vec": vec}})
	t.Logf("OBS r1_f7 vector_len=%d estimate_bytes=%d raw_float32_bytes=%d", len(vec), bytes, len(vec)*4)

	id := NodeID("verify:absent-names")
	_, err := b.CreateNode(context.Background(), &Node{ID: id, Labels: []string{"VerifyCache"}, Properties: map[string]any{"key": "one"}})
	if err != nil { t.Fatalf("seed: %v", err) }
	start := time.Now()
	for i := 0; i < 3000; i++ {
		if _, err := b.GetNodeProjected(context.Background(), id, []string{fmt.Sprintf("missing_%d", i)}); err != nil { t.Fatalf("projected read %d: %v", i, err) }
	}
	t.Logf("OBS r1_f8 unique_absent_reads=3000 elapsed=%s", time.Since(start))
	b.nodeBodyCacheMu.RLock()
	entry := b.nodeBodyCache[id]
	entryPresent := entry != nil
	b.nodeBodyCacheMu.RUnlock()
	t.Logf("OBS r1_f8 cache_entry_present=%v", entryPresent)

	for i := 0; i < 16; i++ {
		_, err := b.CreateNode(context.Background(), &Node{ID: NodeID(fmt.Sprintf("verify:scan-%d", i)), Labels: []string{"VerifyScan"}, Properties: map[string]any{"vec": vec, "key": fmt.Sprint(i)}})
		if err != nil { t.Fatalf("scan seed: %v", err) }
	}
	visited := 0
	err = b.StreamNodesByLabelProjected(context.Background(), "VerifyScan", []string{"vec"}, func(*Node) error { visited++; return nil })
	if err != nil { t.Fatalf("projected label scan: %v", err) }
	b.nodeBodyCacheMu.RLock()
	entries := len(b.nodeBodyCache)
	b.nodeBodyCacheMu.RUnlock()
	t.Logf("OBS r1_f7 label_scan_visited=%d body_cache_entries=%d", visited, entries)

	chunkID := NodeID("verify:chunk-only")
	_, err = b.CreateNode(context.Background(), &Node{ID: chunkID, Labels: []string{"VerifyEmbed"}, Properties: map[string]any{"key": "one"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
	if err != nil { t.Fatalf("chunk seed: %v", err) }
	projected, err := b.GetNodeProjected(context.Background(), chunkID, []string{"has_embedding"})
	if err != nil { t.Fatalf("has_embedding projected read: %v", err) }
	whole, err := b.GetNode(context.Background(), chunkID)
	if err != nil { t.Fatalf("whole read: %v", err) }
	t.Logf("OBS r1_f9 projected=%#v whole_has_embedding=%v chunks=%d", projected.Properties, whole.HasChunkEmbeddings(), len(whole.ChunkEmbeddings))
}
