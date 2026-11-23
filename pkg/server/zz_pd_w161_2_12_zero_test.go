//go:build pd_w161_2_12

package server

// E38.17, zero-failure case: the same two stores as TestL2_e38_17_Repro with
// NO parked failures. Times GET /nornicdb/embed/failures, the worker listing
// and the storage stream, and counts the keys the stream's iterator visits
// (every key under the embedding prefix) against the metadata records it
// decodes (keys ending in the reserved metadata chunk index).

import (
	"context"
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	l2E38_17ZeroPrefixEmbedding = byte(0x08)         // pkg/storage/badger.go prefixEmbedding
	l2E38_17ZeroMetaChunkIndex  = uint32(0x7FFFFFFF) // reservedEmbeddingMetaChunkIndex
	l2E38_17ZeroCalls           = 15
)

func l2E38_17ZeroPercentile(ds []time.Duration, p float64) time.Duration {
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	idx := int(p*float64(len(c)-1) + 0.5)
	return c[idx]
}

// l2E38_17ZeroKeys walks the embedding prefix exactly as
// StreamParkedEmbeddingFailures does (same prefix, keys only) and classifies
// each key the way embeddingMetaFromKey does.
func l2E38_17ZeroKeys(t *testing.T, st *l2E38_17Store) (keys, metadata, chunkKeys int, walk time.Duration) {
	t.Helper()
	base := storage.UnwrapEngine(st.srv.db.GetBaseStorageForManager())
	holder, ok := base.(interface{ DB() *badger.DB })
	if !ok {
		t.Fatalf("%s: base storage %T exposes no badger DB", st.name, base)
	}
	prefix := []byte{l2E38_17ZeroPrefixEmbedding}
	start := time.Now()
	err := holder.DB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			keys++
			if len(key) >= 6 && key[len(key)-5] == 0x00 && binary.BigEndian.Uint32(key[len(key)-4:]) == l2E38_17ZeroMetaChunkIndex {
				metadata++
			} else {
				chunkKeys++
			}
		}
		return nil
	})
	walk = time.Since(start)
	if err != nil {
		t.Fatalf("%s: key walk: %v", st.name, err)
	}
	return keys, metadata, chunkKeys, walk
}

func TestL2_e38_17_Zero(t *testing.T) {
	ctx := context.Background()
	stores := []*l2E38_17Store{
		l2E38_17OpenWith(t, "small", l2E38_17SmallNodes, l2E38_17SmallChunks, 0),
		l2E38_17OpenWith(t, "large", l2E38_17LargeNodes, l2E38_17LargeChunks, 0),
	}
	for _, limit := range []int{1, 1000} {
		for _, st := range stores {
			l2E38_17Get(t, st, limit) // warm-up
		}
		calls := map[string][]time.Duration{}
		for i := 0; i < l2E38_17ZeroCalls; i++ {
			for _, st := range stores {
				d, returned, count := l2E38_17Get(t, st, limit)
				if returned != 0 || count != 0 {
					t.Fatalf("%s: limit=%d returned %d count %d, want 0 and 0", st.name, limit, returned, count)
				}
				calls[st.name] = append(calls[st.name], d)
			}
		}
		for _, st := range stores {
			t.Logf("ZERO_HTTP store=%s nodes=%d chunks=%d parked=0 limit=%d calls=%d median=%v p95=%v min=%v max=%v",
				st.name, st.nodes, st.chunks, limit, l2E38_17ZeroCalls,
				l2E38_17ZeroPercentile(calls[st.name], 0.5), l2E38_17ZeroPercentile(calls[st.name], 0.95),
				l2E38_17ZeroPercentile(calls[st.name], 0), l2E38_17ZeroPercentile(calls[st.name], 1))
		}
	}
	for _, st := range stores {
		streamer, ok := storage.UnwrapEngine(st.srv.db.GetBaseStorageForManager()).(storage.EmbeddingFailureStreamer)
		if !ok {
			t.Fatalf("%s: base storage is not an EmbeddingFailureStreamer", st.name)
		}
		var list, full, walks []time.Duration
		var keys, metadata, chunkKeys int
		for i := 0; i < 7; i++ {
			start := time.Now()
			failures, err := st.srv.db.EmbeddingFailures(ctx, 1)
			list = append(list, time.Since(start))
			if err != nil || len(failures) != 0 {
				t.Fatalf("%s: EmbeddingFailures(1): %d, %v", st.name, len(failures), err)
			}
			start = time.Now()
			n, err := streamer.StreamParkedEmbeddingFailures(ctx, func(storage.NodeID, map[string]any) error { return nil })
			full = append(full, time.Since(start))
			if err != nil || n != 0 {
				t.Fatalf("%s: full stream: %d, %v", st.name, n, err)
			}
			var walk time.Duration
			keys, metadata, chunkKeys, walk = l2E38_17ZeroKeys(t, st)
			walks = append(walks, walk)
		}
		t.Logf("ZERO_CAUSE store=%s nodes=%d chunks=%d parked=0 keys_visited=%d metadata_records_decoded=%d chunk_keys_skipped=%d failures_found=0 worker_listing_limit1_median=%v storage_stream_median=%v key_only_walk_median=%v",
			st.name, st.nodes, st.chunks, keys, metadata, chunkKeys,
			l2E38_17Median(list), l2E38_17Median(full), l2E38_17Median(walks))
	}
}
