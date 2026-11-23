package storage

// v1.5.2 verification (scratch, test-only): storage-level parts of the defects
// found while reviewing PR #1826 that the release has too.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// F4: an ordinary transaction on a write-behind engine.
func TestV152P26WriteBehindSnapshot(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeBehind=%t", writeBehind), func(t *testing.T) {
			ctx := context.Background()
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: writeBehind, WriteBehindInterval: 2 * time.Millisecond}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			for i := 0; i < 5; i++ {
				_, err := b.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(i)}})
				require.NoError(t, err)
			}
			if writeBehind {
				require.NoError(t, b.FlushWriteBehind(ctx)) // what a Cypher BEGIN does
			}
			tx, err := b.BeginTransaction(ctx, OrdinaryBegin)
			require.NoError(t, err)
			defer tx.Rollback()
			read := func() string {
				seen := 0
				require.NoError(t, tx.StreamNodesByLabelProjected(ctx, "Doc", []string{"v"}, func(*Node) error { seen++; return nil }))
				point := "absent"
				if node, err := tx.GetNodeProjected(ctx, "test:new", []string{"v"}); err == nil {
					point = fmt.Sprint(node.Properties["v"])
				}
				value := "absent"
				if node, err := tx.GetNodeProjected(ctx, "test:n1", []string{"v"}); err == nil {
					value = fmt.Sprint(node.Properties["v"])
				}
				return fmt.Sprintf("nodes=%d new=%s n1=%s", seen, point, value)
			}
			before := read()
			_, err = b.CreateNode(ctx, &Node{ID: "test:new", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(100)}})
			require.NoError(t, err)
			require.NoError(t, b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:n1", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(55)}}}))
			after := read()
			t.Logf("P26ROW F4S writeBehind=%t before=%q after=%q", writeBehind, before, after)
			if before != after {
				t.Errorf("P26 F4S writeBehind=%t: the transaction read %q, then after two acknowledged writes %q", writeBehind, before, after)
			}
		})
	}
}

// F5: a transaction's repeated projected label stream.
func TestV152P26StreamSharesImage(t *testing.T) {
	for _, projected := range []bool{false, true} {
		t.Run(fmt.Sprintf("projected=%t", projected), func(t *testing.T) {
			ctx := context.Background()
			b := createTestBadgerEngine(t)
			_, err := b.CreateNode(ctx, &Node{ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"tags": []string{"a", "b"}, "m": []any{"x", int64(1)}}})
			require.NoError(t, err)
			tx, err := b.BeginTransaction(ctx, OrdinaryBegin)
			require.NoError(t, err)
			defer tx.Rollback()
			var properties []string
			if projected {
				properties = []string{"tags", "m"}
			}
			read := func(mutate bool) (out string) {
				require.NoError(t, tx.StreamNodesByLabelProjected(ctx, "Doc", properties, func(n *Node) error {
					out = fmt.Sprintf("%v %v", n.Properties["tags"], n.Properties["m"])
					if mutate {
						n.Properties["tags"].([]string)[0] = "CHANGED"
						n.Properties["m"] = "CHANGED"
					}
					return nil
				}))
				return out
			}
			first := read(true)
			second := read(false)
			t.Logf("P26ROW F5 projected=%t first=%q second=%q", projected, first, second)
			if first != second {
				t.Errorf("P26 F5 projected=%t: the second stream returns the first visitor's changes: %q then %q", projected, first, second)
			}
		})
	}
}

func p26StoredBody(t testing.TB, b *BadgerEngine, id NodeID) []byte {
	var body []byte
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err != nil {
			return err
		}
		body, err = item.ValueCopy(nil)
		return err
	}))
	return body
}

// F6: one changed byte in a stored body, per place.
func TestV152P26OneByteAllocation(t *testing.T) {
	ctx := context.Background()
	b, _ := createTestBadgerEngineOnDisk(t)
	blob := bytes.Repeat([]byte{0x5a, 0x13, 0xe7, 0x21}, 25) // 100 bytes
	text := string(bytes.Repeat([]byte("0123456789"), 10))   // 100 characters
	_, err := b.CreateNode(ctx, &Node{ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"blob": blob, "text": text, "key": "k"}, EmbedMeta: map[string]any{"has_embedding": true}})
	require.NoError(t, err)
	body := p26StoredBody(t, b, "test:n")
	ns := namespaceForNodeID("test:n")
	measure := func(body []byte, include map[string]struct{}) (uint64, error) {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, err := b.decodeNodeProjected(nil, ns, body, include)
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc, err
	}
	valid, err := measure(body, nil)
	require.NoError(t, err)
	t.Logf("P26ROW F6S body of %d bytes, valid: %d bytes allocated", len(body), valid)
	places := []struct {
		name string
		at   int
		to   byte
	}{
		{"property bytes header c4 to c6 (one bit)", bytes.Index(body, append([]byte{0xc4, 100}, blob[:8]...)), 0xc6},
		{"property string header d9 to db (one bit)", bytes.Index(body, append([]byte{0xd9, 100}, text[:8]...)), 0xdb},
	}
	for _, p := range places {
		if p.at < 0 || p.at >= len(body) {
			t.Errorf("place not found: %s", p.name)
			continue
		}
		mutated := append([]byte(nil), body...)
		mutated[p.at] = p.to
		for _, include := range []map[string]struct{}{nil, {"key": {}}, {"blob": {}, "text": {}}} {
			allocated, err := measure(mutated, include)
			message := "nil"
			if err != nil {
				message = err.Error()
			}
			t.Logf("P26ROW F6S %s: %02x -> %02x, projection %v: %d MiB allocated, error: %.160s", p.name, body[p.at], p.to, include, allocated>>20, message)
			if allocated > 64<<20 {
				t.Errorf("P26 F6S %s, projection %v: decoding a body of %d bytes allocates %d MiB", p.name, include, len(body), allocated>>20)
			}
		}
	}
}

// R1 #7: the node caches' byte estimate of numeric lists.
func TestV152P26NumericListEstimate(t *testing.T) {
	base := estimateNodeBodyCacheBytes(&Node{Properties: map[string]any{"v": nil}})
	const n = 1536
	anys, texts := make([]any, n), make([]string, n)
	for i := range anys {
		anys[i], texts[i] = float64(i), "0123456789abcdef"
	}
	for _, c := range []struct {
		name  string
		value any
		real  int64
	}{
		{"[]float32", make([]float32, n), n * 4}, {"[]float64", make([]float64, n), n * 8}, {"[]int64", make([]int64, n), n * 8},
		{"[]bool", make([]bool, n), n}, {"[]any of floats", anys, n * 16}, {"[]string", texts, n * 32}, {"string", string(make([]byte, n*8)), n * 8}, {"[]byte", make([]byte, n*8), n * 8},
	} {
		counted := estimateNodeBodyCacheBytes(&Node{Properties: map[string]any{"v": c.value}}) - base
		t.Logf("P26ROW R7S %s of %d elements: payload %d bytes, counted %d (whole node %d)", c.name, n, c.real, counted, counted+base)
		if counted < c.real/2 {
			t.Errorf("P26 R7S %s of %d elements: %d payload bytes are counted as %d", c.name, n, c.real, counted)
		}
	}
	// The cache itself: a 16 MiB budget, 100 nodes of 1 MiB read once.
	for _, kind := range []string{"string", "floats"} {
		b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), NodeCacheMaxBytes: 16 << 20}))
		require.NoError(t, err)
		ctx := context.Background()
		for i := 0; i < 100; i++ {
			var v any = string(bytes.Repeat([]byte(fmt.Sprintf("%08d", i)), 1<<17))
			if kind == "floats" {
				list := make([]float64, 1<<17)
				for j := range list {
					list[j] = float64(i*j) + 0.5
				}
				v = list
			}
			_, err := b.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("test:n%03d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"v": v}})
			require.NoError(t, err)
		}
		for i := 0; i < 100; i++ {
			_, err := b.GetNode(ctx, NodeID(fmt.Sprintf("test:n%03d", i)))
			require.NoError(t, err)
		}
		b.nodeCacheMu.RLock()
		entries, counted := len(b.nodeCache), b.nodeCacheBytes
		b.nodeCacheMu.RUnlock()
		t.Logf("P26ROW R7S node cache, budget 16 MiB, %s: %d of 100 nodes of 1 MiB kept, counted %d bytes", kind, entries, counted)
		if int64(entries)<<20 > 2*(16<<20) {
			t.Errorf("P26 R7S node cache with a 16 MiB budget keeps %d MiB of %s and counts %d bytes", entries, kind, counted)
		}
		require.NoError(t, b.Close())
	}
}

// R1 #9: has_embedding of a projected point read of a node whose chunk vectors
// carry no metadata, with an empty and a filled cache, outside and inside a
// transaction.
func TestV152P26ProjectedHasEmbedding(t *testing.T) {
	ctx := context.Background()
	b := createTestBadgerEngine(t)
	for _, c := range []struct {
		name, id string
		meta     map[string]any
	}{{"chunks without metadata", "test:bare", nil}, {"chunks with metadata", "test:meta", map[string]any{"has_embedding": true, "chunk_count": 1}}} {
		id := NodeID(c.id)
		_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"key": "one"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}, EmbedMeta: c.meta})
		require.NoError(t, err)
		whole, err := b.GetNode(ctx, id)
		require.NoError(t, err)
		check := func(stage string, node *Node, err error) {
			require.NoError(t, err)
			t.Logf("P26ROW R9S %s, %s: whole read %t, projected read HasChunkEmbeddings=%t chunks=%d meta=%v", c.name, stage, whole.HasChunkEmbeddings(), node.HasChunkEmbeddings(), len(node.ChunkEmbeddings), node.EmbedMeta)
			if node.HasChunkEmbeddings() != whole.HasChunkEmbeddings() {
				t.Errorf("P26 R9S %s, %s: projected [key, has_embedding] read says %t, the whole node %t", c.name, stage, node.HasChunkEmbeddings(), whole.HasChunkEmbeddings())
			}
		}
		names := []string{"key", "has_embedding"}
		node, err := b.GetNodeProjected(ctx, id, names)
		check("cached whole node", node, err)
		b.cacheDeleteNode(id, nodeCacheAllForms)
		node, err = b.GetNodeProjected(ctx, id, names)
		check("empty cache", node, err)
		var scanned *Node
		err = b.StreamNodesByLabelProjected(ctx, "Doc", names, func(n *Node) error {
			if n.ID == id {
				scanned = CopyNode(n)
			}
			return nil
		})
		check("label scan", scanned, err)
		tx, err := b.BeginTransaction(ctx, OrdinaryBegin)
		require.NoError(t, err)
		node, err = tx.GetNodeProjected(ctx, id, names)
		check("transaction", node, err)
		require.NoError(t, tx.Rollback())
	}
}

// TestV152P26StorageAll runs the tests above as one named test.
func TestV152P26StorageAll(t *testing.T) {
	t.Run("WriteBehindSnapshot", TestV152P26WriteBehindSnapshot)
	t.Run("StreamSharesImage", TestV152P26StreamSharesImage)
	t.Run("OneByteAllocation", TestV152P26OneByteAllocation)
	t.Run("NumericListEstimate", TestV152P26NumericListEstimate)
	t.Run("ProjectedHasEmbedding", TestV152P26ProjectedHasEmbedding)
}

// F6, second place: one changed byte in the header of the stored metadata map.
// A watchdog ends the process when the heap passes 1.5 GiB, so the run ends
// with exit status 3 and the stack instead of being killed for memory.
func TestV152P26MetadataMapAllocation(t *testing.T) {
	ctx := context.Background()
	b, _ := createTestBadgerEngineOnDisk(t)
	_, err := b.CreateNode(ctx, &Node{ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"key": "k"}, EmbedMeta: map[string]any{"has_embedding": true}})
	require.NoError(t, err)
	body := p26StoredBody(t, b, "test:n")
	at := bytes.Index(body, []byte("EmbedMeta")) + len("EmbedMeta")
	mutated := append([]byte(nil), body...)
	mutated[at] = 0xdf
	claimed := uint32(mutated[at+1])<<24 | uint32(mutated[at+2])<<16 | uint32(mutated[at+3])<<8 | uint32(mutated[at+4])
	t.Logf("P26ROW F6M body of %d bytes, metadata map header %02x -> df: the map now claims %d entries", len(body), body[at], claimed)
	fmt.Printf("P26ROW F6M body of %d bytes, metadata map header %02x -> df: the map now claims %d entries\n", len(body), body[at], claimed)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapAlloc < 1536<<20 {
				continue
			}
			fmt.Printf("P26ROW F6M heap %d MiB while decoding one body of %d bytes\n", m.HeapAlloc>>20, len(body))
			var out bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&out, 2)
			fmt.Printf("P26STACK %s\n", out.String())
			os.Exit(3)
		}
	}()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = b.decodeNodeProjected(nil, namespaceForNodeID("test:n"), mutated, nil)
	runtime.ReadMemStats(&after)
	close(done)
	t.Logf("P26ROW F6M decode allocated %d MiB, error: %.160v", (after.TotalAlloc-before.TotalAlloc)>>20, err)
	if after.TotalAlloc-before.TotalAlloc > 64<<20 {
		t.Errorf("P26 F6M decoding a body of %d bytes allocates %d MiB", len(body), (after.TotalAlloc-before.TotalAlloc)>>20)
	}
}
