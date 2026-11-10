package storage

// A filtered scan of a label reads through the node read cache (NOR-407): it
// settles its filter on the images the cache holds, and a scan that repeats
// keeps the nodes it had to read, so a statement that scans the label once per
// row reads the store once.

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// filteredLabelScanFixture is an engine with documents Doc nodes d0, d1, … in
// database a, among twice as many other nodes, and nothing in its node caches.
// d0 has stored vectors, which it returns.
func filteredLabelScanFixture(t testing.TB, options BadgerOptions, documents int) (*BadgerEngine, [][]float32) {
	t.Helper()
	options.InMemory = true
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(options))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	vectors := makeLargeChunkEmbeddings()
	for i := 0; i < documents; i++ {
		id := fmt.Sprintf("d%d", i)
		node := &Node{ID: NodeID("a:" + id), Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": id, "title": "t"}}
		if i == 0 {
			node.ChunkEmbeddings = vectors
		}
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	for i := 0; i < 2*documents; i++ {
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("a:o%d", i)), Labels: []string{"Other"}, Properties: map[string]interface{}{"id": fmt.Sprintf("d%d", i)}})
		require.NoError(t, err)
	}
	dropNodeReadCaches(engine)
	return engine, vectors
}

// scanDocumentsByID is the scan MATCH (:Doc {id: $id}) asks the engine for:
// its rows are query images, or whole nodes with their stored vectors.
func scanDocumentsByID(t testing.TB, engine *BadgerEngine, id string, storedVectors bool) []*Node {
	t.Helper()
	options := StreamNodesOptions{
		Prefix:                   "a:",
		Label:                    "Doc",
		QueryNodes:               !storedVectors,
		WithEmbeddings:           storedVectors,
		ApplyDecayFilter:         true,
		PropertyFilter:           func(properties map[string]interface{}) bool { return properties["id"] == id },
		PropertyFilterProjection: []string{"id"},
		PropertyStringEquals:     map[string]string{"id": id},
	}
	var rows []*Node
	require.NoError(t, engine.StreamNodesWithOptions(context.Background(), options, func(node *Node) error {
		rows = append(rows, node)
		return nil
	}))
	return rows
}

// cachedDocuments counts the Doc nodes the engine's node read caches hold.
func cachedDocuments(engine *BadgerEngine) int {
	engine.nodeCacheMu.RLock()
	defer engine.nodeCacheMu.RUnlock()
	held := map[NodeID]bool{}
	for id := range engine.nodeCache {
		held[id] = strings.HasPrefix(string(id), "a:d")
	}
	for id := range engine.queryNodeCache {
		held[id] = strings.HasPrefix(string(id), "a:d")
	}
	count := 0
	for _, document := range held {
		if document {
			count++
		}
	}
	return count
}

func TestFilteredLabelScanReadsThroughTheNodeCache(t *testing.T) {
	for _, storedVectors := range []bool{false, true} {
		name := "query rows"
		if storedVectors {
			name = "rows with stored vectors"
		}
		t.Run(name, func(t *testing.T) {
			const documents = 40
			engine, vectors := filteredLabelScanFixture(t, BadgerOptions{}, documents)
			whole := func(id string) map[string]interface{} { return map[string]interface{}{"id": id, "title": "t"} }
			scan := func(id string) []*Node { return scanDocumentsByID(t, engine, id, storedVectors) }
			one := func(rows []*Node, id NodeID, properties map[string]interface{}) {
				t.Helper()
				require.Len(t, rows, 1)
				require.Equal(t, id, rows[0].ID)
				require.Equal(t, properties, rows[0].Properties)
				if storedVectors && id == "a:d0" {
					require.Equal(t, vectors, rows[0].ChunkEmbeddings)
				}
			}

			// A first scan rejects a node by its stored bytes and keeps nothing.
			one(scan("d7"), "a:d7", whole("d7"))
			require.Zero(t, cachedDocuments(engine), "a first scan keeps the scan decoder")

			// The scan repeats: the nodes it reads are kept, the rejected ones too.
			one(scan("d8"), "a:d8", whole("d8"))
			require.Equal(t, documents, cachedDocuments(engine), "a repeated scan keeps the label's nodes")

			// The next scans settle the filter on the cached images. A row is the
			// caller's own copy, and the same row whether it was read or cached.
			rows := scan("d9")
			one(rows, "a:d9", whole("d9"))
			rows[0].Properties["id"] = "changed by the caller"
			one(scan("d9"), "a:d9", whole("d9"))
			one(scan("d0"), "a:d0", whole("d0"))
			dropNodeReadCaches(engine)
			rows = scan("d9")
			one(rows, "a:d9", whole("d9"))
			rows[0].Properties["id"] = "changed by the caller"
			require.Equal(t, documents, cachedDocuments(engine))
			one(scan("d9"), "a:d9", whole("d9"))
			one(scan("d0"), "a:d0", whole("d0"))
			require.Empty(t, scan("none"))

			// A write is seen by the next scan.
			require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "a:d9", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "moved", "title": "t"}}}))
			require.Empty(t, scan("d9"))
			one(scan("moved"), "a:d9", whole("moved"))
			require.NoError(t, engine.DeleteNode(t.Context(), "a:d7"))
			require.Empty(t, scan("d7"))
			_, err := engine.CreateNode(t.Context(), &Node{ID: "a:new", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "d7", "title": "t"}})
			require.NoError(t, err)
			one(scan("d7"), "a:new", whole("d7"))
			require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "a:d8", Labels: []string{"Gone"}, Properties: whole("d8")}}))
			require.Empty(t, scan("d8"), "a node that lost the label")

			// The stored vectors of a rejected node are not read: one that cannot
			// be read does not fail the scan that keeps the label's nodes.
			corruptFirstEmbeddingChunk(t, engine, "a:d0")
			dropNodeReadCaches(engine)
			one(scan("d5"), "a:d5", whole("d5"))
			one(scan("d6"), "a:d6", whole("d6"))
		})
	}
}

// A label the node read cache cannot hold keeps the scan decoder, however
// often its scan repeats: its nodes would only push each other out.
func TestFilteredLabelScanOfALargeLabelKeepsTheScanDecoder(t *testing.T) {
	engine, _ := filteredLabelScanFixture(t, BadgerOptions{NodeCacheMaxEntries: 16}, 40)
	for _, id := range []string{"d7", "d8", "d9"} {
		rows := scanDocumentsByID(t, engine, id, false)
		require.Len(t, rows, 1)
		require.Equal(t, NodeID("a:"+id), rows[0].ID)
	}
	require.Zero(t, cachedDocuments(engine))
}

// A label whose images the cache's byte budget cannot hold keeps the scan
// decoder too, whether one image is already more than the budget or the
// label's images together are. The scan that finds it out reads no further
// image, nothing of the label is kept, and the label's next scans read no
// image at all: they cost what the label's first scan cost, which ran on the
// scan decoder.
func TestFilteredLabelScanOfALabelBeyondTheByteBudgetKeepsTheScanDecoder(t *testing.T) {
	const documents, payload = 40, 16 << 10
	for _, budget := range []int64{8 << 10, 256 << 10} {
		t.Run(fmt.Sprintf("budget of %d KiB", budget>>10), func(t *testing.T) {
			engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, NodeCacheMaxEntries: 200, NodeCacheMaxBytes: budget}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			// Two labels of the same nodes, each a quarter of the store.
			for i := 0; i < 4*documents; i++ {
				label := "Other"
				switch {
				case i < documents:
					label = "Doc"
				case i < 2*documents:
					label = "Twin"
				}
				_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("a:%03d", i)), Labels: []string{label}, Properties: map[string]interface{}{"id": fmt.Sprint(i), "payload": strings.Repeat("v", payload)}})
				require.NoError(t, err)
			}
			dropNodeReadCaches(engine)
			scan := func(label, id string) []*Node {
				t.Helper()
				var rows []*Node
				require.NoError(t, engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
					Prefix:                   "a:",
					Label:                    label,
					QueryNodes:               true,
					ApplyDecayFilter:         true,
					PropertyFilter:           func(properties map[string]interface{}) bool { return properties["id"] == id },
					PropertyFilterProjection: []string{"id"},
					PropertyStringEquals:     map[string]string{"id": id},
				}, func(node *Node) error {
					rows = append(rows, node)
					return nil
				}))
				return rows
			}
			// allocations is how many allocations one scan of Doc that rejects
			// every node makes.
			allocations := func(scans int) uint64 {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				for i := 0; i < scans; i++ {
					require.Empty(t, scan("Doc", "none"))
				}
				runtime.ReadMemStats(&after)
				return (after.Mallocs - before.Mallocs) / uint64(scans)
			}
			images := func() int {
				engine.nodeCacheMu.RLock()
				defer engine.nodeCacheMu.RUnlock()
				return len(engine.nodeCache) + len(engine.queryNodeCache)
			}
			// The first scan of the twin label pays what the engine's first
			// filtered scan costs once.
			require.Empty(t, scan("Twin", "none"))
			first := allocations(1)
			finding := allocations(1)
			require.Zero(t, images(), "nothing is kept of a label the budget cannot hold")
			repeated := allocations(20)
			t.Logf("allocations a scan: first %d, the scan that finds the label too large %d, the next ones %d", first, finding, repeated)
			require.LessOrEqual(t, repeated, first+first/10, "a repeated scan costs what the label's first scan cost")
			require.Zero(t, images())
			rows := scan("Doc", "7")
			require.Len(t, rows, 1)
			require.Equal(t, NodeID("a:007"), rows[0].ID)
			require.Zero(t, images())

			// The refusal stands while the label's nodes are what they were. A
			// write to one of them ends it: the next scan that repeats offers
			// the label's images again, and they are refused again.
			require.True(t, engine.labelImagesRefused("a:", "Doc"))
			require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "a:007", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "7", "payload": strings.Repeat("w", payload)}}}))
			require.False(t, engine.labelImagesRefused("a:", "Doc"))
			dropNodeReadCaches(engine) // the write itself may have cached the node it wrote
			require.Empty(t, scan("Doc", "none"))
			require.True(t, engine.labelImagesRefused("a:", "Doc"))
			require.Zero(t, images())

			// Its nodes rewritten with nothing but their id, the label fits
			// the budget and is kept.
			for i := 0; i < documents; i++ {
				require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: NodeID(fmt.Sprintf("a:%03d", i)), Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": fmt.Sprint(i)}}}))
			}
			dropNodeReadCaches(engine)
			require.Empty(t, scan("Doc", "none"))
			require.False(t, engine.labelImagesRefused("a:", "Doc"))
			if budget > 8<<10 {
				// The smaller budget holds no forty nodes of any size.
				require.Equal(t, documents, images(), "a label that fits is kept")
			}
		})
	}
}

// dropOtherImages removes from the node read cache the images of the fixture's
// nodes from the given number on, which are not the label's.
func dropOtherImages(engine *BadgerEngine, from int) {
	engine.nodeCacheMu.Lock()
	defer engine.nodeCacheMu.Unlock()
	for i := from; i < 3*from; i++ {
		id := NodeID(fmt.Sprintf("a:%02d", i))
		engine.deleteNodeReadCacheLocked(id, nodeReadCacheQuery)
		engine.deleteNodeReadCacheLocked(id, nodeReadCacheFull)
	}
}

// A label that fits the cache's byte budget is kept again after the cache has
// given its images up, however it did: the cache decides on what it holds when
// the images are offered, and no count of what it once held stands in the way.
func TestFilteredLabelScanIsKeptAgainAfterTheCacheGaveItsImagesUp(t *testing.T) {
	const documents, payload = 12, 16 << 10
	for name, giveUp := range map[string]func(t *testing.T, engine *BadgerEngine){
		"another database dropped": func(t *testing.T, engine *BadgerEngine) {
			_, err := engine.CreateNode(t.Context(), &Node{ID: "b:temp", Labels: []string{"Other"}, Properties: map[string]interface{}{"id": "temporary"}})
			require.NoError(t, err)
			_, _, err = engine.DeleteByPrefix("b:")
			require.NoError(t, err)
		},
		"caches emptied": func(t *testing.T, engine *BadgerEngine) {
			dropNodeReadCaches(engine)
		},
		"evicted for other nodes": func(t *testing.T, engine *BadgerEngine) {
			// Reading the other nodes fills the budget with their images,
			// and the cache removes images of the label to make room.
			for held := true; held; {
				for i := documents; i < 3*documents; i++ {
					_, err := engine.GetQueryNode(t.Context(), NodeID(fmt.Sprintf("a:%02d", i)))
					require.NoError(t, err)
				}
				engine.nodeCacheMu.RLock()
				held = engine.queryNodeCache["a:00"] != nil && engine.queryNodeCache["a:01"] != nil
				engine.nodeCacheMu.RUnlock()
				if held {
					dropOtherImages(engine, documents)
				}
			}
			// The other nodes' images are gone again; the label's evicted
			// ones stay evicted.
			dropOtherImages(engine, documents)
		},
		"its nodes rewritten": func(t *testing.T, engine *BadgerEngine) {
			for i := 0; i < documents; i++ {
				require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: NodeID(fmt.Sprintf("a:%02d", i)), Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": fmt.Sprint(i), "payload": strings.Repeat("w", payload)}}}))
			}
			dropNodeReadCaches(engine)
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, NodeCacheMaxEntries: 200, NodeCacheMaxBytes: 200 << 10}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			for i := 0; i < 3*documents; i++ {
				label := "Other"
				if i < documents {
					label = "Doc"
				}
				_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("a:%02d", i)), Labels: []string{label}, Properties: map[string]interface{}{"id": fmt.Sprint(i), "payload": strings.Repeat("v", payload)}})
				require.NoError(t, err)
			}
			dropNodeReadCaches(engine)
			scan := func() {
				t.Helper()
				require.NoError(t, engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
					Prefix:                   "a:",
					Label:                    "Doc",
					QueryNodes:               true,
					PropertyFilter:           func(properties map[string]interface{}) bool { return properties["id"] == "3" },
					PropertyFilterProjection: []string{"id"},
				}, func(*Node) error { return nil }))
			}
			// held counts the label's nodes whose query images the cache holds.
			held := func() int {
				engine.nodeCacheMu.RLock()
				defer engine.nodeCacheMu.RUnlock()
				count := 0
				for i := 0; i < documents; i++ {
					if engine.queryNodeCache[NodeID(fmt.Sprintf("a:%02d", i))] != nil {
						count++
					}
				}
				return count
			}
			scan()
			scan()
			require.Equal(t, documents, held(), "the label fits the budget")
			giveUp(t, engine)
			require.Less(t, held(), documents, "the cache gave images of the label up")
			for round := 0; round < 4; round++ {
				scan()
			}
			require.Equal(t, documents, held(), "the label is kept again")
			require.False(t, engine.labelImagesRefused("a:", "Doc"))
		})
	}
}

// A scan settles its filter on the image the cache holds of a node and does
// not read that node's record: with images marked, the scan's rows and its
// rejections are the images', not the store's.
func TestFilteredLabelScanSettlesItsFilterOnTheCachedImages(t *testing.T) {
	for _, storedVectors := range []bool{false, true} {
		name := "query rows"
		if storedVectors {
			name = "rows with stored vectors"
		}
		t.Run(name, func(t *testing.T) {
			const documents = 40
			engine, _ := filteredLabelScanFixture(t, BadgerOptions{}, documents)
			scan := func(id string) []*Node { return scanDocumentsByID(t, engine, id, storedVectors) }
			require.Len(t, scan("d7"), 1)
			require.Len(t, scan("d8"), 1)
			require.Equal(t, documents, cachedDocuments(engine))
			// The store says d1 and d2; their images now say otherwise.
			marked := 0
			engine.nodeCacheMu.Lock()
			for _, cache := range []map[NodeID]*Node{engine.nodeCache, engine.queryNodeCache} {
				for _, id := range []NodeID{"a:d1", "a:d2"} {
					if image := cache[id]; image != nil {
						mark := copyNode(image)
						mark.Properties["id"] = "image of " + string(id)
						cache[id] = mark
						marked++
					}
				}
			}
			engine.nodeCacheMu.Unlock()
			require.GreaterOrEqual(t, marked, 2)

			require.Empty(t, scan("d1"), "a node is rejected on its image")
			rows := scan("image of a:d2")
			require.Len(t, rows, 1, "a node is kept on its image")
			require.Equal(t, NodeID("a:d2"), rows[0].ID)
			if !storedVectors {
				require.Equal(t, "image of a:d2", rows[0].Properties["id"], "a query row is a copy of the image")
			}
			require.Len(t, scan("d3"), 1)
		})
	}
}

// A scan reads one view of the store. A write that comes while its visitor
// runs is not in that view, and neither is an image of the written node that
// reaches the cache meanwhile: the scan settles its filter on the node its
// view holds, on a first scan and on one that reads the label through the
// cache.
func TestFilteredLabelScanReadsItsOwnView(t *testing.T) {
	for _, repeated := range []bool{false, true} {
		name := "first scan"
		if repeated {
			name = "repeated scan"
		}
		t.Run(name, func(t *testing.T) {
			const documents = 3
			engine, _ := filteredLabelScanFixture(t, BadgerOptions{}, documents)
			options := StreamNodesOptions{
				Prefix:                   "a:",
				Label:                    "Doc",
				QueryNodes:               true,
				ApplyDecayFilter:         true,
				PropertyFilter:           func(properties map[string]interface{}) bool { return properties["title"] == "t" },
				PropertyFilterProjection: []string{"title"},
			}
			scan := func(visit func(*Node)) []NodeID {
				t.Helper()
				var rows []NodeID
				require.NoError(t, engine.StreamNodesWithOptions(context.Background(), options, func(node *Node) error {
					rows = append(rows, node.ID)
					if visit != nil {
						visit(node)
					}
					return nil
				}))
				return rows
			}
			if repeated {
				require.Len(t, scan(nil), documents)
				require.Len(t, scan(nil), documents)
				require.Equal(t, documents, cachedDocuments(engine))
			} else {
				for i := 0; i < documents; i++ {
					_, err := engine.GetQueryNode(t.Context(), NodeID(fmt.Sprintf("a:d%d", i)))
					require.NoError(t, err)
				}
			}
			// The visitor of the scan's first row rewrites the other nodes of
			// the label and reads them, which caches their new images.
			written := false
			rows := scan(func(node *Node) {
				if written {
					return
				}
				written = true
				for i := 0; i < documents; i++ {
					id := NodeID(fmt.Sprintf("a:d%d", i))
					if id == node.ID {
						continue
					}
					require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": fmt.Sprintf("d%d", i), "title": "rewritten"}}}))
					_, err := engine.GetQueryNode(t.Context(), id)
					require.NoError(t, err)
				}
			})
			require.Len(t, rows, documents, "every node of the label matched in the scan's view")
			require.Len(t, scan(nil), 1, "the next scan sees the writes")
		})
	}
}

// BenchmarkFilteredLabelScanRepeated is the scan a statement repeats once per
// row: MATCH (:Doc {id: ...}) over a label of 2,000 nodes among 6,000.
func BenchmarkFilteredLabelScanRepeated(b *testing.B) {
	const documents = 2000
	engine, _ := filteredLabelScanFixture(b, BadgerOptions{}, documents)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rows := scanDocumentsByID(b, engine, fmt.Sprintf("d%d", i%documents), true); len(rows) != 1 {
			b.Fatalf("scan %d returned %d rows", i, len(rows))
		}
	}
}
