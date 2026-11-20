package cypher

// Batch 4b, previous build only (untracked; never committed).
//  - 35.10 store: the final build's newNOR413Graph shape (4 indexes, col-0 with
//    1 vector, 16 x {doc 1 vector, ver none, src 256 vectors}, 16 MEMBER_OF
//    relationships), vectors stored inline on the node as this build stores
//    them (ChunkEmbeddings + EmbedMeta).
//  - 35.9: the repeated-endpoint statement over a wrapper that counts every
//    edge-read method of the namespaced engine.
//  - 35.12: TestZZEI2TransactionFirstFulltextReuse on this build's API.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func b4bTokenStore(t *testing.T) (*StorageExecutor, uint64, int, int) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	for _, label := range []string{"Document", "Version", "Source", "Collection"} {
		_, err := exec.Execute(context.Background(), fmt.Sprintf("CREATE INDEX nor413_%s_id FOR (n:%s) ON (n.id)", label, label), nil)
		require.NoError(t, err)
	}
	var vectorBytes uint64
	create := func(id, label string, properties map[string]interface{}, vectors int) {
		properties["id"] = id
		node := &storage.Node{ID: storage.NodeID(id), Labels: []string{label}, Properties: properties}
		if vectors > 0 {
			node.ChunkEmbeddings = make([][]float32, vectors)
			for c := range node.ChunkEmbeddings {
				node.ChunkEmbeddings[c] = make([]float32, 2048)
				node.ChunkEmbeddings[c][c%2048] = 1
			}
			node.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": vectors}
			vectorBytes += uint64(vectors * 2048 * 4)
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	create("col-0", "Collection", map[string]interface{}{"name": "collection"}, 1)
	for i := 0; i < 16; i++ {
		doc, version, source := fmt.Sprintf("doc-%d", i), fmt.Sprintf("ver-%d", i), fmt.Sprintf("src-%d", i)
		create(doc, "Document", map[string]interface{}{"current_version_id": version}, 1)
		create(version, "Version", map[string]interface{}{"current_source_id": source}, 0)
		create(source, "Source", map[string]interface{}{"title": "source " + source}, 256)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(doc + "-member"), StartNode: storage.NodeID(doc), EndNode: "col-0", Type: "MEMBER_OF"}))
	}
	return exec, vectorBytes, 1 + 16*3, 16
}

// ---- 35.9 ------------------------------------------------------------------

type b4bEdgeCounter struct {
	*storage.NamespacedEngine
	mu     sync.Mutex
	counts map[string]int
}

func (s *b4bEdgeCounter) hit(m string) {
	s.mu.Lock()
	s.counts[m]++
	s.mu.Unlock()
}
func (s *b4bEdgeCounter) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	s.hit("GetOutgoingEdges")
	return s.NamespacedEngine.GetOutgoingEdges(id)
}
func (s *b4bEdgeCounter) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	s.hit("GetIncomingEdges")
	return s.NamespacedEngine.GetIncomingEdges(id)
}
func (s *b4bEdgeCounter) OutgoingEdgeHeaders(id storage.NodeID) ([]*storage.Edge, bool, error) {
	s.hit("OutgoingEdgeHeaders")
	return s.NamespacedEngine.OutgoingEdgeHeaders(id)
}
func (s *b4bEdgeCounter) IncomingEdgeHeaders(id storage.NodeID) ([]*storage.Edge, bool, error) {
	s.hit("IncomingEdgeHeaders")
	return s.NamespacedEngine.IncomingEdgeHeaders(id)
}
func (s *b4bEdgeCounter) GetAdjacentEdges(id storage.NodeID) ([]*storage.Edge, []*storage.Edge, error) {
	s.hit("GetAdjacentEdges")
	return s.NamespacedEngine.GetAdjacentEdges(id)
}
func (s *b4bEdgeCounter) GetEdgesBetween(a, b storage.NodeID) ([]*storage.Edge, error) {
	s.hit("GetEdgesBetween")
	return s.NamespacedEngine.GetEdgesBetween(a, b)
}
func (s *b4bEdgeCounter) GetEdgeBetween(a, b storage.NodeID, t string) *storage.Edge {
	s.hit("GetEdgeBetween")
	return s.NamespacedEngine.GetEdgeBetween(a, b, t)
}
func (s *b4bEdgeCounter) GetEdgesByType(t string) ([]*storage.Edge, error) {
	s.hit("GetEdgesByType")
	return s.NamespacedEngine.GetEdgesByType(t)
}
func (s *b4bEdgeCounter) AllEdges() ([]*storage.Edge, error) {
	s.hit("AllEdges")
	return s.NamespacedEngine.AllEdges()
}
func (s *b4bEdgeCounter) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	s.hit("GetEdge")
	return s.NamespacedEngine.GetEdge(id)
}
func (s *b4bEdgeCounter) StreamEdges(ctx context.Context, fn func(*storage.Edge) error) error {
	s.hit("StreamEdges")
	return s.NamespacedEngine.StreamEdges(ctx, fn)
}

func TestB4bRepeatedEndpointReadsB6(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2cost")
	for i := 0; i < 1000; i++ {
		id := storage.NodeID(fmt.Sprintf("node-%04d", i))
		_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"P"}, Properties: map[string]any{"id": int64(i)}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("edge-%04d", i)), StartNode: id, EndNode: id, Type: "SELF"}))
	}
	for _, q := range []string{
		`MATCH (p:P)-[:SELF]->(p) RETURN p.id LIMIT 1`,
		`MATCH (p:P)-[:SELF]->(q) RETURN p.id LIMIT 1`,
	} {
		probe := &b4bEdgeCounter{NamespacedEngine: store, counts: map[string]int{}}
		result, err := NewStorageExecutorWithQueryCachePolicy(probe, 0, 0).Execute(context.Background(), q, nil)
		require.NoError(t, err)
		t.Logf("B4B 35.9 %q rows=%v edge reads by method=%v", q, result.Rows, probe.counts)
	}
}

// ---- 35.12 -----------------------------------------------------------------

type b4bFulltextCounter struct {
	*storage.NamespacedEngine
	scans atomic.Int64
	which sync.Map
}

func (s *b4bFulltextCounter) note(m string) {
	s.scans.Add(1)
	v, _ := s.which.LoadOrStore(m, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}
func (s *b4bFulltextCounter) StreamNodes(ctx context.Context, fn func(*storage.Node) error) error {
	s.note("StreamNodes")
	return s.NamespacedEngine.StreamNodes(ctx, fn)
}
func (s *b4bFulltextCounter) StreamNodesWithOptions(ctx context.Context, o storage.StreamNodesOptions, fn func(*storage.Node) error) error {
	s.note("StreamNodesWithOptions")
	return s.NamespacedEngine.StreamNodesWithOptions(ctx, o, fn)
}
func (s *b4bFulltextCounter) StreamNodesWithoutEmbeddings(ctx context.Context, fn func(*storage.Node) error) error {
	s.note("StreamNodesWithoutEmbeddings")
	return s.NamespacedEngine.StreamNodesWithoutEmbeddings(ctx, fn)
}
func (s *b4bFulltextCounter) StreamNodesByPrefix(ctx context.Context, p string, fn func(*storage.Node) error) error {
	s.note("StreamNodesByPrefix")
	return s.NamespacedEngine.StreamNodesByPrefix(ctx, p, fn)
}
func (s *b4bFulltextCounter) StreamNodesByPrefixProjected(ctx context.Context, p string, props []string, fn func(*storage.Node) error) error {
	s.note("StreamNodesByPrefixProjected")
	return s.NamespacedEngine.StreamNodesByPrefixProjected(ctx, p, props, fn)
}
func (s *b4bFulltextCounter) StreamNodesByPrefixWithoutEmbeddings(ctx context.Context, p string, fn func(*storage.Node) error) error {
	s.note("StreamNodesByPrefixWithoutEmbeddings")
	return s.NamespacedEngine.StreamNodesByPrefixWithoutEmbeddings(ctx, p, fn)
}
func (s *b4bFulltextCounter) StreamNodesByLabelProjected(l string, props []string, fn func(*storage.Node) error) error {
	s.note("StreamNodesByLabelProjected")
	return s.NamespacedEngine.StreamNodesByLabelProjected(l, props, fn)
}
func (s *b4bFulltextCounter) AllNodes() ([]*storage.Node, error) {
	s.note("AllNodes")
	return s.NamespacedEngine.AllNodes()
}
func (s *b4bFulltextCounter) GetNodesByLabel(l string) ([]*storage.Node, error) {
	s.note("GetNodesByLabel")
	return s.NamespacedEngine.GetNodesByLabel(l)
}
func (s *b4bFulltextCounter) StreamEdges(ctx context.Context, fn func(*storage.Edge) error) error {
	s.note("StreamEdges")
	return s.NamespacedEngine.StreamEdges(ctx, fn)
}
func (s *b4bFulltextCounter) GetInnerEngine() storage.Engine { return s.NamespacedEngine }

func (s *b4bFulltextCounter) methods() string {
	var parts []string
	s.which.Range(func(k, v any) bool {
		parts = append(parts, fmt.Sprintf("%s=%d", k, v.(*atomic.Int64).Load()))
		return true
	})
	return strings.Join(parts, " ")
}

func TestB4bTransactionFirstFulltextReuseB6(t *testing.T) {
	for _, txFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("txFirst=%t", txFirst), func(t *testing.T) {
			store := &b4bFulltextCounter{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "fulltext_fixture")}
			nodes := make([]*storage.Node, 80)
			for i := range nodes {
				label, text := "Source", strings.Repeat("routine correspondence meeting attachment ", 48)
				if i%4 != 0 {
					label = "Unrelated"
				}
				if i%40 == 0 {
					text += " hotel services torrey pines la jolla"
				}
				nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%06d", i)), Labels: []string{label}, Properties: map[string]any{
					"id": fmt.Sprintf("n%06d", i), "text": text, "title": "Correspondence", "current": i%80 == 0,
				}}
			}
			require.NoError(t, store.BulkCreateNodes(nodes))
			exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			t.Logf("B4B 35.12 txFirst=%t search service before any request: %v", txFirst, exec.search() != nil)
			_, err := exec.Execute(context.Background(), "CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text, n.title]", nil)
			require.NoError(t, err)
			const query = "CALL db.index.fulltext.queryNodes('source_text','hotel') YIELD node,score RETURN node.id,score ORDER BY node.id"
			afterIndex := store.scans.Load()
			if txFirst {
				_, err := exec.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
				inside, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				t.Logf("B4B 35.12 txFirst=true inside-transaction rows=%v scans=%d", inside.Rows, store.scans.Load())
				_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
				require.NoError(t, err)
			}
			first, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			afterFirst := store.scans.Load()
			second, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			afterSecond := store.scans.Load()
			third, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			afterThird := store.scans.Load()
			t.Logf("B4B 35.12 txFirst=%t scans: after index=%d after first=%d after second=%d after third=%d; rows equal first/second=%v first/third=%v; first rows=%v; methods: %s",
				txFirst, afterIndex, afterFirst, afterSecond, afterThird,
				fmt.Sprint(first.Rows) == fmt.Sprint(second.Rows), fmt.Sprint(first.Rows) == fmt.Sprint(third.Rows), first.Rows, store.methods())
		})
	}
}
