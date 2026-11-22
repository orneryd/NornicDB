package cypher

// Port helper: newNOR406RetrievalFixtureOnEngine as it appears in the newer
// revision of retrieval_screening_bench_test.go in Plane item N152-90 (block 16),
// which the block-15 test needs; the pin's copy of that file lacks it. The bulk
// helpers take testing.TB here because the pin's take *testing.B.

import (
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func newNOR406RetrievalFixtureOnEngine(tb testing.TB, population int, engine storage.Engine) *nor406RetrievalFixture {
	tb.Helper()
	ctx := tb.Context()
	store := storage.NewNamespacedEngine(engine, "nor406")

	nodes := make([]*storage.Node, 0, nor406Collections+2*nor406SourceGroups+population)
	edges := make([]*storage.Edge, 0, 2*nor406SourceGroups+population)

	for collection := 0; collection < nor406Collections; collection++ {
		id := "collection-" + strconv.Itoa(collection)
		nodes = append(nodes, &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"Collection"},
			Properties: map[string]any{"id": id, "archived": collection == 2},
		})
	}

	for group := 0; group < nor406SourceGroups; group++ {
		groupID := strconv.Itoa(group)
		documentID := "document-" + groupID
		versionID := "version-" + groupID
		collectionID := "collection-" + strconv.Itoa(group%nor406Collections)
		currentSourceID := nor406SourceIDPrefix + groupID
		nodes = append(nodes,
			&storage.Node{ID: storage.NodeID(documentID), Labels: []string{"Document"}, Properties: map[string]any{"id": documentID}},
			&storage.Node{ID: storage.NodeID(versionID), Labels: []string{"Version"}, Properties: map[string]any{"id": versionID, "current_source_id": currentSourceID}},
		)
		edges = append(edges,
			&storage.Edge{ID: storage.EdgeID("member-of-" + groupID), StartNode: storage.NodeID(documentID), EndNode: storage.NodeID(collectionID), Type: "MEMBER_OF"},
			&storage.Edge{ID: storage.EdgeID("has-version-" + groupID), StartNode: storage.NodeID(documentID), EndNode: storage.NodeID(versionID), Type: "HAS_VERSION"},
		)
	}

	for index := 0; index < population; index++ {
		group := index % nor406SourceGroups
		id := nor406SourceIDPrefix + strconv.Itoa(index)
		current := index == group
		nodes = append(nodes, &storage.Node{
			ID:              storage.NodeID(id),
			Labels:          []string{"SearchSource"},
			Properties:      map[string]any{"id": id, "group": int64(group), "content": nor406SourceContent(index)},
			ChunkEmbeddings: [][]float32{nor406SourceEmbedding(index, current)},
		})
		edges = append(edges, &storage.Edge{
			ID:        storage.EdgeID("has-source-" + strconv.Itoa(index)),
			StartNode: storage.NodeID("version-" + strconv.Itoa(group)),
			EndNode:   storage.NodeID(id),
			Type:      "HAS_SOURCE",
		})
	}

	v152NOR406BulkCreateNodes(tb, store, nodes)
	v152NOR406BulkCreateEdges(tb, store, edges)

	service := search.NewService(store, nor406EmbeddingDimensions, nil)
	tb.Cleanup(func() {
		if err := service.Close(); err != nil {
			tb.Errorf("close search service: %v", err)
		}
	})
	if err := service.BuildIndexes(ctx); err != nil {
		tb.Fatalf("build search indexes: %v", err)
	}

	fixture := &nor406RetrievalFixture{
		store:          store,
		service:        service,
		queryEmbedding: nor406QueryEmbedding(),
		collectionIDs:  []any{"collection-0", "collection-1", "collection-2"},
	}
	service.SetReranker(&fixture.provider)
	return fixture
}

func v152NOR406BulkCreateNodes(tb testing.TB, store *storage.NamespacedEngine, nodes []*storage.Node) {
	tb.Helper()
	const chunk = 500
	for start := 0; start < len(nodes); start += chunk {
		end := min(start+chunk, len(nodes))
		if err := store.BulkCreateNodes(tb.Context(), nodes[start:end]); err != nil {
			tb.Fatalf("create nodes %d-%d: %v", start, end, err)
		}
	}
}

func v152NOR406BulkCreateEdges(tb testing.TB, store *storage.NamespacedEngine, edges []*storage.Edge) {
	tb.Helper()
	const chunk = 2000
	for start := 0; start < len(edges); start += chunk {
		end := min(start+chunk, len(edges))
		if err := store.BulkCreateEdges(tb.Context(), edges[start:end]); err != nil {
			tb.Fatalf("create edges %d-%d: %v", start, end, err)
		}
	}
}
