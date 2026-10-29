package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every vector entry belongs to exactly one node. A node whose own ID looks
// like another node's chunk, named or property vector entry ("doc-chunk-1",
// "x-named-v", "x-prop-emb") keeps its own embedding and is reported under its
// own ID, and the other node's entry is neither overwritten nor attributed to
// it. Checked in memory and after the persisted vector store is reopened.

const vectorOwnerDims = 4

func vectorOwnerForTest(t *testing.T, key string) string {
	t.Helper()
	owner, ok := vectorEntryOwner(key)
	require.True(t, ok)
	return owner
}

func vectorOwnerAxis(axis int) []float32 {
	vector := make([]float32, vectorOwnerDims)
	vector[axis] = 1
	return vector
}

type vectorOwnerQuery struct {
	axis int
	// owner is the node whose embedding lies on axis.
	owner string
	// property selects the owner's embedding for the Cypher route
	// (VectorQuerySpec.Property); "" falls back to its chunk embeddings.
	property string
}

type vectorOwnerCase struct {
	name    string
	nodes   []*storage.Node
	entries int // distinct embeddings that must be stored
	queries []vectorOwnerQuery
}

func vectorOwnerChunkNode(id string, axes ...int) *storage.Node {
	chunks := make([][]float32, 0, len(axes))
	for _, axis := range axes {
		chunks = append(chunks, vectorOwnerAxis(axis))
	}
	return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"},
		Properties: map[string]any{"text": "owner " + id}, ChunkEmbeddings: chunks}
}

func vectorOwnerCases() []vectorOwnerCase {
	var cases []vectorOwnerCase
	// (a) A node whose own ID matches the entry-key grammar, alone and next
	// to the node the grammar would name.
	for _, id := range []string{"doc-chunk-1", "doc-named-v", "doc-prop-emb"} {
		cases = append(cases,
			vectorOwnerCase{
				name:    "own id " + id + " without doc",
				nodes:   []*storage.Node{vectorOwnerChunkNode(id, 0)},
				entries: 1,
				queries: []vectorOwnerQuery{{axis: 0, owner: id}},
			},
			vectorOwnerCase{
				name:    "own id " + id + " with doc",
				nodes:   []*storage.Node{vectorOwnerChunkNode("doc", 1), vectorOwnerChunkNode(id, 0)},
				entries: 2,
				queries: []vectorOwnerQuery{{axis: 0, owner: id}, {axis: 1, owner: "doc"}},
			})
	}
	// (b) Node x with three chunks (entries x, x-chunk-1, x-chunk-2) and node
	// x-chunk-1 with its own embedding.
	chunked := vectorOwnerChunkNode("x", 0, 1, 2)
	cases = append(cases, vectorOwnerCase{
		name:    "chunk entry vs node x-chunk-1",
		nodes:   []*storage.Node{chunked, vectorOwnerChunkNode("x-chunk-1", 3)},
		entries: 4,
		queries: []vectorOwnerQuery{{axis: 1, owner: "x"}, {axis: 3, owner: "x-chunk-1"}},
	})
	// (c) The same for a named vector and a property vector.
	named := &storage.Node{ID: "x", Labels: []string{"Doc"}, Properties: map[string]any{"text": "owner x"},
		NamedEmbeddings: map[string][]float32{"v": vectorOwnerAxis(0)}}
	cases = append(cases, vectorOwnerCase{
		name:    "named entry vs node x-named-v",
		nodes:   []*storage.Node{named, vectorOwnerChunkNode("x-named-v", 1)},
		entries: 2,
		queries: []vectorOwnerQuery{{axis: 0, owner: "x", property: "v"}, {axis: 1, owner: "x-named-v"}},
	})
	property := &storage.Node{ID: "x", Labels: []string{"Doc"},
		Properties: map[string]any{"text": "owner x", "emb": vectorOwnerAxis(0)}}
	cases = append(cases, vectorOwnerCase{
		name:    "property entry vs node x-prop-emb",
		nodes:   []*storage.Node{property, vectorOwnerChunkNode("x-prop-emb", 1)},
		entries: 2,
		queries: []vectorOwnerQuery{{axis: 0, owner: "x", property: "emb"}, {axis: 1, owner: "x-prop-emb"}},
	})
	// (d) A vector name or property key that itself contains an entry-key
	// separator.
	separatorNamed := &storage.Node{ID: "x", Labels: []string{"Doc"}, Properties: map[string]any{"text": "owner x"},
		NamedEmbeddings: map[string][]float32{"a-named-b": vectorOwnerAxis(0)}}
	cases = append(cases, vectorOwnerCase{
		name:    "vector name a-named-b",
		nodes:   []*storage.Node{separatorNamed},
		entries: 1,
		queries: []vectorOwnerQuery{{axis: 0, owner: "x", property: "a-named-b"}},
	})
	separatorProperty := &storage.Node{ID: "x", Labels: []string{"Doc"},
		Properties: map[string]any{"text": "owner x", "p-chunk-1": vectorOwnerAxis(0)}}
	cases = append(cases, vectorOwnerCase{
		name:    "property key p-chunk-1",
		nodes:   []*storage.Node{separatorProperty},
		entries: 1,
		queries: []vectorOwnerQuery{{axis: 0, owner: "x", property: "p-chunk-1"}},
	})
	// A raw node ID may equal an encoded vector key of any entry kind.
	// Its own main entry is escaped, and insert/update/delete must use that
	// same domain rather than deleting the other node's entry by raw ID.
	for _, marker := range []struct {
		name    string
		key     string
		node    *storage.Node
		query   vectorOwnerQuery
		entries int
	}{
		{name: "named", key: nodeNamedVectorKey("doc", "v"), node: &storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "named owner"}, NamedEmbeddings: map[string][]float32{"v": vectorOwnerAxis(0)}}, query: vectorOwnerQuery{axis: 0, owner: "doc", property: "v"}, entries: 2},
		{name: "property", key: nodePropertyVectorKey("doc", "emb"), node: &storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "property owner", "emb": vectorOwnerAxis(0)}}, query: vectorOwnerQuery{axis: 0, owner: "doc", property: "emb"}, entries: 2},
		{name: "chunk", key: nodeChunkVectorKey("doc", 1), node: vectorOwnerChunkNode("doc", 0, 2), query: vectorOwnerQuery{axis: 2, owner: "doc"}, entries: 3},
	} {
		cases = append(cases, vectorOwnerCase{
			name:    "encoded entry vs marker-prefixed node " + marker.name,
			nodes:   []*storage.Node{marker.node, vectorOwnerChunkNode(marker.key, 1)},
			entries: marker.entries,
			queries: []vectorOwnerQuery{marker.query, {axis: 1, owner: marker.key}},
		})
	}
	return cases
}

func cloneVectorOwnerNode(node *storage.Node) *storage.Node {
	properties := make(map[string]any, len(node.Properties))
	for key, value := range node.Properties {
		properties[key] = value
	}
	clone := *node
	clone.Properties = properties
	return &clone
}

// assertVectorOwners checks every public node vector route: the unified
// vector pipeline (VectorSearchCandidates), vector-only Search, and the Cypher
// route (VectorQueryNodes).
func assertVectorOwners(t *testing.T, svc *Service, queries []vectorOwnerQuery) {
	t.Helper()
	ctx := context.Background()
	for _, query := range queries {
		vector := vectorOwnerAxis(query.axis)
		label := fmt.Sprintf("query on axis %d (owner %s)", query.axis, query.owner)

		options := DefaultSearchOptions()
		options.Limit = 10
		candidates, err := svc.VectorSearchCandidates(ctx, vector, options)
		require.NoError(t, err)
		if assert.NotEmpty(t, candidates, "VectorSearchCandidates: %s found nothing", label) {
			assert.Equal(t, query.owner, candidates[0].ID, "VectorSearchCandidates: %s", label)
			assert.InDelta(t, 1, candidates[0].Score, 1e-5, "VectorSearchCandidates: %s", label)
		}

		options = DefaultSearchOptions()
		options.Limit = 10
		response, err := svc.Search(ctx, "", vector, options)
		require.NoError(t, err)
		if assert.NotEmpty(t, response.Results, "Search: %s found nothing", label) {
			assert.Equal(t, query.owner, string(response.Results[0].NodeID), "Search: %s", label)
		}

		hits, err := svc.VectorQueryNodes(ctx, vector, VectorQuerySpec{Property: query.property, Limit: 10})
		require.NoError(t, err)
		if assert.NotEmpty(t, hits, "VectorQueryNodes: %s found nothing", label) {
			assert.Equal(t, query.owner, hits[0].ID, "VectorQueryNodes: %s", label)
			assert.InDelta(t, 1, hits[0].Score, 1e-5, "VectorQueryNodes: %s", label)
		}
	}
}

func TestVectorEntryOwnerInMemory(t *testing.T) {
	for _, tc := range vectorOwnerCases() {
		for _, reversed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reversed=%t", tc.name, reversed), func(t *testing.T) {
				engine := newNamespacedEngine(t)
				nodes := append([]*storage.Node(nil), tc.nodes...)
				if reversed {
					for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
						nodes[i], nodes[j] = nodes[j], nodes[i]
					}
				}
				svc := NewService(engine, vectorOwnerDims, nil)
				t.Cleanup(func() { _ = svc.Close() })
				for _, node := range nodes {
					_, err := engine.CreateNode(cloneVectorOwnerNode(node))
					require.NoError(t, err)
					require.NoError(t, svc.IndexNode(node))
				}
				assert.Equal(t, tc.entries, svc.EmbeddingCount(), "every node's embeddings must be stored under distinct entries")
				assertVectorOwners(t, svc, tc.queries)

				// Removing one node must leave the other node's embeddings.
				removed := nodes[0]
				require.NoError(t, engine.DeleteNode(removed.ID))
				require.NoError(t, svc.RemoveNode(removed.ID))
				var remaining []vectorOwnerQuery
				for _, query := range tc.queries {
					if query.owner != string(removed.ID) {
						remaining = append(remaining, query)
					}
				}
				assertVectorOwners(t, svc, remaining)
			})
		}
	}
}

func TestVectorEntryOwnerAfterPersistedReopen(t *testing.T) {
	for _, tc := range vectorOwnerCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			engine := newNamespacedEngine(t)
			for _, node := range tc.nodes {
				_, err := engine.CreateNode(cloneVectorOwnerNode(node))
				require.NoError(t, err)
			}
			dir := t.TempDir()
			vectorPath := filepath.Join(dir, "vectors")
			configure := func(svc *Service) {
				svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
				svc.SetVectorIndexPath(vectorPath)
				svc.SetPersistenceEnabled(true)
			}

			first := NewService(engine, vectorOwnerDims, nil)
			configure(first)
			require.NoError(t, first.BuildIndexes(ctx))
			require.NotNil(t, first.vectorFileStore, "the build must use the file-backed vector store")
			first.PersistIndexesToDisk()
			require.NoError(t, first.Close())

			// The persisted store itself must hold every embedding.
			stored, err := NewVectorFileStore(vectorPath, vectorOwnerDims)
			require.NoError(t, err)
			require.NoError(t, stored.Load())
			assert.Equal(t, tc.entries, stored.Count(), "the persisted vector store must keep every node's embeddings")
			require.NoError(t, stored.Close())

			second := NewService(engine, vectorOwnerDims, nil)
			t.Cleanup(func() { _ = second.Close() })
			configure(second)
			require.NoError(t, second.BuildIndexes(ctx))
			require.NotNil(t, second.vectorFileStore, "the reopened service must use the persisted vector store")
			assert.Equal(t, tc.entries, second.EmbeddingCount(), "every node's embeddings must survive the reopen")
			assertVectorOwners(t, second, tc.queries)
		})
	}
}
