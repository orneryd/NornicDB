package search

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// N152-287: hidden searchable text must not change which readable keyword hits
// a fine-grained search returns. N152-289: it must not change their scores.
// A search whose grants cover the whole graph reads the shared index instead of
// projecting a readable corpus, so it costs no extra scan.

// keywordCorpusReadSpy records the corpus reads a search makes, so a test can
// separate a projected corpus scan from ordinary result hydration.
type keywordCorpusReadSpy struct {
	storage.Engine
	mu      sync.Mutex
	batches [][]storage.NodeID
	edges   []storage.EdgeID
}

func (spy *keywordCorpusReadSpy) GetInnerEngine() storage.Engine { return spy.Engine }

func (spy *keywordCorpusReadSpy) BatchGetNodes(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	spy.mu.Lock()
	spy.batches = append(spy.batches, append([]storage.NodeID(nil), ids...))
	spy.mu.Unlock()
	return spy.Engine.BatchGetNodes(ctx, ids)
}

func (spy *keywordCorpusReadSpy) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	spy.mu.Lock()
	spy.edges = append(spy.edges, id)
	spy.mu.Unlock()
	return spy.Engine.GetEdge(ctx, id)
}

func (spy *keywordCorpusReadSpy) GraphMutationVersionInNamespace(namespace string) (uint64, bool) {
	provider, ok := spy.Engine.(storage.NamespaceGraphMutationVersionProvider)
	if !ok {
		return 0, false
	}
	return provider.GraphMutationVersionInNamespace(namespace)
}

func (spy *keywordCorpusReadSpy) reset() {
	spy.mu.Lock()
	spy.batches, spy.edges = nil, nil
	spy.mu.Unlock()
}

func (spy *keywordCorpusReadSpy) readNode(suffix string) bool {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	for _, batch := range spy.batches {
		for _, id := range batch {
			if strings.HasSuffix(string(id), ":"+suffix) {
				return true
			}
		}
	}
	return false
}

func (spy *keywordCorpusReadSpy) readEdge(suffix string) bool {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	for _, id := range spy.edges {
		if strings.HasSuffix(string(id), ":"+suffix) {
			return true
		}
	}
	return false
}

// keywordScores is a response's scores by result ID.
func keywordScores(results []SearchResult) map[string]float64 {
	scores := make(map[string]float64, len(results))
	for _, result := range results {
		scores[string(result.ID)] = result.Score
	}
	return scores
}

// orderedIDs is a response's result IDs in rank order.
func orderedIDs(results []SearchResult) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		ids = append(ids, string(result.ID))
	}
	return ids
}

// sortedIDs is a response's result IDs in a corpus-independent order.
func sortedIDs(results []SearchResult) []string {
	ids := orderedIDs(results)
	sort.Strings(ids)
	return ids
}

// fineGrainedKeywordNodeCorpus holds one Article corpus with, optionally,
// hidden searchable text: a hidden property on a readable Article and Internal
// nodes the reader cannot traverse.
type fineGrainedKeywordNodeCorpus struct {
	service    *Service
	reader     auth.FineGrainedRequest
	unrestrict auth.FineGrainedRequest
	spy        *keywordCorpusReadSpy
}

func newFineGrainedKeywordNodeCorpus(t *testing.T, hidden bool) *fineGrainedKeywordNodeCorpus {
	t.Helper()
	ctx := t.Context()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	spy := &keywordCorpusReadSpy{Engine: base}
	store := storage.NewNamespacedEngine(spy, "work")
	one := map[string]any{"content": "albatross"}
	if hidden {
		one["draft"] = "albatross forbiddenword alpha"
	}
	nodes := []*storage.Node{
		{ID: "one", Labels: []string{"Article"}, Properties: one},
		{ID: "two", Labels: []string{"Article"}, Properties: map[string]any{"content": "albatross"}},
		{ID: "three", Labels: []string{"Article"}, Properties: map[string]any{"content": "albatross"}},
		{ID: "noise", Labels: []string{"Article"}, Properties: map[string]any{"content": "pelican"}},
	}
	if hidden {
		for _, id := range []string{"secret-1", "secret-2", "secret-3"} {
			nodes = append(nodes, &storage.Node{ID: storage.NodeID(id), Labels: []string{"Internal"},
				Properties: map[string]any{"content": "albatross albatross albatross"}})
		}
	}
	for _, node := range nodes {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	service := NewService(store, 3, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.SetIndexFlags(true, false)
	require.NoError(t, service.BuildIndexes(ctx))

	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "work", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "articles", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Article"}}},
			{ID: "content", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"content"}}},
			{ID: "all-nodes", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "all-properties", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
		},
		Assignments: map[string][]string{
			"reader": {"work", "articles", "content"},
			"admin":  {"work", "all-nodes", "all-properties"},
		},
	})
	require.NoError(t, err)
	request := func(role string) auth.FineGrainedRequest {
		return auth.FineGrainedRequest{Access: policy.ForRoles([]string{role}), DatabaseID: func(string) string { return "work-id" }}
	}
	return &fineGrainedKeywordNodeCorpus{service: service, spy: spy, reader: request("reader"), unrestrict: request("admin")}
}

// fineGrainedKeywordRelationshipCorpus holds one CITES corpus with, optionally,
// hidden searchable text: a hidden property on a readable relationship and the
// SECRET relationships the reader cannot traverse. Each SECRET relationship's
// text is short, so it scores above every readable relationship.
type fineGrainedKeywordRelationshipCorpus struct {
	service    *Service
	reader     auth.FineGrainedRequest
	unrestrict auth.FineGrainedRequest
	spy        *keywordCorpusReadSpy
}

func newFineGrainedKeywordRelationshipCorpus(t *testing.T, secretEdges int) *fineGrainedKeywordRelationshipCorpus {
	t.Helper()
	ctx := t.Context()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	spy := &keywordCorpusReadSpy{Engine: base}
	store := storage.NewNamespacedEngine(spy, "work")
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		_, err := store.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Doc"},
			Properties: map[string]any{"name": "doc " + string(id)}})
		require.NoError(t, err)
	}
	readable := map[string]any{"reason": "zephyrine bridge over the river"}
	if secretEdges > 0 {
		readable["editorial"] = "zephyrine zephyrine secret staff notes"
	}
	edges := []*storage.Edge{
		{ID: "r1", StartNode: "a", EndNode: "b", Type: "CITES", Properties: readable},
		{ID: "r2", StartNode: "b", EndNode: "c", Type: "CITES", Properties: map[string]any{"reason": "zephyrine crossing"}},
		{ID: "r3", StartNode: "c", EndNode: "a", Type: "CITES", Properties: map[string]any{"reason": "zephyrine road"}},
		{ID: "noise", StartNode: "a", EndNode: "c", Type: "CITES", Properties: map[string]any{"reason": "pelican"}},
	}
	for index := range secretEdges {
		edges = append(edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("secret-%d", index+1)),
			StartNode: "a", EndNode: "b", Type: "SECRET", Properties: map[string]any{"reason": "zephyrine"}})
	}
	for _, edge := range edges {
		require.NoError(t, store.CreateEdge(ctx, edge))
	}

	service := NewService(store, 3, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.EnableRelationshipSearch(nil)
	service.StartRelationshipIndex()

	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "work", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "nodes", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "cites", Action: "graph.relationship.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship, Types: []string{"CITES"}}},
			{ID: "reason", Action: "graph.relationship.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship, Properties: []string{"reason"}}},
			{ID: "all-edges", Action: "graph.relationship.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}},
			{ID: "all-edge-properties", Action: "graph.relationship.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}},
		},
		Assignments: map[string][]string{
			"reader": {"work", "nodes", "cites", "reason"},
			"admin":  {"work", "nodes", "all-edges", "all-edge-properties"},
		},
	})
	require.NoError(t, err)
	request := func(role string) auth.FineGrainedRequest {
		return auth.FineGrainedRequest{Access: policy.ForRoles([]string{role}), DatabaseID: func(string) string { return "work-id" }}
	}
	return &fineGrainedKeywordRelationshipCorpus{service: service, spy: spy, reader: request("reader"), unrestrict: request("admin")}
}

// readableRelationshipIndex is the keyword index of the readable relationships
// alone: the corpus the fine-grained search must rank as if the hidden
// relationships did not exist.
func readableRelationshipIndex(service *Service) bm25Index {
	reference := NewFulltextIndexV2WithAnalyzer(service.bm25Analyzer)
	reasons := map[string]string{
		"r1": "zephyrine bridge over the river", "r2": "zephyrine crossing",
		"r3": "zephyrine road", "noise": "pelican",
	}
	for _, id := range []string{"r1", "r2", "r3", "noise"} {
		text := service.relationshipSearchableText(&storage.Edge{Type: "CITES", Properties: map[string]any{"reason": reasons[id]}})
		reference.Index(id, text, nil, nil, false)
	}
	return reference
}

// TestFineGrainedKeywordSearchShowsSameRelationshipsWithHiddenText: a reader
// sees the readable relationships, in the same order and with the same scores,
// whether or not hidden searchable text exists.
func TestFineGrainedKeywordSearchShowsSameRelationshipsWithHiddenText(t *testing.T) {
	visibleOnly := newFineGrainedKeywordRelationshipCorpus(t, 0)
	withHidden := newFineGrainedKeywordRelationshipCorpus(t, 2)
	options := &SearchOptions{Limit: 10}

	plain, err := visibleOnly.service.SearchRelationships(t.Context(), "zephyrine", nil, options)
	require.NoError(t, err)
	require.Len(t, plain.Results, 3)

	scoped := auth.WithFineGrainedRequest(t.Context(), &withHidden.reader)
	got, err := withHidden.service.SearchRelationships(scoped, "zephyrine", nil, options)
	require.NoError(t, err)
	require.Len(t, got.Results, 3)
	for _, result := range got.Results {
		require.NotContains(t, result.Properties, "editorial")
		require.NotContains(t, string(result.ID), "secret")
	}

	reference := readableRelationshipIndex(withHidden.service).Search("zephyrine", 50)
	require.Len(t, reference, 3)
	require.Equal(t, orderedIDs(plain.Results), orderedIDs(got.Results))
	require.Equal(t, keywordScores(plain.Results), keywordScores(got.Results))
	// A relationship result's score is the RRF of its BM25 rank.
	for rank, hit := range reference {
		require.Equal(t, hit.ID, got.Results[rank].ID)
		require.Equal(t, rank+1, got.Results[rank].BM25Rank)
		require.InDelta(t, 1/(60+float64(rank+1)), got.Results[rank].Score, 1e-12)
	}
}

// TestFineGrainedKeywordSearchHiddenRelationshipsDoNotDisplaceReadableHits:
// hidden relationships that outrank every readable one must not consume the
// ranked window a reader's hits are taken from.
func TestFineGrainedKeywordSearchHiddenRelationshipsDoNotDisplaceReadableHits(t *testing.T) {
	const hiddenEdges = 55 // more than the search's minimum ranked depth (50)
	visibleOnly := newFineGrainedKeywordRelationshipCorpus(t, 0)
	withHidden := newFineGrainedKeywordRelationshipCorpus(t, hiddenEdges)
	options := &SearchOptions{Limit: 10}

	// The shared index ranks hidden relationships first, so a search that took
	// its ranked window from the shared index would return no readable hit.
	shared := readableRelationshipIndex(withHidden.service)
	secretText := withHidden.service.relationshipSearchableText(&storage.Edge{Type: "SECRET", Properties: map[string]any{"reason": "zephyrine"}})
	for index := range hiddenEdges {
		shared.Index(fmt.Sprintf("secret-%d", index+1), secretText, nil, nil, false)
	}
	sharedHits := shared.Search("zephyrine", 50)
	require.Len(t, sharedHits, 50)
	for _, hit := range sharedHits {
		require.NotContains(t, []string{"r1", "r2", "r3"}, hit.ID)
	}

	plain, err := visibleOnly.service.SearchRelationships(t.Context(), "zephyrine", nil, options)
	require.NoError(t, err)
	require.Len(t, plain.Results, 3)

	scoped := auth.WithFineGrainedRequest(t.Context(), &withHidden.reader)
	got, err := withHidden.service.SearchRelationships(scoped, "zephyrine", nil, options)
	require.NoError(t, err)
	require.Equal(t, orderedIDs(plain.Results), orderedIDs(got.Results))
	require.Equal(t, keywordScores(plain.Results), keywordScores(got.Results))
}

// TestFineGrainedUnrestrictedKeywordSearchSkipsCorpusProjection: grants that
// cover the whole graph use the shared index, so they read no candidate the
// search did not already return, while a restricted reader still projects the
// corpus it can read.
func TestFineGrainedUnrestrictedKeywordSearchSkipsCorpusProjection(t *testing.T) {
	t.Run("nodes", func(t *testing.T) {
		corpus := newFineGrainedKeywordNodeCorpus(t, true)
		unrestricted, err := corpus.service.Search(t.Context(), "albatross", nil, nil)
		require.NoError(t, err)
		require.Len(t, unrestricted.Results, 6)

		corpus.spy.reset()
		scoped := auth.WithFineGrainedRequest(t.Context(), &corpus.unrestrict)
		granted, err := corpus.service.Search(scoped, "albatross", nil, nil)
		require.NoError(t, err)
		require.Equal(t, sortedIDs(unrestricted.Results), sortedIDs(granted.Results))
		require.Equal(t, keywordScores(unrestricted.Results), keywordScores(granted.Results))
		require.False(t, corpus.spy.readNode("noise"), "whole-graph search scanned the node corpus")

		corpus.spy.reset()
		restricted := auth.WithFineGrainedRequest(t.Context(), &corpus.reader)
		visible, err := corpus.service.Search(restricted, "albatross", nil, nil)
		require.NoError(t, err)
		require.Len(t, visible.Results, 3)
		require.True(t, corpus.spy.readNode("noise"), "restricted search must project its readable corpus")
	})

	t.Run("relationships", func(t *testing.T) {
		corpus := newFineGrainedKeywordRelationshipCorpus(t, 2)
		options := &SearchOptions{Limit: 10}
		unrestricted, err := corpus.service.SearchRelationships(t.Context(), "zephyrine", nil, options)
		require.NoError(t, err)
		require.Len(t, unrestricted.Results, 5)

		corpus.spy.reset()
		scoped := auth.WithFineGrainedRequest(t.Context(), &corpus.unrestrict)
		granted, err := corpus.service.SearchRelationships(scoped, "zephyrine", nil, options)
		require.NoError(t, err)
		require.Equal(t, orderedIDs(unrestricted.Results), orderedIDs(granted.Results))
		require.False(t, corpus.spy.readEdge("noise"), "whole-graph relationship search scanned the corpus")

		corpus.spy.reset()
		restricted := auth.WithFineGrainedRequest(t.Context(), &corpus.reader)
		visible, err := corpus.service.SearchRelationships(restricted, "zephyrine", nil, options)
		require.NoError(t, err)
		require.Len(t, visible.Results, 3)
		require.True(t, corpus.spy.readEdge("noise"), "restricted relationship search must project its readable corpus")
	})
}

// A stable policy and index revision must reuse the authorization work needed
// for visible-only scoring. Re-reading unrelated documents on every search
// makes restricted keyword queries proportional to the whole database.
func TestFineGrainedRestrictedKeywordSearchReusesVisibleCorpus(t *testing.T) {
	t.Run("nodes", func(t *testing.T) {
		corpus := newFineGrainedKeywordNodeCorpus(t, true)
		corpus.reader.Fingerprint = "reader-policy"
		ctx := auth.WithFineGrainedRequest(t.Context(), &corpus.reader)
		first, err := corpus.service.Search(ctx, "albatross", nil, nil)
		require.NoError(t, err)
		corpus.spy.reset()
		second, err := corpus.service.Search(ctx, "albatross albatross", nil, nil)
		require.NoError(t, err)
		require.Equal(t, orderedIDs(first.Results), orderedIDs(second.Results))
		require.False(t, corpus.spy.readNode("noise"), "stable policy rebuilt the whole node corpus")
	})

	t.Run("relationships", func(t *testing.T) {
		corpus := newFineGrainedKeywordRelationshipCorpus(t, 2)
		corpus.reader.Fingerprint = "reader-policy"
		ctx := auth.WithFineGrainedRequest(t.Context(), &corpus.reader)
		options := &SearchOptions{Limit: 10}
		first, err := corpus.service.SearchRelationships(ctx, "zephyrine", nil, options)
		require.NoError(t, err)
		corpus.spy.reset()
		second, err := corpus.service.SearchRelationships(ctx, "zephyrine zephyrine", nil, options)
		require.NoError(t, err)
		require.Equal(t, orderedIDs(first.Results), orderedIDs(second.Results))
		require.False(t, corpus.spy.readEdge("noise"), "stable policy rebuilt the whole relationship corpus")
	})
}

func TestFineGrainedKeywordSearchHiddenNodeTextPreservesAllVisibleRanks(t *testing.T) {
	visibleOnly := newFineGrainedKeywordNodeCorpus(t, false)
	withHidden := newFineGrainedKeywordNodeCorpus(t, true)
	options := &SearchOptions{Limit: 10}
	plain, err := visibleOnly.service.Search(t.Context(), "albatross", nil, options)
	require.NoError(t, err)
	got, err := withHidden.service.Search(auth.WithFineGrainedRequest(t.Context(), &withHidden.reader), "albatross", nil, options)
	require.NoError(t, err)
	require.Len(t, got.Results, 3)
	require.Equal(t, orderedIDs(plain.Results), orderedIDs(got.Results))
	require.Equal(t, keywordScores(plain.Results), keywordScores(got.Results))
	for rank, hit := range got.Results {
		require.Equal(t, rank+1, hit.BM25Rank)
		require.NotContains(t, hit.Properties, "draft")
	}
}

func TestFineGrainedRestrictedKeywordCorpusInvalidatesOnGraphChange(t *testing.T) {
	corpus := newFineGrainedKeywordNodeCorpus(t, true)
	corpus.reader.Fingerprint = "reader-policy"
	ctx := auth.WithFineGrainedRequest(t.Context(), &corpus.reader)
	first, err := corpus.service.Search(ctx, "albatross", nil, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	require.Len(t, first.Results, 3)

	node, err := corpus.service.engine.GetNode(t.Context(), "noise")
	require.NoError(t, err)
	node.Properties["content"] = "albatross"
	require.NoError(t, corpus.service.engine.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
	require.NoError(t, corpus.service.IndexNode(node))

	second, err := corpus.service.Search(ctx, "albatross", nil, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	require.Len(t, second.Results, 4)
	require.Contains(t, orderedIDs(second.Results), "noise")
}

func TestFineGrainedRestrictedKeywordCorpusDoesNotCacheDecayReads(t *testing.T) {
	corpus := newFineGrainedKeywordNodeCorpus(t, true)
	corpus.reader.Fingerprint = "reader-policy"
	owner, ok := storage.UnwrapEngine(corpus.service.engine).(*storage.BadgerEngine)
	require.True(t, ok)
	owner.SetDecayEnabled(true)
	ctx := auth.WithFineGrainedRequest(t.Context(), &corpus.reader)
	_, err := corpus.service.Search(ctx, "albatross", nil, nil)
	require.NoError(t, err)
	corpus.spy.reset()
	_, err = corpus.service.Search(ctx, "albatross albatross", nil, nil)
	require.NoError(t, err)
	require.True(t, corpus.spy.readNode("noise"), "time-based decay requires fresh visibility statistics")
}

func TestSearchableFieldTextsComposeIndexedText(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	service := NewService(base, 3, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	node := &storage.Node{Labels: []string{"Article", "Public"}, Properties: map[string]any{
		"content": "albatross harbor", "a-custom": "wind", "draft": "hidden winter",
	}}
	for _, properties := range [][]string{nil, {"content", "draft"}} {
		fields := service.searchableFieldTexts(node, properties)
		texts := make([]string, len(fields))
		for i, field := range fields {
			texts[i] = field.Text
		}
		require.Equal(t, service.searchableText(node, properties), strings.Join(texts, " "))
	}
}
