package cypher

// N153 F3 regression: a full-text index query must not rebuild its document
// view for a request whose fine-grained grants cover the whole indexed corpus.
// The shared search service already owns that corpus, so an unrestricted
// request (including the built-in administrator's whole-graph grants) reads it
// again instead of rescanning storage on every call. A request that may not read
// every indexed entity and property keeps its request-scoped view: it must
// never score hidden text or return a hidden entity or field.
//
// The fixtures separate the two corpora: hidden entities carry the probe term
// as their whole body, so a corpus that kept them would rank them first, and the
// indexed title field repeats the probe term, so a corpus that scored that
// hidden field would change the readable documents' scores. The node and the
// relationship corpora are both covered because a relationship document also
// depends on entity visibility.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	fulltextFGACDatabase   = "fgac_fulltext"
	fulltextFGACDatabaseID = "fgac-fulltext-database"
	fulltextFGACVisible    = 60
	fulltextFGACHidden     = 3
)

// fulltextFGACRankedQuery and friends are the public procedure shapes every
// assertion below uses. The ranked shapes compare order and scores through a
// field the reader may read.
const (
	fulltextFGACRankedQuery = "CALL db.index.fulltext.queryNodes('doc_text', $query) YIELD node, score RETURN node.text AS text, score ORDER BY score DESC, text"
	fulltextFGACTitleQuery  = "CALL db.index.fulltext.queryNodes('doc_text', $query) YIELD node, score RETURN node.title AS title, score ORDER BY score DESC"
	fulltextFGACCountQuery  = "CALL db.index.fulltext.queryNodes('doc_text', $query) YIELD node, score RETURN count(node) AS n"
	fulltextFGACLinkQuery   = "CALL db.index.fulltext.queryRelationships('link_text', $query) YIELD relationship, score RETURN relationship.text AS text, score ORDER BY score DESC, text"
)

// fulltextFGACUniqueTerm is one document's own token, so a benchmark query can
// measure corpus acquisition without hydrating a whole result page as well.
func fulltextFGACUniqueTerm(index int) string { return fmt.Sprintf("unique%05d", index) }

type fulltextFGACCorpus struct {
	exec   *StorageExecutor
	store  *fulltextCountingStore
	admin  auth.FineGrainedRequest
	reader auth.FineGrainedRequest
}

// fulltextFGACNodes is one corpus. withHidden adds the entities and indexed
// values the reader may not read; without them the corpus is exactly the
// reader's visible corpus.
func fulltextFGACNodes(visible, hidden int, withHidden bool) []*storage.Node {
	nodes := make([]*storage.Node, 0, visible+hidden)
	for index := 0; index < visible; index++ {
		properties := map[string]any{
			"text": fmt.Sprintf("zephyrine harbor entry %05d %s %s", index, fulltextFGACUniqueTerm(index), strings.Repeat("ledger ", index%4)),
		}
		if withHidden {
			properties["title"] = "zephyrine zephyrine zephyrine staff notes"
		}
		nodes = append(nodes, &storage.Node{
			ID: storage.NodeID(fmt.Sprintf("doc%05d", index)), Labels: []string{"Doc"}, Properties: properties,
		})
	}
	if !withHidden {
		return nodes
	}
	for index := 0; index < hidden; index++ {
		nodes = append(nodes, &storage.Node{
			ID: storage.NodeID(fmt.Sprintf("hidden%02d", index)), Labels: []string{"Doc", "Hidden"},
			Properties: map[string]any{"text": "zephyrine"},
		})
	}
	return nodes
}

// newFulltextFGACCorpus is the node regression fixture: one small corpus with
// the entities and fields a restricted reader may not read.
func newFulltextFGACCorpus(t testing.TB, withHidden bool) *fulltextFGACCorpus {
	t.Helper()
	return newFulltextFGACCorpusSized(t, fulltextFGACVisible, fulltextFGACHidden, withHidden)
}

// newFulltextFGACCorpusSized builds one database with a declared full-text index
// and the shared search service a database runtime supplies, then binds the two
// principals: whole-graph grants, and grants limited to Doc entities without the
// Hidden label and to the text property.
func newFulltextFGACCorpusSized(t testing.TB, visible, hidden int, withHidden bool) *fulltextFGACCorpus {
	t.Helper()
	store := fulltextFGACStore(t)
	require.NoError(t, store.BulkCreateNodes(t.Context(), fulltextFGACNodes(visible, hidden, withHidden)))
	return fulltextFGACBind(t, store, auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "database-access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "procedures", Action: "database.procedure.execute", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "documents", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Doc"}}},
			{ID: "no-hidden", Action: "graph.node.traverse", Deny: true, Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Hidden"}}},
			{ID: "text", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"text"}}},
			{ID: "whole-graph", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "whole-graph-properties", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
		},
		Assignments: map[string][]string{
			"reader": {"database-access", "procedures", "documents", "no-hidden", "text"},
			"admin":  {"database-access", "procedures", "whole-graph", "whole-graph-properties"},
		},
	}, "CREATE FULLTEXT INDEX doc_text FOR (n:Doc) ON EACH [n.text, n.title]")
}

// fulltextFGACStore is the in-memory database both fixtures index.
func fulltextFGACStore(t testing.TB) *fulltextCountingStore {
	t.Helper()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	return &fulltextCountingStore{NamespacedEngine: storage.NewNamespacedEngine(base, fulltextFGACDatabase)}
}

// fulltextFGACBind wires the shared search service and the two principals, then
// declares the index the queries use.
func fulltextFGACBind(t testing.TB, store *fulltextCountingStore, catalog auth.FineGrainedCatalog, ddl string) *fulltextFGACCorpus {
	t.Helper()
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	service := search.NewService(store, search.DefaultVectorDimensions, nil)
	exec.SetSearchService(service)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	_, err := exec.Execute(t.Context(), ddl, nil)
	require.NoError(t, err)
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), catalog)
	require.NoError(t, err)
	request := func(role string) auth.FineGrainedRequest {
		return auth.FineGrainedRequest{
			Access:      policy.ForRoles([]string{role}),
			Fingerprint: role + "-policy-1",
			DatabaseID: func(name string) string {
				if name == fulltextFGACDatabase {
					return fulltextFGACDatabaseID
				}
				return ""
			},
		}
	}
	return &fulltextFGACCorpus{exec: exec, store: store, admin: request("admin"), reader: request("reader")}
}

// fulltextFGACLinks indexes one relationship corpus. withHidden adds the indexed
// values and endpoints the reader may not read: a repeating title field and
// relationships whose far endpoint carries the Hidden label.
func newFulltextFGACLinkCorpus(t testing.TB, withHidden bool) *fulltextFGACCorpus {
	t.Helper()
	store := fulltextFGACStore(t)
	nodes := []*storage.Node{
		{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "anchor start"}},
		{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"text": "anchor end"}},
	}
	if withHidden {
		nodes = append(nodes, &storage.Node{ID: "h", Labels: []string{"Doc", "Hidden"}, Properties: map[string]any{"text": "anchor hidden"}})
	}
	for _, node := range nodes {
		_, err := store.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	links := make([]*storage.Edge, 0, fulltextFGACVisible+fulltextFGACHidden)
	for index := 0; index < fulltextFGACVisible; index++ {
		properties := map[string]any{"text": fmt.Sprintf("zephyrine relay %05d %s", index, fulltextFGACUniqueTerm(index))}
		if withHidden {
			properties["title"] = "zephyrine zephyrine zephyrine staff notes"
		}
		links = append(links, &storage.Edge{
			ID: storage.EdgeID(fmt.Sprintf("link%05d", index)), Type: "LINK", StartNode: "a", EndNode: "b", Properties: properties,
		})
	}
	if withHidden {
		for index := 0; index < fulltextFGACHidden; index++ {
			links = append(links, &storage.Edge{
				ID: storage.EdgeID(fmt.Sprintf("hiddenlink%02d", index)), Type: "LINK", StartNode: "a", EndNode: "h",
				Properties: map[string]any{"text": "zephyrine"},
			})
		}
	}
	for _, link := range links {
		require.NoError(t, store.CreateEdge(t.Context(), link))
	}
	return fulltextFGACBind(t, store, auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "database-access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "procedures", Action: "database.procedure.execute", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "documents", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Doc"}}},
			{ID: "no-hidden", Action: "graph.node.traverse", Deny: true, Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Hidden"}}},
			{ID: "links", Action: "graph.relationship.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship, Types: []string{"LINK"}}},
			{ID: "link-text", Action: "graph.relationship.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship, Properties: []string{"text"}}},
			{ID: "whole-nodes", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "whole-links", Action: "graph.relationship.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}},
			{ID: "whole-link-properties", Action: "graph.relationship.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}},
		},
		Assignments: map[string][]string{
			"reader": {"database-access", "procedures", "documents", "no-hidden", "links", "link-text"},
			"admin":  {"database-access", "procedures", "whole-nodes", "whole-links", "whole-link-properties"},
		},
	}, "CREATE FULLTEXT INDEX link_text FOR ()-[r:LINK]-() ON EACH [r.text, r.title]")
}

func fulltextFGACRun(t *testing.T, corpus *fulltextFGACCorpus, ctx context.Context, query, term string) [][]any {
	t.Helper()
	result, err := corpus.exec.Execute(ctx, query, map[string]any{"query": term})
	require.NoError(t, err)
	return result.Rows
}

// TestFineGrainedFulltextReusesWholeGraphCorpus: a request whose grants cover
// every indexed entity and property reads the shared document view on repeat,
// and shares the view an unscoped query already populated.
func TestFineGrainedFulltextReusesWholeGraphCorpus(t *testing.T) {
	t.Run("repeat", func(t *testing.T) {
		corpus := newFulltextFGACCorpus(t, true)
		ctx := auth.WithFineGrainedRequest(t.Context(), &corpus.admin)
		first := fulltextFGACRun(t, corpus, ctx, fulltextFGACRankedQuery, "zephyrine")
		require.Len(t, first, fulltextFGACVisible+fulltextFGACHidden)

		corpus.store.take()
		second := fulltextFGACRun(t, corpus, ctx, fulltextFGACRankedQuery, "zephyrine")
		require.Equal(t, first, second)
		scans, _ := corpus.store.take()
		require.Zero(t, scans, "a repeated whole-graph full-text query rebuilt the document view from storage")
	})

	t.Run("shared with unscoped", func(t *testing.T) {
		corpus := newFulltextFGACCorpus(t, true)
		unscoped := fulltextFGACRun(t, corpus, t.Context(), fulltextFGACRankedQuery, "zephyrine")
		require.Len(t, unscoped, fulltextFGACVisible+fulltextFGACHidden)

		corpus.store.take()
		scoped := auth.WithFineGrainedRequest(t.Context(), &corpus.admin)
		granted := fulltextFGACRun(t, corpus, scoped, fulltextFGACRankedQuery, "zephyrine")
		require.Equal(t, unscoped, granted)
		scans, _ := corpus.store.take()
		require.Zero(t, scans, "a whole-graph full-text query rebuilt a view it may share")
	})

	t.Run("graph revision", func(t *testing.T) {
		corpus := newFulltextFGACCorpus(t, true)
		ctx := auth.WithFineGrainedRequest(t.Context(), &corpus.admin)
		require.Len(t, fulltextFGACRun(t, corpus, ctx, fulltextFGACRankedQuery, "zephyrine"), fulltextFGACVisible+fulltextFGACHidden)

		node, err := corpus.store.GetNode(t.Context(), "doc00000")
		require.NoError(t, err)
		node.Properties["text"] = "quantumbridge entry"
		require.NoError(t, corpus.store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))

		corpus.store.take()
		changed := fulltextFGACRun(t, corpus, ctx, fulltextFGACRankedQuery, "quantumbridge")
		require.Len(t, changed, 1)
		require.Equal(t, "quantumbridge entry", changed[0][0])
		scans, _ := corpus.store.take()
		require.Zero(t, scans, "a committed change refreshed the shared view without rereading storage")
	})

	t.Run("relationships", func(t *testing.T) {
		corpus := newFulltextFGACLinkCorpus(t, true)
		ctx := auth.WithFineGrainedRequest(t.Context(), &corpus.admin)
		first := fulltextFGACRun(t, corpus, ctx, fulltextFGACLinkQuery, "zephyrine")
		require.Len(t, first, fulltextFGACVisible+fulltextFGACHidden)

		corpus.store.take()
		corpus.store.takeEdgeLists()
		second := fulltextFGACRun(t, corpus, ctx, fulltextFGACLinkQuery, "zephyrine")
		require.Equal(t, first, second)
		scans, _ := corpus.store.take()
		require.Zero(t, scans, "a repeated whole-graph relationship query streamed the document view from storage")
		require.Zero(t, corpus.store.takeEdgeLists(), "a repeated whole-graph relationship query rebuilt the document view from storage")
	})
}

// TestFineGrainedRestrictedFulltextStaysRequestScoped: a reader that may not read
// every indexed entity or property scores and returns only its visible corpus,
// and neither reuses nor damages the shared whole-graph view.
func TestFineGrainedRestrictedFulltextStaysRequestScoped(t *testing.T) {
	t.Run("nodes", func(t *testing.T) {
		visibleOnly := newFulltextFGACCorpus(t, false)
		withHidden := newFulltextFGACCorpus(t, true)
		reference := fulltextFGACRun(t, visibleOnly, t.Context(), fulltextFGACRankedQuery, "zephyrine")
		require.Len(t, reference, fulltextFGACVisible)

		reader := auth.WithFineGrainedRequest(t.Context(), &withHidden.reader)
		withHidden.store.take()
		visible := fulltextFGACRun(t, withHidden, reader, fulltextFGACRankedQuery, "zephyrine")
		require.Equal(t, reference, visible, "a reader's ranked corpus must be its readable documents alone")
		scans, _ := withHidden.store.take()
		require.NotZero(t, scans, "a restricted full-text query must project its readable corpus")

		// The hidden title field holds the probe term; a reader must not match or
		// return it, while the same corpus does match it for whole-graph grants.
		require.Empty(t, fulltextFGACRun(t, withHidden, reader, fulltextFGACTitleQuery, "staff"))
		admin := auth.WithFineGrainedRequest(t.Context(), &withHidden.admin)
		before := fulltextFGACRun(t, withHidden, admin, fulltextFGACRankedQuery, "zephyrine")
		require.Len(t, before, fulltextFGACVisible+fulltextFGACHidden)
		require.Len(t, fulltextFGACRun(t, withHidden, admin, fulltextFGACTitleQuery, "staff"), fulltextFGACVisible)

		// A reader's call must leave the whole-graph view it may not use unchanged.
		withHidden.store.take()
		require.Equal(t, reference, fulltextFGACRun(t, withHidden, reader, fulltextFGACRankedQuery, "zephyrine"))
		scans, _ = withHidden.store.take()
		require.Zero(t, scans, "a restricted full-text query should reuse its own policy view")
		require.Equal(t, before, fulltextFGACRun(t, withHidden, admin, fulltextFGACRankedQuery, "zephyrine"))
		scans, _ = withHidden.store.take()
		require.Zero(t, scans, "the whole-graph view must survive a restricted call")
	})

	t.Run("relationships", func(t *testing.T) {
		visibleOnly := newFulltextFGACLinkCorpus(t, false)
		withHidden := newFulltextFGACLinkCorpus(t, true)
		reference := fulltextFGACRun(t, visibleOnly, t.Context(), fulltextFGACLinkQuery, "zephyrine")
		require.Len(t, reference, fulltextFGACVisible)

		reader := auth.WithFineGrainedRequest(t.Context(), &withHidden.reader)
		withHidden.store.take()
		withHidden.store.takeEdgeLists()
		visible := fulltextFGACRun(t, withHidden, reader, fulltextFGACLinkQuery, "zephyrine")
		require.Equal(t, reference, visible, "a reader's ranked corpus must be its readable relationships alone")
		require.NotZero(t, withHidden.store.takeEdgeLists(), "a restricted relationship query must project its readable corpus")

		admin := auth.WithFineGrainedRequest(t.Context(), &withHidden.admin)
		require.Len(t, fulltextFGACRun(t, withHidden, admin, fulltextFGACLinkQuery, "zephyrine"), fulltextFGACVisible+fulltextFGACHidden)
		require.Empty(t, fulltextFGACRun(t, withHidden, reader, fulltextFGACLinkQuery, "staff"))
		require.Len(t, fulltextFGACRun(t, withHidden, admin, fulltextFGACLinkQuery, "staff"), fulltextFGACVisible)

		withHidden.store.take()
		withHidden.store.takeEdgeLists()
		require.Equal(t, reference, fulltextFGACRun(t, withHidden, reader, fulltextFGACLinkQuery, "zephyrine"))
		require.Zero(t, withHidden.store.takeEdgeLists(), "a restricted relationship query should reuse its own policy view")
	})
}

func TestFineGrainedRestrictedFulltextInvalidatesOnGraphAndPolicyRevision(t *testing.T) {
	corpus := newFulltextFGACCorpus(t, true)
	ctx := auth.WithFineGrainedRequest(t.Context(), &corpus.reader)
	initial := fulltextFGACRun(t, corpus, ctx, fulltextFGACRankedQuery, "zephyrine")
	require.Len(t, initial, fulltextFGACVisible)
	corpus.store.take()
	require.Equal(t, initial, fulltextFGACRun(t, corpus, ctx, fulltextFGACRankedQuery, "zephyrine"))
	scans, _ := corpus.store.take()
	require.Zero(t, scans)

	node, err := corpus.store.GetNode(t.Context(), "doc00000")
	require.NoError(t, err)
	node.Properties["text"] = "newterm entry"
	require.NoError(t, corpus.store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
	changed := fulltextFGACRun(t, corpus, ctx, fulltextFGACRankedQuery, "newterm")
	require.Len(t, changed, 1)
	require.Equal(t, "newterm entry", changed[0][0])
	scans, _ = corpus.store.take()
	require.NotZero(t, scans, "a graph revision must rebuild the projected corpus")

	revised := corpus.reader
	revised.Fingerprint = "reader-policy-2"
	revisedCtx := auth.WithFineGrainedRequest(t.Context(), &revised)
	require.Equal(t, changed, fulltextFGACRun(t, corpus, revisedCtx, fulltextFGACRankedQuery, "newterm"))
	scans, _ = corpus.store.take()
	require.NotZero(t, scans, "a policy revision must not reuse the old projected corpus")
}

// BenchmarkNOR153FGACFulltextSharedView measures the public full-text procedure
// on the named 10k-node fixture with one document's own term, so the query
// isolates corpus acquisition from result hydration. A whole-graph request must
// read the shared document view on every repeat; a restricted request keeps
// paying for its own request-scoped projection, which is the visibility it is
// entitled to.
func BenchmarkNOR153FGACFulltextSharedView(b *testing.B) {
	const nodes = 10000
	for _, tc := range []struct {
		name    string
		request func(*fulltextFGACCorpus) *auth.FineGrainedRequest
	}{
		{name: "whole-graph", request: func(c *fulltextFGACCorpus) *auth.FineGrainedRequest { return &c.admin }},
		{name: "restricted", request: func(c *fulltextFGACCorpus) *auth.FineGrainedRequest { return &c.reader }},
	} {
		b.Run(fmt.Sprintf("nodes=%d/%s", nodes, tc.name), func(b *testing.B) {
			corpus := newFulltextFGACCorpusSized(b, nodes, fulltextFGACHidden, true)
			ctx := auth.WithFineGrainedRequest(b.Context(), tc.request(corpus))
			params := map[string]any{"query": fulltextFGACUniqueTerm(nodes / 2)}
			warm, err := corpus.exec.Execute(ctx, fulltextFGACCountQuery, params)
			require.NoError(b, err)
			require.Equal(b, int64(1), warm.Rows[0][0])
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := corpus.exec.Execute(ctx, fulltextFGACCountQuery, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
