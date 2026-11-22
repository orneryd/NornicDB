package search

// Scratch, test-only: cause diagnostics for the FGAC search findings. For the
// same visible data with hidden data present (S0) and absent (S2), log the
// values that decide the restricted caller's answer at the owners:
//   - the shared BM25 corpus statistics the scoped query is scored with;
//   - for every shared keyword hit, the indexed document and the caller's
//     authorized text compared in adaptiveBM25Search (search.go) and in
//     SearchRelationships (relationship_search.go).

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func fgacCauseNodes(hidden bool) []*storage.Node {
	person := func(id, name, bio string, extra map[string]any) *storage.Node {
		props := map[string]any{"name": name, "bio": bio, "dept": "eng"}
		if hidden {
			for k, v := range extra {
				props[k] = v
			}
		}
		return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Person"}, Properties: props}
	}
	nodes := []*storage.Node{
		person("alice", "alice", "alpha bravo", map[string]any{"ssn": "ZQX-SSN-0", "secretnote": "zqxkeyword alpha alpha alpha"}),
		person("bob", "bob", "alpha charlie", map[string]any{"ssn": "ZQX-SSN-1", "secretnote": "delta echo"}),
		person("carol", "carol", "bravo charlie", map[string]any{"ssn": "ZQX-SSN-2", "secretnote": "zqxkeyword"}),
		person("dave", "dave", "alpha", map[string]any{"ssn": "ZQX-SSN-3"}),
		{ID: "doc0", Labels: []string{"Doc"}, Properties: map[string]any{"title": "alpha doc"}},
		{ID: "memo0", Labels: []string{"Note"}, Properties: map[string]any{"text": "alpha memo", "level": "low"}},
	}
	if hidden {
		nodes = append(nodes,
			&storage.Node{ID: "secret", Labels: []string{"Person", "Secret"}, Properties: map[string]any{"name": "aaa-zqx", "bio": "alpha alpha alpha bravo", "ssn": "ZQX-SSN-H"}},
			&storage.Node{ID: "memotop", Labels: []string{"Note"}, Properties: map[string]any{"text": "alpha alpha zqxtop memo", "level": "top"}},
		)
	}
	return nodes
}

func fgacCauseEdges(hidden bool) []*storage.Edge {
	props := map[string]any{"note": "alpha friend"}
	if hidden {
		props["secret"] = "zqxrelsecret alpha alpha"
	}
	edges := []*storage.Edge{
		{ID: "e1", StartNode: "alice", EndNode: "bob", Type: "KNOWS", Properties: props},
		{ID: "e2", StartNode: "bob", EndNode: "carol", Type: "KNOWS", Properties: map[string]any{"note": "bravo friend"}},
	}
	if hidden {
		edges = append(edges, &storage.Edge{ID: "e3", StartNode: "alice", EndNode: "dave", Type: "HATES", Properties: map[string]any{"note": "alpha alpha hate"}})
	}
	return edges
}

func fgacCauseRequest(t *testing.T) auth.FineGrainedRequest {
	t.Helper()
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "db", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "trav", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person", "Doc", "Note"}}},
			{ID: "nosecret", Action: "graph.node.traverse", Deny: true, Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Secret"}}},
			{ID: "person", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Properties: []string{"name", "bio", "dept"}}},
			{ID: "doc", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Doc"}, Properties: []string{"title"}}},
			{ID: "note", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Note"}, Properties: []string{"text", "level"}}},
			{ID: "knows", Action: "graph.relationship.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship, Types: []string{"KNOWS"}}},
			{ID: "knowsprop", Action: "graph.relationship.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship, Types: []string{"KNOWS"}, Properties: []string{"note"}}},
		},
		Assignments: map[string][]string{"reader": {"db", "trav", "nosecret", "person", "doc", "note", "knows", "knowsprop"}},
	})
	require.NoError(t, err)
	return auth.FineGrainedRequest{Access: policy.ForRoles([]string{"reader"}), DatabaseID: func(string) string { return "fgs-id" }, Fingerprint: "cause"}
}

func fgacCauseService(t *testing.T, hidden bool) (*Service, storage.Engine) {
	t.Helper()
	ctx := t.Context()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "fgs")
	for _, node := range fgacCauseNodes(hidden) {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	for _, edge := range fgacCauseEdges(hidden) {
		require.NoError(t, store.CreateEdge(ctx, edge))
	}
	service := NewService(store, 4, nil)
	t.Cleanup(func() { _ = service.Close() })
	service.SetIndexFlags(true, false)
	require.NoError(t, service.BuildIndexes(ctx))
	service.EnableRelationshipSearch(nil)
	service.StartRelationshipIndex()
	if rs := service.relationshipSearch(); rs != nil {
		select {
		case <-rs.loaded.Done():
		case <-time.After(30 * time.Second):
			t.Fatalf("relationship index did not load")
		}
	}
	return service, store
}

func TestFGACSearchCauseDiagnostics(t *testing.T) {
	request := fgacCauseRequest(t)
	for _, state := range []struct {
		name   string
		hidden bool
	}{{"S0", true}, {"S2", false}} {
		t.Run(state.name, func(t *testing.T) {
			ctx := t.Context()
			service, store := fgacCauseService(t, state.hidden)
			scoped := auth.WithFineGrainedRequest(ctx, &request)
			graph := request.Graph(store, "fgs")

			// Corpus statistics of the shared node index (the inputs BM25 uses
			// for the scoped query, since scoring runs on that index).
			index, ok := service.fulltext().(*FulltextIndexV2)
			require.True(t, ok, "fulltext index type %T", service.fulltext())
			index.mu.RLock()
			df, idf := 0, 0.0
			if term := index.termIndex["alpha"]; term != nil {
				df, idf = len(term.Postings)-term.dead, term.IDF
			}
			t.Logf("CAUSE2 [%s] shared node index: docCount=%d avgDocLength=%.4f totalDocLength=%d df(alpha)=%d idf(alpha)=%.6f",
				state.name, index.docCount, index.avgDocLength, index.totalDocLength, df, idf)
			index.mu.RUnlock()

			// What the restricted caller could see: its own authorized corpus.
			visibleDocs, visibleLength, visibleDF := 0, 0, 0
			for _, node := range fgacCauseNodes(state.hidden) {
				projected, err := graph.GetNode(ctx, node.ID)
				if err != nil {
					continue
				}
				text := service.extractSearchableText(projected)
				if text == "" {
					continue
				}
				visibleDocs++
				tokens := service.tokenizeForDiagnostics(text)
				visibleLength += len(tokens)
				for _, token := range tokens {
					if token == "alpha" {
						visibleDF++
						break
					}
				}
			}
			t.Logf("CAUSE2 [%s] authorized corpus of the caller: docCount=%d totalLength=%d df(alpha)=%d", state.name, visibleDocs, visibleLength, visibleDF)

			// The compared strings for every shared hit (search.go adaptiveBM25Search filter).
			documents := map[string]string{}
			service.indexMu.RLock()
			hits := index.searchDocuments("alpha", 50, nil, documents)
			service.indexMu.RUnlock()
			for _, hit := range hits {
				projected, err := graph.GetNode(ctx, storage.NodeID(hit.ID))
				authorized := "<not visible: " + fmt.Sprint(err) + ">"
				if err == nil {
					authorized = service.extractSearchableText(projected)
				}
				t.Logf("CAUSE3 [%s] hit %s score=%.6f admitted=%v\n    indexed   =%q\n    authorized=%q",
					state.name, hit.ID, hit.Score, err == nil && documents[hit.ID] == authorized, documents[hit.ID], authorized)
			}

			// The public outcome for the same caller.
			for _, opts := range []*SearchOptions{{Limit: 10}, {Limit: 10, Types: []string{"Person"}}} {
				response, err := service.Search(scoped, "alpha", nil, opts)
				require.NoError(t, err)
				var rows []string
				for _, result := range response.Results {
					rows = append(rows, fmt.Sprintf("%s:%.6f", result.ID, result.Score))
				}
				t.Logf("CAUSE3 [%s] scoped Search(alpha, types=%v) => %v", state.name, opts.Types, rows)
			}

			// Relationship keyword search (relationship_search.go).
			rs := service.relationshipSearch()
			require.NotNil(t, rs)
			rs.mu.RLock()
			relHits := rs.fulltext.Search("alpha", 50)
			relDocs := map[string]string{}
			for _, hit := range relHits {
				if document, ok := rs.fulltext.GetDocument(hit.ID); ok {
					relDocs[hit.ID] = document
				}
			}
			rs.mu.RUnlock()
			sort.Slice(relHits, func(i, j int) bool { return relHits[i].ID < relHits[j].ID })
			for _, hit := range relHits {
				edge, _, err := auth.ReadRelationshipSearchHit(ctx, graph, storage.EdgeID(hit.ID))
				authorized := "<not visible: " + fmt.Sprint(err) + ">"
				if err == nil && edge != nil {
					authorized = service.relationshipSearchableText(edge)
				}
				t.Logf("CAUSE4 [%s] relationship hit %s score=%.6f admitted=%v\n    indexed   =%q\n    authorized=%q",
					state.name, hit.ID, hit.Score, relDocs[hit.ID] == authorized, relDocs[hit.ID], authorized)
			}
			response, err := service.SearchRelationships(scoped, "alpha", nil, &SearchOptions{Limit: 10})
			require.NoError(t, err)
			var rows []string
			for _, result := range response.Results {
				rows = append(rows, fmt.Sprintf("%s:%.6f", result.ID, result.Score))
			}
			t.Logf("CAUSE4 [%s] scoped SearchRelationships(alpha) => %v", state.name, rows)
		})
	}
}

// tokenizeForDiagnostics splits text the way the index's analyzer does.
func (s *Service) tokenizeForDiagnostics(text string) []string {
	return normalizeAnalyzer(s.bm25Analyzer).Analyze(text)
}
