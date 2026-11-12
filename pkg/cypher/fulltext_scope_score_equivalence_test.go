package cypher

// NOR413/A2a acceptance coverage: the public fulltext procedures must rank the
// corpus their index declares. TestFulltextPopulationUsesDeclaredLabels already
// proves the acquisition shape and the row counts, but not the exact ranked rows
// and scores. The controls below pin both against the established release
// oracle: for a node label union (including one entity that carries two declared
// labels), for a relationship type scope, and for the legitimate no-label and
// wildcard corpora that keep covering the whole store.
//
// No scoring formula is restated here and no production or storage adapter is
// added: the fixture is the existing namespaced engine with the shared search
// service, and the existing captured-engine adapter carries the relationship
// corpus into the oracle. Each corpus is compared with its own oracle, because
// corpora with different document counts, document frequencies or average
// lengths are not interchangeable.

import (
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// fulltextScopeEntity is one corpus node: the identity property the procedures
// project, the labels it carries, and its scoring body.
type fulltextScopeEntity struct {
	ID     string
	Labels []string
	Text   string
}

// fulltextScopeLink is one corpus relationship; a relationship's type stands in
// for the label a node scope declares.
type fulltextScopeLink struct {
	ID   string
	Type string
	Text string
}

// fulltextScopeIndex is one declared node projection under test.
type fulltextScopeIndex struct {
	name    string
	pattern string
	labels  []string
}

// fulltextScopeCorpus keeps its out-of-scope entities relevant: both Other nodes
// carry the probe term, so a whole-graph population would change the document
// count, the body document frequency and the average length, and could not
// reproduce a declared scope's scores.
var fulltextScopeCorpus = []fulltextScopeEntity{
	{ID: "doc-harbor-one", Labels: []string{"Doc"}, Text: "harbor keeper light"},
	{ID: "doc-harbor-two", Labels: []string{"Doc"}, Text: "harbor harbor tide chart"},
	{ID: "multi-harbor", Labels: []string{"Doc", "Note"}, Text: "harbor harbor harbor signal"},
	{ID: "note-harbor-one", Labels: []string{"Note"}, Text: "harbor notebook scribble"},
	{ID: "other-harbor-one", Labels: []string{"Other"}, Text: "harbor harbor log"},
	{ID: "other-harbor-two", Labels: []string{"Other"}, Text: "harbor beacon registry"},
}

var fulltextScopeLinks = []fulltextScopeLink{
	{ID: "link-harbor-one", Type: "LINK", Text: "harbor relay"},
	{ID: "link-harbor-two", Type: "LINK", Text: "harbor harbor harbor dispatch"},
	{ID: "link-harbor-other", Type: "OTHER", Text: "harbor harbor exclusion"},
}

var fulltextScopeIndexes = []fulltextScopeIndex{
	{name: "doc_scope", pattern: "n:Doc", labels: []string{"Doc"}},
	{name: "note_scope", pattern: "n:Note", labels: []string{"Note"}},
	{name: "doc_or_note_scope", pattern: "n:Doc|Note", labels: []string{"Doc", "Note"}},
	{name: "note_or_doc_scope", pattern: "n:Note|Doc", labels: []string{"Note", "Doc"}},
	{name: "whole_scope", pattern: "n:Doc|Note|Other", labels: []string{"Doc", "Note", "Other"}},
}

// fulltextScopeFixture seeds the corpus in one namespace, declares every
// projection under test, and attaches the shared search service the procedures
// read. The query-result cache stays disabled so every call executes.
func fulltextScopeFixture(t *testing.T) (storage.Engine, *StorageExecutor) {
	t.Helper()
	store := newTestBadgerNamespacedStore(t)
	for _, entity := range fulltextScopeCorpus {
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:         storage.NodeID(entity.ID),
			Labels:     entity.Labels,
			Properties: map[string]any{"id": entity.ID, "text": entity.Text},
		})
		require.NoError(t, err, "seed %s", entity.ID)
	}
	for _, link := range fulltextScopeLinks {
		require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{
			ID:         storage.EdgeID(link.ID),
			StartNode:  storage.NodeID("doc-harbor-one"),
			EndNode:    storage.NodeID("doc-harbor-two"),
			Type:       link.Type,
			Properties: map[string]any{"id": link.ID, "text": link.Text},
		}), "seed %s", link.ID)
	}
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	service := search.NewService(store, search.DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	exec.SetSearchService(service)
	for _, definition := range fulltextScopeIndexes {
		_, err := exec.Execute(t.Context(), "CREATE FULLTEXT INDEX "+definition.name+" FOR ("+definition.pattern+") ON EACH [n.text]", nil)
		require.NoError(t, err, "declare %s over %s", definition.name, definition.pattern)
	}
	_, err := exec.Execute(t.Context(), "CREATE FULLTEXT INDEX link_scope FOR ()-[r:LINK]-() ON EACH [r.text]", nil)
	require.NoError(t, err, "declare the relationship scope")
	return store, exec
}

// fulltextScopeRows runs the public node procedure and returns its projected
// rows in the procedure's ranked order.
func fulltextScopeRows(t *testing.T, exec *StorageExecutor, index, query string, relationships bool) [][]any {
	t.Helper()
	entity, procedure := "node", "queryNodes"
	if relationships {
		entity, procedure = "relationship", "queryRelationships"
	}
	statement := fmt.Sprintf("CALL db.index.fulltext.%s($index, $query) YIELD %s, score RETURN %s.id, score", procedure, entity, entity)
	result, err := exec.Execute(t.Context(), statement,
		map[string]any{"index": index, "query": query})
	require.NoError(t, err, "public node fulltext query %q on index %q", query, index)
	return result.Rows
}

// fulltextScopeIDs returns the projected identities in sorted order, so a
// membership control never depends on tie order.
func fulltextScopeIDs(t *testing.T, rows [][]any) []string {
	t.Helper()
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		require.Len(t, row, 2, "the control projects an identity and a score")
		id, ok := row[0].(string)
		require.True(t, ok, "the identity projects a string, got %T", row[0])
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// fulltextScopeScore returns the score the procedure reported for one identity.
func fulltextScopeScore(t *testing.T, rows [][]any, id string) float64 {
	t.Helper()
	for _, row := range rows {
		if row[0] == id {
			score, ok := row[1].(float64)
			require.True(t, ok, "the score projects a float64, got %T", row[1])
			return score
		}
	}
	t.Fatalf("%s is missing from the ranked rows %v", id, rows)
	return 0
}

// fulltextScopeLinkCorpus adapts the stored relationships to the field kernel's
// entity images for the oracle, exactly as the existing population controls do,
// so relationship scoring is compared with the same release sequence.
func fulltextScopeLinkCorpus(t *testing.T, store storage.Engine) storage.Engine {
	t.Helper()
	edges, err := store.AllEdges(t.Context())
	require.NoError(t, err)
	rows := make([]*storage.Node, 0, len(edges))
	for _, edge := range edges {
		rows = append(rows, &storage.Node{ID: storage.NodeID(edge.ID), Labels: []string{edge.Type}, Properties: edge.Properties})
	}
	return &fulltextCapturedEngine{Engine: store, rows: rows}
}

func TestFulltextScopeScoresMatchDeclaredCorpus(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store, exec := fulltextScopeFixture(t)

		t.Run("declared_scope_ranks_the_declared_corpus", func(t *testing.T) {
			for _, definition := range fulltextScopeIndexes {
				want := fulltextReleaseOracle(t, store, "harbor", definition.labels, []string{"text"})
				require.NotEmpty(t, want, "control: %s selects entities that match the probe term", definition.name)
				got := fulltextScopeRows(t, exec, definition.name, "harbor", false)
				require.Equal(t, want, got,
					"%s must return the release reader's rows, order and score bits for its declared label scope", definition.name)
				require.Equal(t, got, fulltextScopeRows(t, exec, definition.name, "harbor", false),
					"%s re-ranks an unchanged corpus identically", definition.name)
			}
		})

		t.Run("same_label_filtered_corpus_scores_identically", func(t *testing.T) {
			docNote := fulltextScopeRows(t, exec, "doc_or_note_scope", "harbor", false)
			noteDoc := fulltextScopeRows(t, exec, "note_or_doc_scope", "harbor", false)
			require.Len(t, docNote, 4, "control: Doc and Note select four entities")
			require.Equal(t, docNote, noteDoc,
				"the same label-filtered corpus must score identically in either declaration order")
		})

		t.Run("multi_label_entity_is_one_document", func(t *testing.T) {
			counted := map[string]int{}
			for _, id := range fulltextScopeIDs(t, fulltextScopeRows(t, exec, "doc_or_note_scope", "harbor", false)) {
				counted[id]++
			}
			require.Equal(t, map[string]int{
				"doc-harbor-one":  1,
				"doc-harbor-two":  1,
				"multi-harbor":    1,
				"note-harbor-one": 1,
			}, counted, "an entity carrying both declared labels is one document, and an undeclared label contributes none")
		})

		t.Run("different_corpora_are_not_equated", func(t *testing.T) {
			docRows := fulltextScopeRows(t, exec, "doc_scope", "harbor", false)
			docNoteRows := fulltextScopeRows(t, exec, "doc_or_note_scope", "harbor", false)
			wholeRows := fulltextScopeRows(t, exec, "whole_scope", "harbor", false)
			require.Len(t, docRows, 3, "control: Doc selects three entities")
			require.Len(t, docNoteRows, 4, "control: Doc and Note select four entities")
			require.Len(t, wholeRows, 6, "control: every declared label selects all six entities")
			// Every corpus carries its own document count, document frequencies and
			// average length, so a document shared between two scopes must not keep
			// one scope's statistics in the other.
			require.NotEqual(t, fulltextScopeScore(t, docRows, "multi-harbor"), fulltextScopeScore(t, docNoteRows, "multi-harbor"),
				"a wider scope with another matching document must not reuse the narrower scope's score")
			require.NotEqual(t, fulltextScopeScore(t, docNoteRows, "multi-harbor"), fulltextScopeScore(t, wholeRows, "multi-harbor"),
				"the widest scope must not reuse the narrower scope's score")
		})

		t.Run("wildcard_and_undeclared_corpora", func(t *testing.T) {
			scopedWildcard := fulltextScopeRows(t, exec, "doc_scope", "*", false)
			require.Equal(t, []string{"doc-harbor-one", "doc-harbor-two", "multi-harbor"}, fulltextScopeIDs(t, scopedWildcard),
				"a wildcard stays inside the declared labels")
			for _, row := range scopedWildcard {
				require.Equal(t, float64(1), row[1], "unranked wildcard rows carry score 1")
			}
			wholeWildcard := fulltextScopeIDs(t, fulltextScopeRows(t, exec, "whole_scope", "*", false))
			require.Equal(t, []string{
				"doc-harbor-one", "doc-harbor-two", "multi-harbor",
				"note-harbor-one", "other-harbor-one", "other-harbor-two",
			}, wholeWildcard, "the widest declared union covers every entity once")
			require.Equal(t, wholeWildcard, fulltextScopeIDs(t, fulltextScopeRows(t, exec, "default", "*", false)),
				"an index that declares no label keeps the whole corpus")
			// The built-in default searches its fixed documented field list,
			// which excludes id. Of those fields this fixture supplies only
			// text, so the widest declared text index has the same corpus.
			wantDefault := fulltextReleaseOracle(t, store, "harbor", nil, []string{"text"})
			require.NotEmpty(t, wantDefault)
			require.Equal(t, wantDefault, fulltextScopeRows(t, exec, "default", "harbor", false),
				"the implicit corpus retains its documented searchable fields and all-label statistics")
			require.Equal(t, fulltextScopeRows(t, exec, "whole_scope", "harbor", false), fulltextScopeRows(t, exec, "default", "harbor", false), "equivalent text corpora preserve exact score bits")
		})

		t.Run("relationship_scope_ranks_the_declared_types", func(t *testing.T) {
			want := fulltextReleaseOracle(t, fulltextScopeLinkCorpus(t, store), "harbor", []string{"LINK"}, []string{"text"})
			require.Len(t, want, 2, "control: two LINK relationships match the probe term")
			got := fulltextScopeRows(t, exec, "link_scope", "harbor", true)
			require.Equal(t, want, got,
				"queryRelationships must return the release reader's rows, order and score bits for its declared types")
			require.NotContains(t, fulltextScopeIDs(t, got), "link-harbor-other",
				"an undeclared relationship type contributes no document")
		})
	})
}
