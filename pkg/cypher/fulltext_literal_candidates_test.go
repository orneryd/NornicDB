package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFulltextLiteralCandidatesPreservePublicDialectAndScores(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, store := fulltextMaintainedFixture(t, 1024)
		node, err := store.GetNode(t.Context(), "n000000")
		require.NoError(t, err)
		node.Properties["text"] = ""
		node.Properties["title"] = "  alpha"
		require.NoError(t, store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
		statements := []string{"hotel", "ote", "ot", "hotel OR missing", "hotel AND services", "hotel -routine", "NOT missing",
			`title:"  alpha"`, `"  alpha"`, `title:Correspondence AND (hotel OR missing)`, `title:Correspondence OR hotel`,
			`(hotel OR missing) AND title:Correspondence`, `hotel^2`, `hotel~1`, `ho*`, `/hotel/`, `title:[A TO Z]`,
			`+hotel -missing`, `title:missing OR hotel`, `title:Correspondence AND -hotel`, `"pines la"`}
		wide := []string{"hotel"}
		for i := 0; i < 24; i++ {
			wide = append(wide, fmt.Sprintf("missing%02d", i))
		}
		statements = append(statements, strings.Join(wide, " OR "), "(hotel OR missingA) AND ((services OR missingB) OR (pines AND (torrey OR missingC)))")
		check := func() {
			t.Helper()
			for _, text := range statements {
				want := fulltextReleaseOracle(t, store, text, []string{"Source"}, []string{"text", "title"})
				result, err := exec.Execute(t.Context(), "CALL db.index.fulltext.queryNodes('source_text',$query) YIELD node,score RETURN node.id,score", map[string]interface{}{"query": text})
				require.NoError(t, err, text)
				require.Equal(t, want, result.Rows, text)
			}
		}
		check()
		node, err = store.GetNode(t.Context(), "n000040")
		require.NoError(t, err)
		node.Properties["text"] = "alpha hotel services"
		require.NoError(t, store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
		check()
		require.NoError(t, store.DeleteNode(t.Context(), "n000000"))
		check()
	})
}

func TestFulltextLiteralCandidatesPreserveRelationshipFieldsAndChanges(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store, exec := fulltextScopeFixture(t)
		for i := 0; i < 256; i++ {
			text := "ordinary text"
			if i == 19 {
				text = "  alpha"
			}
			if i == 23 {
				text = "αβ界é harbor"
			}
			id := fmt.Sprintf("literal-link-%03d", i)
			require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: storage.EdgeID(id), StartNode: "doc-harbor-one", EndNode: "doc-harbor-two", Type: "LINK", Properties: map[string]interface{}{"id": id, "text": text}}))
		}
		queries := []string{`text:"  alpha"`, `"  alpha"`, "alpha OR harbor", "harbor AND -ordinary", "harbor~1", "ha*", "/harbor/", "text:[a TO z]", "é", "αβ界"}
		check := func() {
			t.Helper()
			for _, query := range queries {
				expected := fulltextReleaseOracle(t, fulltextScopeLinkCorpus(t, store), query, []string{"LINK"}, []string{"text"})
				require.Equal(t, expected, fulltextScopeRows(t, exec, "link_scope", query, true), query)
			}
		}
		check()
		_, err := exec.Execute(t.Context(), "MATCH ()-[r:LINK {id:'literal-link-019'}]->() SET r.text='updated harbor'", nil)
		require.NoError(t, err)
		check()
		require.NoError(t, store.DeleteEdge(t.Context(), "literal-link-023"))
		check()
	})
}

func TestFulltextLiteralLoweringUsesNoSmallQueryHeap(t *testing.T) {
	reader := fulltextLiteralReader{}
	for _, query := range []string{"hotel torrey", "(hotel OR torrey) AND (pines OR services)"} {
		parsed, err := ParseFulltextQuery(query)
		require.NoError(t, err)
		var readError error
		children := 0
		allocations := testing.AllocsPerRun(100, func() {
			description, err := reader.Describe(t.Context(), parsed.root)
			readError = err
			children = 0
			for i := 0; i < description.Children; i++ {
				_, included, err := reader.Child(t.Context(), parsed.root, description.Op, i)
				readError = err
				if included {
					children++
				}
			}
		})
		require.NoError(t, readError)
		require.Positive(t, children)
		require.Zero(t, allocations, query)
	}
}
