package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/search"
	"testing"

	"github.com/stretchr/testify/require"
)

// Neo4j's MultiFieldQueryParser builds a phrase query for each property,
// never a phrase spanning different property fields. Default relationship
// indexes must follow that rule too, independent of map iteration order.
func TestFulltextRelationshipFieldContract(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		for _, declared := range []bool{false, true} {
			t.Run(fmt.Sprintf("fresh=%t/declared=%t", fresh, declared), func(t *testing.T) {
				exec := NewStorageExecutor(newTestBadgerNamespacedStore(t), QueryCachePolicy{MaxEntries: 0})
				if fresh {
					exec.SetSearchServiceResolver("", func(string) *search.Service { return nil })
				}
				for _, statement := range []string{
					`CREATE ()-[:LINK {name:'split', A:'alpha', B:'beta', hidden:'classified'}]->()`,
					`CREATE ()-[:LINK {B:'noise', A:'alpha beta', name:'whole', hidden:'classified'}]->()`,
				} {
					_, err := exec.Execute(t.Context(), statement, nil)
					require.NoError(t, err)
				}
				index := "default"
				if declared {
					index = "relationship_fields"
					_, err := exec.Execute(t.Context(), `CREATE FULLTEXT INDEX relationship_fields FOR ()-[r:LINK]-() ON EACH [r.A, r.B]`, nil)
					require.NoError(t, err)
				}
				firstRows := make(map[string][][]any)
				query := func(text string) []string {
					t.Helper()
					result, err := exec.Execute(t.Context(), `CALL db.index.fulltext.queryRelationships($index, $query) YIELD relationship, score RETURN relationship.name, score ORDER BY relationship.name`, map[string]any{"index": index, "query": text})
					require.NoError(t, err)
					if first, ok := firstRows[text]; ok {
						require.Equal(t, first, result.Rows, "map traversal must not change matches or scores")
					} else {
						firstRows[text] = result.Rows
					}
					names := make([]string, 0, len(result.Rows))
					for _, row := range result.Rows {
						require.Len(t, row, 2)
						require.Positive(t, row[1])
						name, ok := row[0].(string)
						require.True(t, ok)
						names = append(names, name)
					}
					return names
				}
				for repeat := 0; repeat < 12; repeat++ {
					require.Equal(t, []string{"whole"}, query(`"alpha beta"`), "phrase must stay inside one field")
					require.Equal(t, []string{"split", "whole"}, query("A:alpha"))
					require.Empty(t, query("a:alpha"), "property names are case-sensitive")
					if declared {
						require.Empty(t, query("hidden:classified"), "only declared properties have postings")
					}
				}
			})
		}
	}
}

func TestFulltextNodeFieldInsertionOrder(t *testing.T) {
	queries := []string{"Text:UppercaseZulu", "text:lowercaseBeta", "+Text:UppercaseZulu +text:lowercaseBeta", `"uppercasezulu lowercasebeta"`}
	var expected [][]pr1683Row
	for _, properties := range []string{
		`{name:'mixed', Text:'UppercaseZulu', text:'lowercaseBeta'}`,
		`{text:'lowercaseBeta', Text:'UppercaseZulu', name:'mixed'}`,
	} {
		exec := NewStorageExecutor(newTestBadgerNamespacedStore(t), QueryCachePolicy{MaxEntries: 0})
		_, err := exec.Execute(t.Context(), "CREATE (:Doc "+properties+")", nil)
		require.NoError(t, err)
		_, err = exec.Execute(t.Context(), "CREATE FULLTEXT INDEX field_order FOR (n:Doc) ON EACH [n.Text,n.text]", nil)
		require.NoError(t, err)
		var got [][]pr1683Row
		for _, query := range queries {
			got = append(got, pr1683FulltextRows(t, exec, "field_order", query))
		}
		for _, rows := range got[:3] {
			require.Len(t, rows, 1)
			require.Equal(t, "mixed", rows[0].Name)
		}
		require.Empty(t, got[3], "a phrase cannot cross the two case-distinct properties")
		if expected == nil {
			expected = got
		} else {
			require.Equal(t, expected, got, "property insertion order cannot change matches or score bits")
		}
	}
}
