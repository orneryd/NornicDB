package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Public results and actual rewrite text are separate observations: top-level
// quoted-variable lowering can replace the client's bindings with internal ones.
func TestNameEmissionMissingConstructions(t *testing.T) {
	ctx := context.Background()
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "missing_constructions")
			e := NewStorageExecutor(store)
			run := func(t *testing.T, text string) *ExecuteResult {
				result, err := e.Execute(ctx, text, nil)
				require.NoError(t, err, text)
				return result
			}
			quoted := cyphertext.QuoteName(name)
			emittedName := cyphertext.NameText(name)
			valueName := cyphertext.QuoteName("value " + name)
			t.Run("map", func(t *testing.T) {
				body := quoted + ": " + valueName + " IN {a: 7} | " + quoted + ": " + valueName
				constructed, ok := mapComprehensionText(body)
				require.True(t, ok)
				require.Contains(t, constructed, emittedName+" IN keys(")
				require.Contains(t, constructed, valueName+" IN [({a: 7})["+emittedName+"]]")
				wantMap := [][]any{{map[string]any{"a": int64(7)}}}
				internal, err := e.executeInternal(ctx, "RETURN "+constructed+" AS result", nil)
				require.NoError(t, err)
				require.Equal(t, wantMap, internal.Rows)
				require.Equal(t, wantMap, run(t, "CYPHER 25 RETURN {"+body+"} AS result").Rows)
			})

			t.Run("group", func(t *testing.T) {
				run(t, "CREATE (:G25 {name:'Ann', age:30}), (:G25 {name:'Ann', age:40})")
				group := "MATCH (n:G25) RETURN n.name AS " + quoted + ", count(*) AS c GROUP BY n.name, n.age"
				rewritten, rewrites, err := desugarCypher25Expressions(group, true)
				require.NoError(t, err)
				require.NotEmpty(t, rewrites)
				require.NotEqual(t, group, rewritten)
				require.Contains(t, rewritten, " AS "+emittedName)
				grouped := run(t, "CYPHER 25 "+group)
				require.Equal(t, []string{name, "c"}, grouped.Columns)
				require.Equal(t, [][]any{{"Ann", int64(1)}, {"Ann", int64(1)}}, grouped.Rows)
			})

			t.Run("predicate", func(t *testing.T) {
				run(t, "CREATE (:Other:"+quoted+" {id:'selected'}), (:Other {id:'unrelated'})")
				predicate := "WITH ['Other'] AS labs MATCH (n:$(labs)&" + quoted + ") RETURN n.id AS id"
				rewritten, _, err := desugarLabelExpressions(predicate, nil, false)
				require.NoError(t, err)
				require.Contains(t, rewritten, "n:"+quoted)
				require.Equal(t, [][]any{{"selected"}}, run(t, predicate).Rows)
			})

			t.Run("alternatives", func(t *testing.T) {
				seenTypes := map[string]bool{}
				for _, typ := range []string{"A", "B", "A|B", name} {
					if seenTypes[typ] {
						continue
					}
					seenTypes[typ] = true
					run(t, "CREATE ()-[:"+cyphertext.QuoteName(typ)+"]->()")
				}
				// A|B is one literal type; A and B are unrelated controls.
				alternatives := "MATCH ()-[r IS `A|B`|" + quoted + "]->() RETURN type(r) AS typ"
				rewritten, _, err := desugarLabelExpressions(alternatives, nil, false)
				require.NoError(t, err)
				if name != "A|B" {
					require.Contains(t, rewritten, ":`A|B`|"+emittedName)
				}
				rows := run(t, alternatives).Rows
				wantTypes := [][]any{{"A|B"}, {name}}
				if name == "A|B" {
					wantTypes = [][]any{{"A|B"}}
				}
				require.ElementsMatch(t, wantTypes, rows)
			})

			t.Run("conditional", func(t *testing.T) {
				conditional := "WHEN true THEN RETURN 7 AS " + quoted + " ELSE RETURN 8 AS " + quoted
				rewritten, _, err := desugarQueryStructure(ctx, conditional, e)
				require.NoError(t, err)
				require.True(t, strings.HasSuffix(rewritten, " RETURN "+quoted), rewritten)
				result := run(t, "CYPHER 25 "+conditional)
				require.Equal(t, []string{name}, result.Columns)
				require.Equal(t, [][]any{{int64(7)}}, result.Rows)
			})

			t.Run("incoming", func(t *testing.T) {
				ddl := "CREATE CONSTRAINT incoming FOR ()<-[r:" + quoted + "]-() REQUIRE MAX COUNT 3"
				run(t, ddl)
				constraints := store.GetSchema().GetAllConstraints()
				require.Len(t, constraints, 1)
				require.Equal(t, name, constraints[0].Label)
				require.Equal(t, "INCOMING", constraints[0].Direction)
				require.Equal(t, 3, constraints[0].MaxCount)
				shown := run(t, "SHOW CONSTRAINTS YIELD name, createStatement RETURN name, createStatement")
				require.Len(t, shown.Rows, 1)
				require.Equal(t, "incoming", shown.Rows[0][0])
				recreation := shown.Rows[0][1].(string)
				require.Equal(t, "CREATE CONSTRAINT `incoming` FOR ()<-[r:"+quoted+"]-() REQUIRE MAX COUNT 3", recreation)
				otherStore := storage.NewNamespacedEngine(newTestMemoryEngine(t), "recreated_incoming")
				_, err := NewStorageExecutor(otherStore).Execute(ctx, recreation, nil)
				require.NoError(t, err)
				require.Equal(t, constraints, otherStore.GetSchema().GetAllConstraints())
			})
		})
	}
}

func TestNameEmissionDecodedSentinels(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			t.Run("references", func(t *testing.T) {
				query := "RETURN " + cyphertext.QuoteName(name)
				require.True(t, isIdentifierReferenced(query, name))
				require.False(t, isIdentifierReferenced("'"+strings.ReplaceAll(name, "'", "''")+"'", name), "a string literal is not an identifier reference")
				require.Equal(t, []string{name}, detectReferencedCallSubquerySeedColumns(&ExecuteResult{Columns: []string{"", name}}, query))
				binding := &ExecuteResult{Columns: []string{name}, Rows: [][]any{{int64(7)}}}
				require.Equal(t, binding, appendCorrelatedBinding(binding, name, int64(8)))
				require.Equal(t, []string{name}, binding.Columns)
				node := &storage.Node{ID: "seed"}
				seeds, err := (&StorageExecutor{}).extractSeedNodesFromResult(&ExecuteResult{Columns: []string{name}, Rows: [][]any{{node}}}, name)
				require.NoError(t, err)
				require.Equal(t, []*storage.Node{node}, seeds)
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "decoded_import"))
				result, err := e.Execute(context.Background(), "WITH 7 AS "+cyphertext.QuoteName(name)+" CALL ("+cyphertext.QuoteName(name)+") { RETURN "+cyphertext.QuoteName(name)+" AS value } RETURN value", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{int64(7)}}, result.Rows)
			})
			t.Run("property-index", func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "decoded_index")
				_, err := store.CreateNode(&storage.Node{ID: "indexed", Labels: []string{name}, Properties: map[string]any{name: int64(7)}})
				require.NoError(t, err)
				schema := store.GetSchema()
				require.NoError(t, schema.AddPropertyIndex("property", name, []string{name}))
				e := NewStorageExecutor(store)
				require.Equal(t, []string{name}, e.indexCandidateLabels(schema, nil, name))
				require.Empty(t, e.indexCandidateLabels(schema, nil, name+" "))
			})
			t.Run("vector-index", func(t *testing.T) {
				schema := storage.NewSchemaManager()
				require.NoError(t, schema.AddVectorIndexForEntity(name, name, name, 3, "cosine", storage.ConstraintEntityNode))
				found, ok := findCosineVectorIndexName(schema, name, name, storage.ConstraintEntityNode)
				require.True(t, ok)
				require.Equal(t, name, found)
				_, ok = findCosineVectorIndexName(schema, name+" ", name, storage.ConstraintEntityNode)
				require.False(t, ok)
				_, ok = findCosineVectorIndexName(schema, name, name+" ", storage.ConstraintEntityNode)
				require.False(t, ok)
			})
		})
	}
}
