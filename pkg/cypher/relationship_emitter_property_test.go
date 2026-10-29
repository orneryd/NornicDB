package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	apocexport "github.com/orneryd/nornicdb/apoc/export"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipEmitterReviewExact(t *testing.T) {
	text := apocexport.Cypher(nil, []*apocexport.Relationship{{StartNode: 1, EndNode: 2, Type: "A|B"}})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "emitter_exact"))
	_, err := exec.Execute(context.Background(), text, nil)
	require.NoError(t, err, text)
	require.Contains(t, text, "[r:`A|B`]")
	name, err := cyphernames.ReadType(text)
	require.NoError(t, err)
	require.Equal(t, "A|B", name)
}

func TestRelationshipEmitterProperty(t *testing.T) {
	t.Logf("generated literal names: %d", len(cyphernames.Names()))
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "emitter_property"))
			ctx := context.Background()
			quoted := cyphertext.QuoteName(name)
			_, err := exec.Execute(ctx, "CREATE (:"+quoted+")-[:"+quoted+"]->()", nil)
			require.NoError(t, err)
			rels := []*apocexport.Relationship{{StartNode: 1, EndNode: 2, Type: name, Properties: map[string]any{name: 1}}}
			toString, err := apocexport.ToString(nil, rels, "cypher")
			require.NoError(t, err)
			edge := &storage.Edge{Type: name, StartNode: "a", EndNode: "b"}
			valueText, ok := cypher25ValueText(edge)
			require.True(t, ok)
			pathText, ok := cypher25ValueText(&PathResult{Nodes: []*storage.Node{{ID: "a", Labels: []string{name}}, {ID: "b"}}, Relationships: []*storage.Edge{edge}})
			require.True(t, ok)
			rewritten, _, err := desugarLabelExpressions("MATCH ()-[r IS "+quoted+"]->() RETURN r", nil, false)
			require.NoError(t, err)
			texts := map[string]string{
				"go-cypher":          apocexport.Cypher(nil, rels),
				"go-toString":        toString,
				"show-index":         showIndexCreateStatement("RANGE", "idx", "RELATIONSHIP", []string{name}, []string{name}, nil),
				"show-fulltext":      showIndexCreateStatement("FULLTEXT", "ft", "RELATIONSHIP", []string{name}, []string{name}, nil),
				"show-vector":        showIndexCreateStatement("VECTOR", "vec", "RELATIONSHIP", []string{name}, []string{name}, nil),
				"show-property-type": showPropertyTypeCreateStatement("pt", storage.ConstraintEntityRelationship, name, name, "INTEGER"),
				"value":              valueText,
				"path":               pathText,
				"label-chain":        "()-[r" + labelChainText([]string{name}) + "]->()",
				"label-rewrite":      rewritten,
			}
			for _, kind := range []storage.ConstraintType{storage.ConstraintUnique, storage.ConstraintExists, storage.ConstraintRelationshipKey, storage.ConstraintTemporal, storage.ConstraintDomain, storage.ConstraintCardinality, storage.ConstraintPolicy} {
				c := storage.Constraint{Name: "c", Type: kind, EntityType: storage.ConstraintEntityRelationship, Label: name, Properties: []string{name}, MaxCount: 2, PolicyMode: "ALLOWED", SourceLabel: name, TargetLabel: name}
				if kind == storage.ConstraintTemporal {
					c.Properties = []string{name, "valid`from", "valid`to"}
				}
				if kind == storage.ConstraintDomain {
					c.AllowedValues = []any{int64(1)}
				}
				texts["show-constraint-"+string(kind)] = showConstraintCreateStatement(c).(string)
			}
			for route, text := range texts {
				t.Run(route, func(t *testing.T) {
					properties := map[string]any{}
					if route == "go-cypher" || route == "go-toString" {
						properties[name] = int64(1)
					}
					checkEmitterRoundTrip(t, text, name, nil, properties)
					if strings.HasPrefix(route, "show-") {
						keys := []string{name}
						if strings.HasSuffix(route, "TEMPORAL_NO_OVERLAP") {
							keys = []string{name, "valid`from", "valid`to"}
						}
						if strings.HasSuffix(route, "CARDINALITY") || strings.HasSuffix(route, "POLICY") {
							keys = nil
						}
						checkEmitterSchemaRoundTrip(t, text, name, keys)
					}
					got, err := cyphernames.ReadType(text)
					require.NoError(t, err, text)
					require.Equal(t, name, got, text)
					open := strings.IndexByte(text, '[')
					require.GreaterOrEqual(t, open, 0)
					close := cyphertext.FindMatchingDelimiter(text, open, '[', ']', cyphertext.OpeningDelimiter)
					require.Greater(t, close, open)
					declaration := relationshipDeclarationOf(text[open+1 : close])
					require.Equal(t, []string{name}, declaration.types.storageNames(), text)

				})
			}
			require.Equal(t, texts["go-cypher"], toString)
			// Re-import the actual public Go export's CREATE pattern, including
			// its emitted property identifier, then inspect stored data.
			text := texts["go-cypher"]
			open, close := readRelationshipBracket(text, 0, requireClosedRelationship)
			require.GreaterOrEqual(t, open, 0, "exported relationship bracket: %s", text)
			require.Greater(t, close, open, "exported relationship bracket: %s", text)
			result, err := exec.Execute(ctx, "CREATE ()-"+text[open:close+1]+"->() RETURN type(r), r", nil)
			require.NoError(t, err, text)
			require.Equal(t, name, result.Rows[0][0])
			require.EqualValues(t, 1, result.Rows[0][1].(*storage.Edge).Properties[name])
			// Labels and keys share the public export path.
			nodeText := apocexport.Cypher([]*apocexport.Node{{ID: 9, Labels: []string{name}, Properties: map[string]any{name: 1}}}, nil)
			result, err = exec.Execute(ctx, strings.TrimSuffix(nodeText, ";\n")+" RETURN n9", nil)
			require.NoError(t, err)
			node := result.Rows[0][0].(*storage.Node)
			require.Equal(t, []string{name}, node.Labels)
			require.EqualValues(t, 1, node.Properties[name])
			// Dynamic row and constant lowering both re-render relationship names.
			for _, query := range []string{"WITH $type AS t CREATE ()-[r:$(t)]->() RETURN type(r)", "CREATE ()-[r:$($type)]->() RETURN type(r)"} {
				result, err := exec.Execute(ctx, query, map[string]any{"type": name})
				require.NoError(t, err, query)
				require.Equal(t, [][]any{{name}}, result.Rows)
			}
			for i, form := range cyphernames.DeclarationForms {
				t.Run(fmt.Sprintf("dynamic-form-%d", i), func(t *testing.T) {
					for _, input := range []string{"WITH $type AS t CREATE ()-[r" + form + "$(t) {" + quoted + ":1}]->() RETURN r", "CREATE ()-[r" + form + "$($type) {" + quoted + ":1}]->() RETURN r"} {
						result, err := exec.Execute(ctx, input, map[string]any{"type": name})
						require.NoError(t, err, input)
						require.Len(t, result.Rows, 1)
						edge := result.Rows[0][0].(*storage.Edge)
						require.Equal(t, name, edge.Type)
						require.EqualValues(t, 1, edge.Properties[name])
					}
				})
			}
			// EXPLAIN/PROFILE retain source instead of reconstructing a type.
			query := "MATCH ()-[r:" + quoted + "]->() RETURN type(r)"
			plan, err := exec.buildExecutionPlan(query)
			require.NoError(t, err)
			got, err := cyphernames.ReadType(plan.Query)
			require.NoError(t, err)
			require.Equal(t, name, got)
			labelPlan := exec.analyzeNodeScan("MATCH (n:" + quoted + ") RETURN n")
			start := strings.IndexByte(labelPlan.Description, ':') + 1
			require.Positive(t, start)
			token, ok := cyphertext.ScanIdentifier(labelPlan.Description, start)
			label := token.Value
			require.True(t, ok)
			require.Equal(t, name, label)
			for _, mode := range []string{"EXPLAIN", "PROFILE"} {
				result, err := exec.Execute(ctx, mode+" "+query, nil)
				require.NoError(t, err)
				require.NotEmpty(t, result.Metadata)
			}
			for i, form := range cyphernames.DeclarationForms {
				t.Run(fmt.Sprintf("fabric-form-%d", i), func(t *testing.T) {
					input := "MATCH ()-[r" + form + quoted + "]->() RETURN r"
					fragment, err := fabric.NewFabricPlanner(fabric.NewCatalog()).Plan(t.Context(), input, "test", fabric.RawSource)
					require.NoError(t, err)
					checkEmitterRoundTrip(t, fragment.(*fabric.FragmentExec).Query, name, nil, nil)
				})
				t.Run(fmt.Sprintf("rewrite-form-%d", i), func(t *testing.T) {
					input := "MATCH ()-[r" + form + quoted + "]->() RETURN r"
					output, _, err := desugarLabelExpressions(input, nil, false)
					require.NoError(t, err)
					checkEmitterRoundTrip(t, output, name, nil, nil)
				})
			}
			// Fabric fragments likewise preserve already quoted pattern text.
			fragment, err := fabric.NewFabricPlanner(fabric.NewCatalog()).Plan(t.Context(), query, "test", fabric.RawSource)
			require.NoError(t, err)
			fragmentText := fragment.(*fabric.FragmentExec).Query
			got, err = cyphernames.ReadType(fragmentText)
			require.NoError(t, err)
			require.Equal(t, name, got)
			result, err = exec.Execute(ctx, fragmentText, nil)
			require.NoError(t, err)
			for _, row := range result.Rows {
				require.Equal(t, name, row[0])
			}
			require.NotEmpty(t, result.Rows)
		})
	}
}

func TestRelationshipEmitterPropertyRejectsUnquoted(t *testing.T) {
	rejected := 0
	for _, name := range cyphernames.Names() {
		got, err := cyphernames.ReadType(fmt.Sprintf("[:%s]", name))
		if err != nil || got != name {
			rejected++
		}
	}
	require.Greater(t, rejected, 0, "planted unquoted emitter escaped the property")
	_, err := cyphernames.ReadType("[:A|B]")
	require.Error(t, err, "the exact review mutant must be rejected")
}

func TestRelationshipEmitterHintProperty(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			for _, kind := range []IndexHintType{HintIndex, HintScan, HintJoin} {
				for _, properties := range [][]string{{name}, {name, "second`key"}} {
					hint := IndexHint{Type: kind, Variable: name, Label: name, Property: name, Properties: properties}
					text := hint.String()
					hints, _ := ParseIndexHints("MATCH (n) " + text + " RETURN n")
					require.Len(t, hints, 1, text)
					require.Equal(t, name, hints[0].Variable, text)
					if kind != HintJoin {
						require.Equal(t, name, hints[0].Label, text)
					}
					if kind == HintIndex {
						require.Equal(t, properties, hints[0].Properties, text)
					}
				}
			}
		})
	}
}

func emitterObservations(executor *StorageExecutor) cyphernames.Execute {
	return func(query string, params map[string]any) ([]cyphernames.StoredRelationship, error) {
		result, err := executor.Execute(context.Background(), query, params)
		if err != nil {
			return nil, err
		}
		var rows []cyphernames.StoredRelationship
		for _, row := range result.Rows {
			edge, ok := row[0].(*storage.Edge)
			if !ok {
				return nil, fmt.Errorf("expected relationship, got %T", row[0])
			}
			rows = append(rows, cyphernames.StoredRelationship{Type: edge.Type, Properties: edge.Properties})
		}
		return rows, nil
	}
}
func checkEmitterRoundTrip(t *testing.T, text, name string, params, properties map[string]any) {
	t.Helper()
	bracket, err := cyphernames.RelationshipBracket(text)
	require.NoError(t, err)
	for i, form := range cyphernames.DeclarationForms {
		t.Run(fmt.Sprintf("form-%d", i), func(t *testing.T) {
			variant, err := cyphernames.WithDeclarationForm(bracket, form)
			require.NoError(t, err)
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "emitter_roundtrip"))
			require.NoError(t, cyphernames.RoundTrip(variant, name, params, properties, emitterObservations(executor)))
		})
	}
}

func checkEmitterSchemaRoundTrip(t *testing.T, text, name string, keys []string) {
	t.Helper()
	bracket, err := cyphernames.RelationshipBracket(text)
	require.NoError(t, err)
	for i, form := range cyphernames.DeclarationForms {
		t.Run(fmt.Sprintf("schema-form-%d", i), func(t *testing.T) {
			variant, err := cyphernames.WithDeclarationForm(bracket, form)
			require.NoError(t, err)
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "emitter_schema"))
			require.NoError(t, cyphernames.SchemaRoundTrip(strings.Replace(text, bracket, variant, 1), name, keys, func(query string) ([][]any, error) {
				result, err := executor.Execute(context.Background(), query, nil)
				if err != nil {
					return nil, err
				}
				return result.Rows, nil
			}))
		})
	}
}
