package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRuntimeMapBatchContract(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "batch_contract"))
	ctx := context.Background()
	for _, input := range []struct {
		name, source string
		row          interface{}
	}{
		{"null", "row", nil},
		{"typed-nil", "row", map[string]interface{}(nil)},
		{"nested-nil", "row.props", map[string]interface{}{"props": contractNamedMap(nil)}},
		{"json-null", "apoc.convert.fromJsonMap(row.text)", map[string]interface{}{"text": "null"}},
		{"json-error", "apoc.convert.fromJsonMap(row.text)", map[string]interface{}{"text": "1"}},
		{"empty", "row", map[string]interface{}{}},
	} {
		for _, consumer := range []struct{ name, query string }{
			{"node=", "MATCH (n:Contract {id:1}) SET n = " + input.source},
			{"node+=", "MATCH (n:Contract {id:1}) SET n += " + input.source},
			{"rel=", "MATCH (n:Contract)-[r:CONTRACT]->() SET r = " + input.source},
			{"rel+=", "MATCH (n:Contract)-[r:CONTRACT]->() SET r += " + input.source},
			{"merge-match", "MERGE (n:Contract {id:1}) ON MATCH SET n = " + input.source},
			{"merge-create", "MERGE (n:Made {id:9}) ON CREATE SET n = " + input.source},
			{"create-field", "CREATE (:Made {value:" + input.source + "})"},
		} {
			t.Run(input.name+"/"+consumer.name, func(t *testing.T) {
				_, err := e.Execute(ctx, "MATCH (n) DETACH DELETE n", nil)
				require.NoError(t, err)
				_, err = e.Execute(ctx, "CREATE (:Contract {id:1,keep:1})-[:CONTRACT {keep:2}]->(:Other)", nil)
				require.NoError(t, err)
				_, err = e.Execute(ctx, "UNWIND $rows AS row "+consumer.query, map[string]interface{}{"rows": []interface{}{input.row}})
				success := input.name == "empty" && consumer.name != "create-field" || input.name != "empty" && input.name != "json-error" && consumer.name == "create-field"
				if success {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				state, err := e.Execute(ctx, "MATCH (n:Contract)-[r:CONTRACT]->() RETURN properties(n),properties(r)", nil)
				require.NoError(t, err)
				node := map[string]interface{}{"id": int64(1), "keep": int64(1)}
				rel := map[string]interface{}{"keep": int64(2)}
				if success && input.name == "empty" {
					switch consumer.name {
					case "node=", "merge-match":
						node = map[string]interface{}{}
					case "rel=":
						rel = map[string]interface{}{}
					}
				}
				require.Equal(t, [][]interface{}{{node, rel}}, state.Rows)
				made, err := e.Execute(ctx, "MATCH (n:Made) RETURN properties(n)", nil)
				require.NoError(t, err)
				if success && (consumer.name == "create-field" || consumer.name == "merge-create") {
					require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, made.Rows)
				} else {
					require.Empty(t, made.Rows)
				}
			})
		}
	}
	// Optional entities still produce NULL, including through properties/projection.
	for _, expr := range []string{"o", "s", "properties(o)", "o{.*}"} {
		for _, op := range []string{"=", "+="} {
			_, err := e.Execute(ctx, "MATCH (n:Contract) OPTIONAL MATCH (o:Absent)-[s:ABSENT]->() SET n "+op+" "+expr, nil)
			require.Error(t, err)
		}
	}
	// Existing unsupported whole-map MERGE/procedure forms stay unsupported.
	for _, query := range []string{"MERGE (n:Made $p)", "CALL apoc.create.setProperties([],[],[])"} {
		_, err := e.Execute(ctx, query, map[string]interface{}{"p": map[string]interface{}{}})
		require.Error(t, err)
		if strings.HasPrefix(query, "CALL") {
			require.Contains(t, err.Error(), "ProcedureNotFound")
		}
	}
}
