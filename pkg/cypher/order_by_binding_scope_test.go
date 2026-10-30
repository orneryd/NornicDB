package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const orderByBindingScopeSeed = `
CREATE (r:Repository {id:'repo-x', name:'x'})-[:REPO_CONTAINS]->(f:File {relative_path:'a.go'}),
       (f)-[:CONTAINS]->(:Function {id:'fn-09', name:'Worker',        cyclomatic_complexity:7}),
       (f)-[:CONTAINS]->(:Function {id:'fn-03', name:'Apply',         cyclomatic_complexity:7}),
       (f)-[:CONTAINS]->(:Function {id:'fn-11', name:'start',         cyclomatic_complexity:4}),
       (f)-[:CONTAINS]->(:Function {id:'fn-01', name:'Divide',        cyclomatic_complexity:7}),
       (f)-[:CONTAINS]->(:Function {id:'fn-07', name:'Error',         cyclomatic_complexity:4}),
       (f)-[:CONTAINS]->(:Function {id:'fn-12', name:'Map',           cyclomatic_complexity:4}),
       (f)-[:CONTAINS]->(:Function {id:'fn-05', name:'calculate_sum', cyclomatic_complexity:2}),
       (f)-[:CONTAINS]->(:Function {id:'fn-02', name:'Min',           cyclomatic_complexity:9}),
       (f)-[:CONTAINS]->(:Function {id:'fn-10', name:'get_first',     cyclomatic_complexity:2}),
       (f)-[:CONTAINS]->(:Function {id:'fn-04', name:'Pop',           cyclomatic_complexity:4}),
       (f)-[:CONTAINS]->(:Function {id:'fn-08', name:'show',          cyclomatic_complexity:1}),
       (f)-[:CONTAINS]->(:Function {id:'fn-06', name:'Error',         cyclomatic_complexity:7})
`

func orderByBindingScopeIDs(t *testing.T, result *ExecuteResult) []string {
	t.Helper()
	ids := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.NotEmpty(t, row)
		switch value := row[0].(type) {
		case string:
			ids = append(ids, value)
		case *storage.Node:
			id, ok := value.Properties["id"].(string)
			require.True(t, ok)
			ids = append(ids, id)
		default:
			t.Fatalf("unexpected identifier projection %T", value)
		}
	}
	return ids
}

func TestOrderByEvaluatesKeysAgainstPreProjectionBindings(t *testing.T) {
	nameThenID := []string{"fn-03", "fn-01", "fn-06", "fn-07", "fn-12", "fn-02", "fn-04", "fn-09", "fn-05", "fn-10", "fn-08", "fn-11"}
	complexityThenID := []string{"fn-02", "fn-01", "fn-03", "fn-06", "fn-09", "fn-04", "fn-07", "fn-11", "fn-12", "fn-05", "fn-10", "fn-08"}
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "relationship pattern hidden properties",
			query: "MATCH (f:File)-[:CONTAINS]->(e:Function) RETURN e.id AS id ORDER BY e.name, e.id",
			want:  nameThenID,
		},
		{
			name:  "optional match hidden properties",
			query: "MATCH (e:Function) OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File) RETURN e.id AS id ORDER BY e.name, e.id",
			want:  nameThenID,
		},
		{
			name:  "optional match raw projected expression",
			query: "MATCH (e:Function) OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File) RETURN e.id AS id, e.name AS name ORDER BY e.name, e.id",
			want:  nameThenID,
		},
		{
			name:  "optional match function and hidden tie breakers",
			query: "MATCH (e:Function) OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File) RETURN e.id AS id, coalesce(e.cyclomatic_complexity, 0) AS complexity ORDER BY coalesce(e.cyclomatic_complexity, 0) DESC, e.id LIMIT 5",
			want:  complexityThenID[:5],
		},
		{
			name:  "optional match alias and hidden tie breakers",
			query: "MATCH (e:Function) OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File) RETURN e.id AS id, e.name AS name, coalesce(e.cyclomatic_complexity, 0) AS complexity ORDER BY complexity DESC, e.name, e.id LIMIT 5",
			want:  []string{"fn-02", "fn-03", "fn-01", "fn-06", "fn-09"},
		},
		{
			name:  "bare match function key",
			query: "MATCH (e:Function) RETURN e.id AS id ORDER BY coalesce(e.cyclomatic_complexity, 0) DESC, e.id",
			want:  complexityThenID,
		},
		{
			name:  "returned node property id",
			query: "MATCH (f:File)-[:CONTAINS]->(e:Function) RETURN e ORDER BY e.id",
			want:  []string{"fn-01", "fn-02", "fn-03", "fn-04", "fn-05", "fn-06", "fn-07", "fn-08", "fn-09", "fn-10", "fn-11", "fn-12"},
		},
	}

	for _, mode := range []struct {
		name     string
		explicit bool
	}{
		{name: "autocommit"},
		{name: "explicit transaction", explicit: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "order_by_binding_scope"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, orderByBindingScopeSeed, nil)
			require.NoError(t, err)
			if mode.explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, test.query, nil)
					require.NoError(t, err)
					require.Equal(t, test.want, orderByBindingScopeIDs(t, result))
				})
			}
		})
	}
}
