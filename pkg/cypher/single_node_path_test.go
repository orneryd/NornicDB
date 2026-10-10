package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A path assigned to a single node without a variable (p = (:L {k: 1})) is
// matched like p = (n:L {k: 1}), wherever the MATCH is; RETURN * lists only
// the variables the statement names (#907).
func TestSingleAnonymousNodePath(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "single_node_path"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:L {k: 1}), (:L {k: 2})", nil)
	require.NoError(t, err)
	l := func(values ...interface{}) []interface{} { return values }
	for query, want := range map[string][][]interface{}{
		"MATCH p = (:L {k: 1}) RETURN length(p) AS l":                                          {l(int64(0))},
		"MATCH p = (:L) RETURN length(p) AS l ORDER BY l":                                      {l(int64(0)), l(int64(0))},
		"MATCH p = () RETURN count(p) AS c":                                                    {l(int64(2))},
		"MATCH p = (:L {k: 1}) RETURN [n IN nodes(p) | n.k] AS ks":                             {l(l(int64(1)))},
		"MATCH (x:L {k: 2}) MATCH p = (:L {k: 1}) RETURN x.k AS x, length(p) AS l":             {l(int64(2), int64(0))},
		"MATCH p = (:L {k: 1}), q = (:L {k: 2}) RETURN [n IN nodes(p) + nodes(q) | n.k] AS ks": {l(l(int64(1), int64(2)))},
		"OPTIONAL MATCH p = (:L {k: 2}) RETURN length(p) AS l":                                 {l(int64(0))},
		"OPTIONAL MATCH p = (:Nope) RETURN p":                                                  {l(nil)},
		"CALL () { MATCH p = (:L {k: 1}) RETURN length(p) AS l } RETURN l":                     {l(int64(0))},
		"MATCH p = (:L {k: 1}) WHERE length(p) = 0 RETURN count(*) AS c":                       {l(int64(1))},
		"MATCH p = (n:L) WHERE size(nodes(p)) = 1 RETURN count(*) AS c":                        {l(int64(2))},
		"MATCH p = (n:L {k: 1}) WHERE length(p) = 0 RETURN n.k AS k":                           {l(int64(1))},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	result, err := exec.Execute(ctx, "MATCH p = (:L {k: 1}) RETURN *", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"p"}, result.Columns)

	rewritten, _, err := desugarLabelExpressions("MATCH p = (:L {k: 1})-[:R]->() RETURN p", nil)
	require.NoError(t, err)
	require.Equal(t, "MATCH p = (:L {k: 1})-[:R]->() RETURN p", rewritten, "a longer path is left as written")
	require.False(t, mayAssignAnonymousNodePath("MATCH (n) WHERE n.k = (1) RETURN n"))
}
