package cypher

import (
	"context"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAdmittedParameterCachePublic(t *testing.T) {
	for _, prefix := range []string{"", "USE comp.one "} {
		exec := newFabricCallRowsExecutor(t)
		ctx := context.Background()
		query := prefix + "WITH $p AS x UNWIND [] AS stop CALL (x) { WITH 2 AS y RETURN x.a AS v } RETURN v"
		for _, value := range []interface{}{map[string]interface{}{"a": int64(1)}, int64(1), map[string]interface{}{"a": int64(2)}, true} {
			result, err := exec.Execute(ctx, query, map[string]interface{}{"p": value})
			if _, valid := value.(map[string]interface{}); valid {
				require.NoError(t, err)
				require.Empty(t, result.Rows)
			} else {
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			}
		}
	}
}
