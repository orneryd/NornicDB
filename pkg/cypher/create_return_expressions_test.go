package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every RETURN item after a plain CREATE is evaluated, not only items that
// reference a created variable (#551).
func TestCreateReturnEvaluatesEveryItem(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			params := map[string]interface{}{"a": int64(7)}
			for _, tc := range []struct {
				q    string
				want [][]interface{}
			}{
				{"CREATE (:P {v: 1}) RETURN 1 AS ok", [][]interface{}{{int64(1)}}},
				{"CREATE (p:P {v: 1}) RETURN 1 AS ok", [][]interface{}{{int64(1)}}},
				{"CREATE (p:P {v: 1}) RETURN p.v AS v, 1 AS ok", [][]interface{}{{int64(1), int64(1)}}},
				{"CREATE (p:P {v: 1}) RETURN 'x' AS s, 2 + 3 AS n, $a AS a", [][]interface{}{{"x", int64(5), int64(7)}}},
				{"CREATE (p:P {v: 1})-[:R]->(:Q) RETURN 1 AS ok", [][]interface{}{{int64(1)}}},
				{"CREATE (p:P {v: 2}) RETURN p.v + 1 AS v1, toString(p.v) AS s", [][]interface{}{{int64(3), "2"}}},
				{"CREATE (ab:P {v: 3}), (a:P {v: 4}) RETURN ab.v AS x, a.v AS y", [][]interface{}{{int64(3), int64(4)}}},
			} {
				res, err := exec.Execute(ctx, tc.q, params)
				require.NoError(t, err, tc.q)
				assert.Equal(t, tc.want, res.Rows, tc.q)
			}
		})
	}
}
