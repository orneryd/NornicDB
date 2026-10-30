package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nodes_created counts anonymous nodes in multi-CREATE statements, in
// auto-commit and inside an explicit transaction (#546).
func TestMultiCreateCountsAnonymousNodes(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			for _, tc := range []struct {
				q    string
				want int
			}{
				{"CREATE (a:A) CREATE (:B)", 2},
				{"CREATE (a:A) CREATE (b:B)", 2},
				{"CREATE (a:A) CREATE (:B) RETURN a.x AS x", 2},
				{"CREATE (:A) CREATE (:B), (:C)", 3},
				{"CREATE (a:A) CREATE (:B)-[:R]->(a)", 2},
			} {
				res, err := exec.Execute(ctx, tc.q, nil)
				require.NoError(t, err, tc.q)
				require.NotNil(t, res.Stats, tc.q)
				assert.Equal(t, tc.want, res.Stats.NodesCreated, tc.q)
			}
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			res, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
			require.NoError(t, err)
			assert.Equal(t, int64(11), res.Rows[0][0])
		})
	}
}
