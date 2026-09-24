package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SET m:Label with an undefined variable m must fail and change nothing, after
// MATCH and after CREATE, like SET m.x = 1 already does (#542).
func TestSetLabelOnUndefinedVariableErrors(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:T)", nil)
			require.NoError(t, err)
			if mode == "explicit transaction" {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			for _, q := range []string{
				"MATCH (n:T) SET m:B RETURN labels(n) AS l",
				"MATCH (n:T) SET m.x = 1 RETURN n.x AS x",
				"CREATE (n:U) SET m:B RETURN labels(n) AS l",
				"MATCH (n:T) SET n:C, m:B RETURN labels(n) AS l",
			} {
				_, err := exec.Execute(ctx, q, nil)
				assert.Error(t, err, q)
			}
			if mode == "explicit transaction" {
				_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			}
			res, err := exec.Execute(ctx, "MATCH (n) RETURN labels(n) AS l", nil)
			require.NoError(t, err)
			assert.Equal(t, [][]interface{}{{[]interface{}{"T"}}}, res.Rows)

			// Defined variables keep working.
			res, err = exec.Execute(ctx, "MATCH (n:T) SET n:B RETURN labels(n) AS l", nil)
			require.NoError(t, err)
			assert.ElementsMatch(t, []interface{}{"T", "B"}, res.Rows[0][0])
			res, err = exec.Execute(ctx, "CREATE (n:U) SET n:B RETURN labels(n) AS l", nil)
			require.NoError(t, err)
			assert.ElementsMatch(t, []interface{}{"U", "B"}, res.Rows[0][0])
		})
	}
}
