package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// x IN $p with a non-list, non-null parameter is a type error, in WHERE as in
// RETURN, instead of silently matching nothing (#537).
func TestInNonListParameterIsTypeError(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:T {id: 5})", nil)
			require.NoError(t, err)
			if mode == "explicit transaction" {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			for _, tc := range []struct {
				q string
				p interface{}
			}{
				{"MATCH (n:T) WHERE n.id IN $p RETURN count(n) AS c", int64(5)},
				{"MATCH (n:T) WHERE n.id IN $p RETURN count(n) AS c", "abc"},
				{"MATCH (n:T) WHERE NOT n.id IN $p RETURN count(n) AS c", int64(5)},
				{"MATCH (n:T) WHERE n.id IN $p RETURN n.id AS id", map[string]interface{}{"a": 1}},
				{"RETURN 5 IN $p AS x", int64(5)},
			} {
				_, err := exec.Execute(ctx, tc.q, map[string]interface{}{"p": tc.p})
				assert.Error(t, err, "%s with $p=%v", tc.q, tc.p)
			}
			// Lists and null keep working.
			res, err := exec.Execute(ctx, "MATCH (n:T) WHERE n.id IN $p RETURN count(n) AS c", map[string]interface{}{"p": []interface{}{int64(5)}})
			require.NoError(t, err)
			assert.Equal(t, int64(1), res.Rows[0][0])
			res, err = exec.Execute(ctx, "MATCH (n:T) WHERE n.id IN $p RETURN count(n) AS c", map[string]interface{}{"p": nil})
			require.NoError(t, err)
			assert.Equal(t, int64(0), res.Rows[0][0])
		})
	}
}

func TestValidateMembershipParameters(t *testing.T) {
	params := map[string]interface{}{"p": int64(5), "l": []interface{}{int64(1)}, "m": map[string]interface{}{"list": []interface{}{1}}, "n": nil}
	for _, q := range []string{
		"MATCH (n) WHERE n.id IN $p RETURN n",
		"MATCH (n) WHERE (n.id IN $p) RETURN n",
		"MATCH (n) WHERE n.id in $p AND n.x = 1 RETURN n",
		"RETURN [x IN $p | x] AS l",
		"FOREACH (x IN $p | CREATE (:N))",
	} {
		assert.Error(t, validateMembershipParameters(q, params), q)
	}
	for _, q := range []string{
		"MATCH (n) WHERE n.id IN $l RETURN n",
		"MATCH (n) WHERE n.id IN $n RETURN n",
		"MATCH (n) WHERE n.id IN $m.list RETURN n",
		"MATCH (n) WHERE n.id IN $p + [1] RETURN n",
		"MATCH (n) WHERE n.id IN $missing RETURN n",
		"MATCH (n) WHERE n.name = 'x IN $p' RETURN n",
		"MATCH (n) WHERE n.id = $p RETURN n",
		"MATCH (n:INx) WHERE n.MIN > $p RETURN n",
	} {
		assert.NoError(t, validateMembershipParameters(q, params), q)
	}
}
