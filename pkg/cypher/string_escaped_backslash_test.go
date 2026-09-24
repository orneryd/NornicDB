package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A quote is escaped only when an odd number of backslashes precedes it:
// 'x\\' is the one-character-plus-backslash string x\ and its closing quote
// ends the literal (#541).
func TestIsBackslashEscapedCountsConsecutiveBackslashes(t *testing.T) {
	for _, tc := range []struct {
		s    string
		want bool
	}{
		{`'x'`, false},
		{`'x\'`, true},
		{`'x\\'`, false},
		{`'x\\\'`, true},
		{`'x\\\\'`, false},
	} {
		assert.Equal(t, tc.want, isBackslashEscaped(tc.s, len(tc.s)-1), tc.s)
	}
}

func TestSplittersKeepStringEndingInEscapedBackslash(t *testing.T) {
	parts := splitTopLevelComma(`n.a = 'x\\', n.b = 1`)
	require.Len(t, parts, 2)
	assert.Equal(t, `n.a = 'x\\'`, parts[0])

	strict := splitTopLevelCommaKeepEmpty(`n.b = 1, n.a = 'x\\', n.c = 'y'`)
	require.Len(t, strict, 3)
}

func TestStringLiteralEndingInEscapedBackslash(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			_, err := exec.Execute(ctx, "CREATE (:T)", nil)
			require.NoError(t, err)

			res, err := exec.Execute(ctx, `RETURN 'x\\' AS a`, nil)
			require.NoError(t, err)
			assert.Equal(t, `x\`, res.Rows[0][0])

			res, err = exec.Execute(ctx, `MATCH (n:T) SET n.a = 'x\\' RETURN n.a AS a`, nil)
			require.NoError(t, err)
			assert.Equal(t, `x\`, res.Rows[0][0])

			res, err = exec.Execute(ctx, `MATCH (n:T) SET n.a = 'y\\', n.b = 1 RETURN n.a AS a, n.b AS b`, nil)
			require.NoError(t, err)
			assert.Equal(t, `y\`, res.Rows[0][0])
			assert.EqualValues(t, 1, res.Rows[0][1])

			res, err = exec.Execute(ctx, `MATCH (n:T) SET n.b = 2, n.a = 'z\\' RETURN n.a AS a, n.b AS b`, nil)
			require.NoError(t, err)
			assert.Equal(t, `z\`, res.Rows[0][0])
			assert.EqualValues(t, 2, res.Rows[0][1])

			res, err = exec.Execute(ctx, `CREATE (m:W {p: 'C:\\temp\\', q: 'k'}) RETURN m.p AS p, m.q AS q`, nil)
			require.NoError(t, err)
			assert.Equal(t, `C:\temp\`, res.Rows[0][0])
			assert.Equal(t, "k", res.Rows[0][1])

			// An escaped quote still does not close the literal.
			res, err = exec.Execute(ctx, `RETURN 'it\'s' AS a`, nil)
			require.NoError(t, err)
			assert.Equal(t, "it's", res.Rows[0][0])

			if mode == "explicit transaction" {
				_, err = exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			res, err = exec.Execute(ctx, `MATCH (n:T) RETURN n.a AS a, n.b AS b`, nil)
			require.NoError(t, err)
			assert.Equal(t, `z\`, res.Rows[0][0])
		})
	}
}
