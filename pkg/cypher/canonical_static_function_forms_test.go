package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The static function rules read calls from the grammar: argument types and
// arity, trim's FROM form, reduce, DISTINCT in an aggregate, and a type
// predicate that is not a call. Every form gives the same outcome in RUN,
// EXPLAIN and PROFILE; accepted forms run. Each leaf logs its outcome so a
// release run of the same test compares leaf by leaf.
func TestCanonicalStaticFunctionForms(t *testing.T) {
	for _, form := range []struct {
		name, query string
		accepted    bool
	}{
		{"argument type", "RETURN toUpper(1) AS v", false},
		{"node argument", "RETURN labels('x') AS v", false},
		{"arity", "RETURN substring('abc') AS v", false},
		{"trim source", "RETURN trim(1) AS v", false},
		{"trim from characters", "RETURN trim(LEADING 1 FROM 'x') AS v", false},
		{"trim from source", "RETURN trim(BOTH 'x' FROM 1) AS v", false},
		{"reduce list", "RETURN reduce(s = 0, x IN 1 | s + x) AS v", false},
		{"distinct nested call", "RETURN count(DISTINCT toUpper(1)) AS v", false},
		{"accepted argument", "RETURN toUpper('a') AS v", true},
		{"accepted trim from", "RETURN trim(LEADING 'x' FROM 'xa') AS v", true},
		{"accepted reduce", "RETURN reduce(s = 0, x IN [1, 2] | s + x) AS v", true},
		{"accepted distinct", "RETURN count(DISTINCT 1) AS v", true},
		{"type predicate", "RETURN 1 IS :: INTEGER AS v", true},
		{"unwound argument", "UNWIND [1] AS x RETURN toString(x) AS v", true},
	} {
		t.Run(form.name, func(t *testing.T) {
			var wantCode, wantDetail, wantMessage string
			for index, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
				exec, ctx := newUnitExecutor(t)
				result, err := exec.Execute(ctx, mode+form.query, nil)
				code, detail, message := boundRuleError(err)
				t.Logf("STATIC_FUNCTION_FORM_LEAF query=%q code=%q detail=%q message=%q result=%t", mode+form.query, code, detail, message, result != nil)
				if form.accepted {
					require.NoError(t, err, mode+form.query)
					continue
				}
				require.Error(t, err, mode+form.query)
				if index == 0 {
					wantCode, wantDetail, wantMessage = code, detail, message
					continue
				}
				require.Equal(t, wantCode, code, mode+form.query)
				require.Equal(t, wantDetail, detail, mode+form.query)
				require.Equal(t, wantMessage, message, mode+form.query)
			}
		})
	}
}
