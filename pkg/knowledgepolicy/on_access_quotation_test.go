package knowledgepolicy

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEvalOnAccessCanonicalQuotation(t *testing.T) {
	for _, tc := range []struct{ literal, value string }{
		{`'O\'Reilly, Sons'`, "O'Reilly, Sons"},
		{`"Say \"yes, [now]\""`, `Say "yes, [now]"`},
		{`'a\\b, c'`, `a\b, c`},
		{`'a\n, b'`, "a\n, b"},
		{`'\u03bb, \U0001F600'`, "λ, 😀"},
		{`'\uD800'`, "\xed\xa0\x80"},
		{`'\uDC00'`, "\xed\xb0\x80"},
		{"'`a``b`,:{}[]() // /* */'", "`a``b`,:{}[]() // /* */"},
	} {
		got, err := evalOnAccessExpression(tc.literal, evalCtxWith(nil, nil))
		require.NoError(t, err, tc.literal)
		require.Equal(t, tc.value, got)
		got, err = evalOnAccessExpression("/* '\" */ "+tc.literal+" = n.name // ignored ' quote", evalCtxWith(map[string]interface{}{"name": tc.value}, nil))
		require.NoError(t, err, tc.literal)
		require.Equal(t, true, got)
	}
	for _, literal := range []string{`'unterminated`, `"unterminated`} {
		_, err := evalOnAccessExpression(literal, evalCtxWith(nil, nil))
		require.Error(t, err, literal)
	}
}
