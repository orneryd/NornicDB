package knowledgepolicy

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOnAccessLexicalFoundation(t *testing.T) {
	for _, separator := range []rune{'\t', '\n', '\v', '\f', '\r', ' ', '\u0085', '\u00a0', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200a', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000'} {
		t.Run(fmt.Sprintf("U%04X", separator), func(t *testing.T) {
			query := "$café" + string(separator) + "+" + string(separator) + "2"
			value, err := evalOnAccessExpression(query, evalCtxWith(nil, map[string]any{"café": int64(3)}))
			require.NoError(t, err)
			require.Equal(t, int64(5), value)
		})
	}
	for _, malformed := range []string{"1.2.3", "1__0", "0xg", "'a''b'"} {
		_, err := evalOnAccessExpression(malformed, evalCtxWith(nil, nil))
		require.Error(t, err)
	}
}
