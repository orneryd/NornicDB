package fabric

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFoundationDiagnosticTokenStart(t *testing.T) {
	for _, test := range []struct{ source, token string }{
		{"$db", "$"}, {"1abc", "1abc"}, {"a$b", "a$b"},
		{"`a,b`", "`a,b`"}, {"→", "→"}, {":tail", ":"},
	} {
		require.Equal(t, test.token, inputToken(test.source, 0), test.source)
	}
}
