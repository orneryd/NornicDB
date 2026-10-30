package antlr

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// Author: #1663 lane. A name-only view requests entry identity, while extent
// projection still requires the original separator tokens. The contextual
// product, rather than lexical entry shape, decides whether the name is a key.
func TestBraceKeyIdentityInOperandWindows(t *testing.T) {
	for _, test := range []struct {
		query, name string
		key         bool
	}{
		{`RETURN {x: 1}`, "x", true},
		{`MATCH (n {x: 1}) RETURN n`, "x", true},
		{`MATCH (n) RETURN COUNT { RETURN n:Label }`, "n", false},
	} {
		t.Run(test.query, func(t *testing.T) {
			source := cyphertext.NewSource(t.Context(), test.query, cyphertext.AcceptedSource)
			require.NoError(t, source.Err)
			analysis := OriginalBraceAnalysis(source, false, nil)
			require.NoError(t, analysis.Admission())
			start := strings.LastIndex(test.query, test.name+":")
			require.GreaterOrEqual(t, start, 0)
			view := source.Window(start, start+len(test.name))
			name := cyphertext.Span{End: len(view.Text)}
			require.Equal(t, test.key, analysis.MapKeyAt(view, name))
			colonView := source.Window(start+len(test.name), start+len(test.name)+1)
			require.Equal(t, test.key, analysis.MapColonAt(colonView, cyphertext.Span{End: 1}))
			if test.key {
				_, found := view.OriginalEntryAt(name)
				require.True(t, found)
				_, _, err := view.EntryAt(name)
				require.Error(t, err)
			}
		})
	}
}
