package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	grammar "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUseReferenceRetainsOriginalModifierBody(t *testing.T) {
	for _, tc := range []struct {
		body string
		args []string
	}{{"ALL", nil}, {"/*c*/aLl/*c*/", nil}, {"DISTINCT all", []string{"all"}}, {"DISTINCT `all`", []string{"`all`"}}, {"`ALL`", []string{"`ALL`"}}, {"'ALL'", []string{"'ALL'"}}, {"all.x", []string{"all.x"}}, {"(all)", []string{"(all)"}}, {"all+''", []string{"all+''"}}, {"all,x", []string{"all", "x"}}, {"ALL日本", []string{"ALL日本"}}, {"", nil}} {
		t.Run(tc.body, func(t *testing.T) {
			selector := "USE graph.byName(" + tc.body + ")"
			reference, err := grammar.ReadUseReference(t.Context(), cyphertext.NewSource(t.Context(), selector+" RETURN 7", cyphertext.RawSource))
			require.NoError(t, err)
			require.Equal(t, "graph.byName", reference.Function)
			require.Equal(t, tc.args, reference.Args)
			require.Equal(t, len(selector), reference.End)
		})
	}
}
