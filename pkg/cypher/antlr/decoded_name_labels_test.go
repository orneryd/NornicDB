package antlr_test

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
)

// Analyze is a public metadata consumer. ClauseInfo's variable adapters belong
// to the credentials layer; this check exercises only this layer's name owner.
func TestDecodedNameANTLRLabelMetadata(t *testing.T) {
	for _, name := range []string{"ordinary", " a  b ", " ", "  ", "\t", "a,b.c", "a`b", "A|B"} {
		t.Run(name, func(t *testing.T) {
			for _, query := range []string{
				"MATCH (n:" + cyphertext.QuoteName(name) + ") RETURN n",
				"MATCH ()-[r:" + cyphertext.QuoteName(name) + "]->() RETURN r",
			} {
				parsed, err := cypherantlr.Parse(query)
				require.NoError(t, err)
				info := cypherantlr.NewQueryAnalyzer().Analyze(query, parsed)
				require.Equal(t, []string{name}, info.Labels, query)
			}
		})
	}
}
