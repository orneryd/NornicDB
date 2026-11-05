package server

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Bolt and HTTP results are unchanged by storage publishing written bodies as
// their codec image: for every value kind a client can send, the row a CREATE
// returns, the row a MATCH returns right after the write and the row it
// returns after a restart are the same, over both protocols.
func TestProtocolValuesUnchangedByStoredImage(t *testing.T) {
	literals := []string{
		"30", "-7", "0.5", "'x'", "true", "null", "['a', 'b']", "[1, 2]", "[1, 2.5]", "[0.5, 1.5]", "[true, false]", "[]",
		"date('2024-01-02')", "datetime('2024-01-02T03:04:05.000000006+01:00')",
		"localdatetime('2024-01-02T03:04:05')", "duration('P1DT2H')", "point({x: 1.5, y: 2.5})",
	}
	set, returns, relReturns := "", "", ""
	for i, literal := range literals {
		set += fmt.Sprintf(", v%d: %s", i, literal)
		if i > 0 {
			returns += ", "
			relReturns += ", "
		}
		returns += fmt.Sprintf("n.v%d", i)
		relReturns += fmt.Sprintf("r.v%d", i)
	}
	previousParser := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previousParser) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, route := range []string{"bolt", "http"} {
			for _, buffered := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/write-behind=%v", parser, route, buffered), func(t *testing.T) {
					config.SetParserType(parser)
					dir := t.TempDir()
					h := openHiddenDeleteServer(t, dir, buffered, route)
					t.Cleanup(func() {
						if h != nil {
							h.shutdown(t)
						}
					})
					run := func(query string) [][]interface{} {
						t.Helper()
						outcome := h.run(t, query)
						require.Empty(t, outcome.code, query)
						return outcome.rows
					}
					created := run("CREATE (n:K {id: 'n'" + set + "}) RETURN " + returns)
					written := run("MATCH (a:K {id: 'n'}) CREATE (a)-[r:LINK {id: 'r'" + set + "}]->(:E) RETURN " + relReturns)
					matchNode := "MATCH (n:K {id: 'n'}) RETURN " + returns
					matchRel := "MATCH ()-[r:LINK {id: 'r'}]->() RETURN " + relReturns
					afterNode, afterRel := run(matchNode), run(matchRel)
					h.shutdown(t)
					h = openHiddenDeleteServer(t, dir, buffered, route)
					restartNode, restartRel := run(matchNode), run(matchRel)
					require.Equal(t, created, afterNode, "node: CREATE vs MATCH after write")
					require.Equal(t, afterNode, restartNode, "node: MATCH after write vs after restart")
					require.Equal(t, written, afterRel, "relationship: CREATE vs MATCH after write")
					require.Equal(t, afterRel, restartRel, "relationship: MATCH after write vs after restart")
				})
			}
		}
	}
}
