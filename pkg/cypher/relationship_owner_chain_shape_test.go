package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A bound COUNT/EXISTS chain body is admitted once per body; each row then
// only traverses from its bound entities.
func TestBoundChainShapeReadOncePerBody(t *testing.T) {
	for body, want := range map[string]boundChainShape{
		"(a)-[:A|B]->(:Q)":                       {pattern: "(a)-[:A|B]->(:Q)", ok: true},
		"MATCH (a)-[:`A|B`]->(q) WHERE q.id > 1": {pattern: "(a)-[:`A|B`]->(q)", where: "q.id > 1", ok: true},
		"(a)-[:A]->(q:Q {id: a.id})":             {},
		"(a)-[:A]->(q), (q)-[:B]->(r)":           {},
		"MATCH (a)-[:A]->(q) RETURN q":           {},
		"MATCH p = (a)-[:A]->(q)":                {},
	} {
		require.Equal(t, want, boundChainShapeFor(body), body)
		require.Zero(t, testing.AllocsPerRun(20, func() { boundChainShapeFor(body) }), body)
	}
}
