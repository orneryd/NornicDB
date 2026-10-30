package fabric

import (
	"context"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// testStatementWrites stands in for the Cypher executor's statement-effect
// owner in planner tests: a fragment with a mutation keyword outside strings,
// quoted names and comments writes.
func testStatementWrites(_ context.Context, query string) bool {
	for i := 0; i < len(query); i++ {
		if next, quoted := cyphertext.SkipToken(query, i); quoted {
			i = next - 1
			continue
		}
		for _, keyword := range []string{"CREATE", "MERGE", "DETACH DELETE", "DELETE", "SET", "REMOVE"} {
			if keywordAt(query, i, keyword) {
				return true
			}
		}
	}
	return false
}
