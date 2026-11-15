package storage_test

// B21 public transport regression: a temporal constraint created over a label
// whose nodes already exist, issued through a real Bolt or HTTP client.
//
// The statement's begin holds the engine's exclusive lifecycle admission from
// before its snapshot, so the history and current-version entries its index
// needs for those nodes are staged in the statement's own transaction
// (storage.MarkNewTemporalConstraintsForEngine). The statement publishes the
// constraint and its entries together: the constraint answers for the node
// written before it, and the same transaction rolls both back if it fails.
// A pass of the engine's own maintenance would have to take that admission a
// second time, which the statement already holds: the epoch test
// TestSymmetricEpochIndexSurvivesRestart pins the statement's own lifetime.
//
// The in-package schedules (schema_ddl_temporal_test.go) pin the staging seam:
// nothing reaches storage before the commit, a rollback discards both, and a
// view whose report does not own its transaction is refused.
//
// The Cypher parser is process-wide (config.init), so both parsers run these
// identical statements in separate NORNICDB_PARSER=nornic and
// NORNICDB_PARSER=antlr runs of this test.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublicTemporalConstraintOverExistingNodes runs the regression over both
// public transports, with write-behind off and on.
func TestPublicTemporalConstraintOverExistingNodes(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, route := range []string{"Bolt", "HTTP"} {
			t.Run(fmt.Sprintf("writeBehind=%v/%s", writeBehind, route), func(t *testing.T) {
				fixture := openProtocolServerFixture(t, protocolServerOptions{writeBehind: writeBehind})
				// The label's nodes are stored before the constraint exists.
				fixture.seed(t,
					"CREATE (:Period {key:'a', name:'first', start:datetime('2026-01-01T00:00:00Z'), end:datetime('2026-01-10T00:00:00Z')})",
					"CREATE (:Period {key:'b', name:'other', start:datetime('2026-01-05T00:00:00Z'), end:datetime('2026-01-15T00:00:00Z')})",
				)
				client := pr1655OpenPublicClient(t, fixture.server, route)

				_, err := client.run("CREATE CONSTRAINT periods FOR (n:Period) REQUIRE temporal.epoch(n.start,n.end) GROUP BY n.key")
				require.NoError(t, err, "a temporal constraint over existing nodes must publish")

				// The published index answers for the node written before it.
				rows, err := client.run("CALL db.temporal.asOf('Period','key','a','start','end','2026-01-07T00:00:00Z') YIELD node RETURN node.name AS name")
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"first"}}, rows,
					"the current version of the node written before the constraint")

				_, err = client.run("CREATE (:Period {key:'a', name:'overlap', start:datetime('2026-01-05T00:00:00Z'), end:datetime('2026-01-15T00:00:00Z')})")
				require.Error(t, err, "an interval overlapping one written before the constraint must be rejected")

				_, err = client.run("CREATE (:Period {key:'a', name:'adjacent', start:datetime('2026-01-10T00:00:00Z'), end:datetime('2026-01-20T00:00:00Z')})")
				require.NoError(t, err, "a touching interval is not an overlap")
				_, err = client.run("CREATE (:Period {key:'c', name:'other-group', start:datetime('2026-01-05T00:00:00Z'), end:datetime('2026-01-15T00:00:00Z')})")
				require.NoError(t, err, "another grouping key is its own history")

				// A write-behind acknowledgement may complete a write before its
				// replay applies it: drain through the existing public flush and
				// assert the accepted writes are stored.
				require.NoError(t, fixture.engine.FlushWriteBehind(context.Background()))
				for _, name := range []string{"first", "other", "adjacent", "other-group"} {
					rows, err := client.run(fmt.Sprintf("MATCH (n:Period {name:'%s'}) RETURN n.key AS key", name))
					require.NoError(t, err)
					require.Lenf(t, rows, 1, "the accepted write %q is stored", name)
				}
			})
		}
	}
}
