package bolt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRelationshipMergeRechecksAfterWaitingForEndpointWriter(t *testing.T) {
	for _, tc := range []struct{ name, prefix, pattern, first, action string }{
		{name: "on_match", action: "ON MATCH", pattern: "(d)-[r:HAS]->(x)", first: "(d)-[:HAS]->(x)"},
		{name: "unlocked", pattern: "(d)-[r:HAS]->(x)", first: "(d)-[:HAS]->(x)"},
		{name: "start_locked", prefix: "SET d += {} WITH d, x ", pattern: "(d)-[r:HAS]->(x)", first: "(d)-[:HAS]->(x)"},
		{name: "keyed_start_locked", prefix: "SET d += {} WITH d, x ", pattern: "(d)-[r:HAS {id:$rid}]->(x)", first: "(d)-[:HAS {id:$rid}]->(x)"},
		{name: "incoming", pattern: "(x)<-[r:HAS]-(d)", first: "(d)-[:HAS]->(x)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			action, wantHolders := tc.action, int64(2)
			if action == "" {
				action, wantHolders = "ON CREATE", 1
			}
			port, waits := nor213RelationshipStore(t, false)
			setup := openBoltTestConn(t, port)
			params := map[string]any{"d": "holder", "x": "asset", "rid": "identity"}
			runBoltQueryWithParams(t, setup, "CREATE (:Holder {id:$d}), (:Asset {id:$x, holders:0})", params)
			first, second := openBoltTestConn(t, port), openBoltTestConn(t, port)
			t.Cleanup(func() { _ = first.Close(); _ = second.Close(); _ = setup.Close() })
			nor213BoltBegin(t, first)
			nor213BoltBegin(t, second)
			const read = "MATCH (d:Holder {id:$d}), (x:Asset {id:$x}) OPTIONAL MATCH (d)-[r:HAS]->(x) RETURN count(r), x.holders"
			require.Equal(t, [][]any{{int64(0), int64(0)}}, runBoltQueryWithParams(t, first, read, params))
			require.Equal(t, [][]any{{int64(0), int64(0)}}, runBoltQueryWithParams(t, second, read, params))
			runBoltQueryWithParams(t, first, "MATCH (d:Holder {id:$d}), (x:Asset {id:$x}) CREATE "+tc.first+" SET x.holders = x.holders + 1", params)
			nor213DrainEntityWriteWaits(waits)
			done := make(chan error, 1)
			go func() {
				done <- nor213RunBoltStatement(t, second,
					"MATCH (d:Holder {id:$d}), (x:Asset {id:$x}) "+tc.prefix+"MERGE "+tc.pattern+" "+action+" SET x.holders = x.holders + 1", params)
			}()
			select {
			case <-waits:
			case err := <-done:
				t.Fatalf("MERGE must wait for the open endpoint writer: %v", err)
			case <-time.After(nor213WireWaitLimit):
				t.Fatal("MERGE neither waited nor completed")
			}
			nor213BoltCommit(t, first)
			require.NoError(t, nor213AwaitBoltStatement(t, second, done, "relationship MERGE"))
			nor213BoltCommit(t, second)
			require.Equal(t, [][]any{{int64(1), wantHolders}}, runBoltQueryWithParams(t, setup, read, params),
				"MERGE must match the relationship published while it waited, with the selected action applied exactly once")
		})
	}
}
