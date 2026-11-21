package nornicdb_test

// Lane L6b (closeout): S04 L-H1-004 follow-up. The storage-level run showed
// indexEdgeAdjacency drops the self-loop's incoming overlay entry, yet the
// engine's incoming read on a write-behind BadgerEngine still returned it.
// Here the database is opened as the verifier's server did (async writes on),
// and per iteration the same self-loop is read three ways right after the
// write: the lead's Cypher shape, the incoming storage read of the namespaced
// base storage, and the outgoing read (control). Decides whether the miss is
// in the storage incoming read or in the Cypher route.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestL6bSelfLoopAsyncDBLayers(t *testing.T) {
	for _, async := range []bool{true, false} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			cfg := nornicdb.DefaultConfig()
			cfg.Database.AsyncWritesEnabled = async
			cfg.Database.StrictDurability = !async
			db, err := nornicdb.Open(t.TempDir(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "nornic")
			cypherOK, inOK, outOK := 0, 0, 0
			const n = 30
			for i := 0; i < n; i++ {
				if _, err := db.ExecuteCypher(ctx, "MATCH (n) DETACH DELETE n", nil); err != nil {
					t.Fatal(err)
				}
				res, err := db.ExecuteCypher(ctx, "CREATE (n:A)-[:LOOP]->(n) RETURN elementId(n) AS e, id(n) AS i", nil)
				if err != nil || len(res.Rows) == 0 {
					t.Fatalf("create: %v", err)
				}
				nodeID := storage.NodeID(fmt.Sprint(res.Rows[0][1]))
				q, err := db.ExecuteCypher(ctx, "MATCH (:A)<--() RETURN count(*) AS c", nil)
				c := any(nil)
				if err == nil && len(q.Rows) > 0 {
					c = q.Rows[0][0]
				}
				in, inErr := store.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: nodeID, Headers: true})
				out, outErr := store.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentOutgoing, Node: nodeID, Headers: true})
				if fmt.Sprint(c) == "1" {
					cypherOK++
				}
				if len(in) == 1 {
					inOK++
				}
				if len(out) == 1 {
					outOK++
				}
				if i < 3 {
					t.Logf("L6B_S04DB async=%v iter=%d node=%s cypher count=%v err=%v | storage incoming=%d err=%v outgoing=%d err=%v", async, i, nodeID, c, err, len(in), inErr, len(out), outErr)
				}
			}
			t.Logf("L6B_S04DB_SUMMARY async=%v of %d: cypher (:A)<--() found=%d storage incoming found=%d storage outgoing found=%d", async, n, cypherOK, inOK, outOK)
		})
	}
}
