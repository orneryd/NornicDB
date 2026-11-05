package bolt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Exercise the real Bolt database-manager adapter, including BEGIN/RUN/PULL/
// COMMIT. A fresh connection for each case preserves its first error.
func TestBoltRowVariableMatchMaterialization(t *testing.T) {
	_, port := startTransactionLifecycleServer(t)
	seed := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, seed, "CREATE (a:CodeIntel {id:0,uuid:'b0'})-[:T {w:1}]->(b:CodeIntel {id:1,uuid:'b1'}), (b)-[:T {w:21}]->(a)")
	require.NoError(t, seed.Close())
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "transaction"
		}
		for _, tc := range []struct {
			name  string
			query string
			want  int64
		}{
			{"scalar", "UNWIND [0,1] AS i MATCH (a:CodeIntel {id:i})-[r:T]->() RETURN count(r)", 2},
			{"literal-subscript", "UNWIND [0,1] AS i MATCH (a:CodeIntel {uuid:['b0','b1'][i]})-[r:T]->() RETURN count(r)", 2},
			{"row-map-both-ends", "UNWIND [{src:'b0',tgt:'b1'},{src:'b1',tgt:'b0'}] AS row MATCH (a {uuid:row.src})-[r:T]->(b {uuid:row.tgt}) RETURN count(r)", 2},
			{"row-map-and-where", "UNWIND [{src:'b0',tgt:'b1',w:1},{src:'b1',tgt:'b0',w:21}] AS row MATCH (a {uuid:row.src})-[r:T]->(b {uuid:row.tgt}) WHERE r.w = row.w RETURN count(r)", 2},
			{"node-uuid", "UNWIND ['b0','b1'] AS node_uuid MATCH ()-[:T]->(n:CodeIntel {uuid:node_uuid}) RETURN count(n)", 2},
			{"pattern-where-row", "UNWIND [1,21] AS wanted MATCH (a:CodeIntel) WHERE (a)-[:T {w:wanted}]->() RETURN count(a)", 2},
			{"pattern-where-node", "MATCH (a:CodeIntel) WHERE (a)-[:T {w:a.id * 20 + 1}]->() RETURN count(a)", 2},
			{"pattern-where-map", "UNWIND [{wanted:1},{wanted:21}] AS row MATCH (a:CodeIntel) WHERE exists((a)-[:T {w:row.wanted}]->()) RETURN count(a)", 2},
			{"where-only", "UNWIND [0,1] AS i MATCH (a:CodeIntel)-[r:T]->() WHERE a.id = i RETURN count(r)", 2},
			{"relationship-map", "UNWIND [1,21] AS w MATCH ()-[r:T {w:w}]->() RETURN count(r)", 2},
			{"bound-relationship-list", "MATCH (a:CodeIntel)-[r:T]->() WITH [r] AS relationships, a.id AS wanted MATCH (start:CodeIntel {id:wanted})-[relationships*]->() RETURN count(start)", 2},
			{"bound-relationship-list-mismatch", "MATCH (a:CodeIntel)-[r:T]->() WITH [r] AS relationships, a.id + 10 AS wanted MATCH (start:CodeIntel {id:wanted})-[relationships*]->() RETURN count(start)", 0},
			{"bound-relationship-list-where", "MATCH (a:CodeIntel)-[r:T]->() WITH [r] AS relationships, a.id AS wanted MATCH (start:CodeIntel {id:wanted})-[relationships*]->() WHERE start.id = 1 RETURN count(start)", 1},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				conn := openBoltTestConn(t, port)
				defer conn.Close()
				if explicit {
					beginExplicitTransaction(t, conn, nil)
				}
				require.Equal(t, [][]any{{tc.want}}, runBoltQueryAndCollectRecords(t, conn, tc.query))
				if explicit {
					require.NoError(t, SendCommit(t, conn))
					require.NoError(t, ReadSuccess(t, conn))
				}
			})
		}
	}
}
