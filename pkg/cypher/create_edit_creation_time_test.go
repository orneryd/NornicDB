package cypher

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The transaction stamps creation times on the namespaced copy of each node
// and relationship a statement creates. The executor's own image, which every
// later edit in the statement sends back as the complete image, must carry the
// same times, for single and batched creates alike (NOR-295).
func TestCreateThenEditKeepsTransactionCreationTimes(t *testing.T) {
	statements := []string{
		"CREATE (n:Stamp {id: 'set'}) SET n.payload = 'x'",
		"UNWIND ['unwind-1', 'unwind-2', 'unwind-3'] AS id CREATE (n:Stamp {id: id}) SET n.payload = 'x'",
		"UNWIND ['batch-1', 'batch-2'] AS id CREATE (a:Stamp {id: id + '-start'})-[r:BATCH {id: id}]->(b:Stamp {id: id + '-end'}) SET r.payload = 'x', a.payload = 'x', b.payload = 'x'",
		"CREATE (a:Stamp {id: 'pair-a'})-[r:PAIR {id: 'pair-1'}]->(b:Stamp {id: 'pair-b'}), (b)-[s:PAIR {id: 'pair-2'}]->(a) SET r.payload = 'x', s.payload = 'x', a.payload = 'x', b.payload = 'x'",
		"MERGE (n:Stamp {id: 'merge'}) ON CREATE SET n.payload = 'x' REMOVE n.missing SET n:Merged",
	}
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint("explicit=", explicit), func(t *testing.T) {
			exec, store := newTestExecutor(t)
			ctx := t.Context()
			started := time.Now()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			for _, statement := range statements {
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			finished := time.Now()

			nodes, err := storage.GetStoredNodesByLabel(ctx, store, "Stamp")
			require.NoError(t, err)
			require.Len(t, nodes, 11)
			edges := 0
			within := func(what string, created, updated time.Time) {
				t.Helper()
				assert.False(t, created.IsZero(), "%s: CreatedAt is the zero time; UpdatedAt=%s", what, updated)
				assert.False(t, created.Before(started) || created.After(finished), "%s: CreatedAt %s outside [%s, %s]", what, created, started, finished)
				assert.False(t, updated.Before(created) || updated.After(finished), "%s: UpdatedAt %s precedes CreatedAt %s", what, updated, created)
			}
			for _, node := range nodes {
				within(fmt.Sprint("node ", node.Properties["id"]), node.CreatedAt, node.UpdatedAt)
				require.Equal(t, "x", node.Properties["payload"], "node %v keeps its same-statement edit", node.Properties["id"])
				outgoing, err := storage.ReadStoredAdjacentEdges(ctx, store, storage.AdjacentEdgeRead{Direction: storage.AdjacentOutgoing, Node: node.ID})
				require.NoError(t, err)
				for _, edge := range outgoing {
					edges++
					within(fmt.Sprint("relationship ", edge.Properties["id"]), edge.CreatedAt, edge.UpdatedAt)
					require.Equal(t, "x", edge.Properties["payload"], "relationship %v keeps its same-statement edit", edge.Properties["id"])
				}
			}
			require.Equal(t, 4, edges)
		})
	}
}
