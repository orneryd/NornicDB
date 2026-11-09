package storage_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: PR #1670 implementation lane. Test only; no product seam.
//
// A node carries :Doc and :Aux, each with an index. Node maintenance walks the
// labels in order, so while this test holds the :Aux index's read lock a counter
// update has already finished with the :Doc index and is parked on :Aux. The
// update has not changed the indexed key, so a seek of that key on every public
// route must find the one existing node at that point.
func TestUnchangedIndexedKeyVisibleWhileWriteMaintainsAnotherIndex(t *testing.T) {
	for _, kind := range indexKinds {
		for _, route := range indexRoutes {
			t.Run(kind.name+"/"+route.String(), func(t *testing.T) {
				env := openIndexEnv(t)
				admin := cypher.NewStorageExecutor(env.store)
				mustRunIndex(t, admin, kind.ddl, nil)
				mustRunIndex(t, admin, "CREATE INDEX aux_gate FOR (n:Aux) ON (n.gate)", nil)
				mustRunIndex(t, admin, "CREATE (:Doc:Aux {id:'stable', tenant:'t', gate:'g', c:0})", nil)
				nodes, err := env.store.GetNodesByLabel(context.Background(), "Doc")
				require.NoError(t, err)
				require.Len(t, nodes, 1)
				require.Equal(t, []string{"Doc", "Aux"}, nodes[0].Labels, "the schedule needs :Doc maintained before :Aux")
				gates := env.store.GetSchema().GetCompositeIndexesForLabel("Aux")
				require.Len(t, gates, 1)

				release, writerWaiting := storage.HoldIndexReadForTesting(gates[0])
				var releaseOnce sync.Once
				open := func() { releaseOnce.Do(release) }
				defer open()

				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				written := make(chan indexOutcome, 1)
				go func() {
					written <- runIndexExecute(ctx, cypher.NewStorageExecutor(env.store), false, "MATCH (n:Doc {id:'stable'}) SET n.c = n.c + 1 RETURN n.c", nil)
				}()
				awaitIndexPublication(t, ctx, writerWaiting, written)

				readCtx, cancelRead := context.WithTimeout(ctx, 20*time.Second)
				defer cancelRead()
				read := env.run(readCtx, cypher.NewStorageExecutor(env.store), route, kind.reader, map[string]any{"id": "stable"})
				open()
				writer := <-written
				require.NoError(t, writer.err)
				require.Len(t, writer.rows, 1)
				require.NoError(t, read.err, "the seek must complete while the update is in index maintenance")
				require.Len(t, read.rows, 1, "a seek on an unchanged key during index maintenance must find the node")
				require.Equal(t, [][]any{{int64(1), int64(1)}}, mustRunIndex(t, admin, "MATCH (n:Doc) RETURN count(n), sum(n.c)", nil),
					"exactly the one counter node, incremented once")
			})
		}
	}
}
