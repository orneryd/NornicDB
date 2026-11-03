package storage_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: PR #1670 implementation lane (Codex). The transaction's committed
// and final images retain the indexed key, despite different staged images.
// Holding Aux parks publication after Doc, using the existing index lock.
func TestCommitIndexMembershipNeverPublishesIntermediateNode(t *testing.T) {
	for _, kind := range indexKinds {
		readers := []struct{ name, query string }{{"seek", kind.reader}}
		if strings.HasPrefix(kind.reader, "MERGE") {
			readers[0].name = "merge"
			readers = append(readers, struct{ name, query string }{"seek", "MATCH (n:Doc {tenant:'t', id:$id}) RETURN n.c"})
		}
		for _, roundTrip := range []string{"key", "label"} {
			for _, reader := range readers {
				for _, route := range indexRoutes {
					t.Run(kind.name+"/"+roundTrip+"/"+reader.name+"/"+route.String(), func(t *testing.T) {
						env := openIndexEnv(t)
						admin := cypher.NewStorageExecutor(env.store)
						mustRunIndex(t, admin, kind.ddl, nil)
						mustRunIndex(t, admin, "CREATE INDEX aux_gate FOR (n:Aux) ON (n.gate)", nil)
						mustRunIndex(t, admin, "CREATE (:Doc:Aux {id:'stable', tenant:'t', gate:'g', c:0})", nil)
						nodes, err := env.store.GetNodesByLabel(context.Background(), "Doc")
						require.NoError(t, err)
						require.Len(t, nodes, 1)
						require.Equal(t, []string{"Doc", "Aux"}, nodes[0].Labels)
						writer := cypher.NewStorageExecutor(env.store)
						mustRunIndex(t, writer, "BEGIN", nil)
						defer writer.Execute(context.Background(), "ROLLBACK", nil)
						var first, last string
						if roundTrip == "key" {
							first = "MATCH (n:Doc {id:'stable'}) SET n.id = 'temporary', n.c = n.c + 1 RETURN n.c"
							last = "MATCH (n:Doc {id:'temporary'}) SET n.id = 'stable' RETURN n.c"
						} else {
							first = "MATCH (n:Doc {id:'stable'}) REMOVE n:Doc:Aux SET n.c = n.c + 1 RETURN n.c"
							last = "MATCH (n {gate:'g'}) SET n:Doc:Aux RETURN n.c"
						}
						require.Len(t, mustRunIndex(t, writer, first, nil), 1)
						require.Len(t, mustRunIndex(t, writer, last, nil), 1)
						gates := env.store.GetSchema().GetCompositeIndexesForLabel("Aux")
						require.Len(t, gates, 1)
						release, waiting := storage.HoldIndexReadForTesting(gates[0])
						var once sync.Once
						open := func() { once.Do(release) }
						defer open()
						ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
						defer cancel()
						written := make(chan indexOutcome, 1)
						go func() {
							_, err := writer.Execute(ctx, "COMMIT", nil)
							written <- indexOutcome{err: err}
						}()
						awaitIndexPublication(t, ctx, waiting, written)
						readCtx, cancelRead := context.WithTimeout(ctx, 20*time.Second)
						defer cancelRead()
						read := env.run(readCtx, cypher.NewStorageExecutor(env.store), route, reader.query, map[string]any{"id": "stable"})
						open()
						committed := <-written
						require.NoError(t, committed.err)
						require.NoError(t, read.err)
						require.Len(t, read.rows, 1, "a commit must not publish a staged key or label")
						require.Equal(t, [][]any{{int64(1), int64(1)}}, mustRunIndex(t, admin, "MATCH (n:Doc) RETURN count(n), sum(n.c)", nil), "MERGE must retain one node and the committed counter")
					})
				}
			}
		}
	}
}

func awaitIndexPublication(t *testing.T, ctx context.Context, waiting func() bool, written <-chan indexOutcome) {
	t.Helper()
	for !waiting() {
		select {
		case result := <-written:
			t.Fatalf("commit finished before reaching the held index or claim lock: %+v", result)
		case <-ctx.Done():
			t.Fatal("commit never reached the held index or claim lock")
		case <-time.After(time.Millisecond):
		}
	}
}
