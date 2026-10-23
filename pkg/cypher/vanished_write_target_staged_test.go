package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An explicit transaction stages a relationship SET/REMOVE or an endpoint
// CREATE/MERGE and an unrelated marker; a peer autocommit statement then
// deletes the target. COMMIT fails as a concurrent modification and the
// transaction leaves nothing behind (independent review of d9c2ff3, jobs
// db00/a75f).
func TestVanishedWriteTargetPublicDeleteAfterStaging(t *testing.T) {
	for _, write := range []string{
		"MATCH ()-[r:R]->() SET r.note = 'set'",
		"MATCH ()-[r:R]->() REMOVE r.note",
		"MATCH (a:A), (b:B) CREATE (a)-[:NEW]->(b)",
		"MATCH (a:A), (b:B) MERGE (a)-[:NEW]->(b)",
	} {
		t.Run(write, func(t *testing.T) {
			b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer b.Close()
			store := storage.NewNamespacedEngine(b, "test")
			ctx := context.Background()
			setup := NewStorageExecutor(store)
			_, err = setup.Execute(ctx, "CREATE (:A)-[:R {note: 'old'}]->(:B)", nil)
			require.NoError(t, err)
			require.NoError(t, b.FlushWriteBehind())
			writer, peer := NewStorageExecutor(store), NewStorageExecutor(store)
			_, err = writer.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer writer.Execute(ctx, "ROLLBACK", nil)
			_, err = writer.Execute(ctx, write, nil)
			require.NoError(t, err)
			_, err = writer.Execute(ctx, "CREATE (:Marker {id: 'must-roll-back'})", nil)
			require.NoError(t, err)
			drop := "MATCH ()-[r:R]->() DELETE r"
			if strings.Contains(write, ":NEW") {
				drop = "MATCH (b:B) DETACH DELETE b"
			}
			_, err = peer.Execute(ctx, drop, nil)
			require.NoError(t, err)
			_, err = writer.Execute(ctx, "COMMIT", nil)
			require.ErrorIs(t, err, storage.ErrConflict)
			require.NoError(t, b.FlushWriteBehind())
			result, err := setup.Execute(ctx, "MATCH (m:Marker) RETURN count(m)", nil)
			require.NoError(t, err)
			require.Equal(t, int64(0), result.Rows[0][0], "the failed transaction keeps no earlier write")
			result, err = setup.Execute(ctx, "MATCH ()-[r]->() RETURN count(r)", nil)
			require.NoError(t, err)
			require.Equal(t, int64(0), result.Rows[0][0], "no relationship is revived or created")
		})
	}
}

// Concurrent autocommit statements: one SETs a relationship or node and pads
// the statement with more writes, the other deletes the same target. When
// both succeed the delete is never undone (independent review of 2be99f2,
// TestClaudeR73PublicConcurrentSetDelete).
func TestVanishedWriteTargetPublicConcurrentSetDelete(t *testing.T) {
	for _, writeBehind := range []bool{true, false} {
		for _, target := range []string{"relationship", "node"} {
			t.Run(fmt.Sprintf("wb=%t/%s", writeBehind, target), func(t *testing.T) {
				b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				defer b.Close()
				flush := func() {
					if writeBehind {
						require.NoError(t, b.FlushWriteBehind())
					}
				}
				store := storage.NewNamespacedEngine(b, "test")
				ctx := context.Background()
				setup := NewStorageExecutor(store)
				_, err = setup.Execute(ctx, "UNWIND range(1, 96) AS k CREATE (:Pad {k: k})", nil)
				require.NoError(t, err)
				_, err = setup.Execute(ctx, "CREATE (:Anchor)", nil)
				require.NoError(t, err)
				setQuery := "MATCH (:Anchor)-[r:R {i: $i}]->() SET r.note = 'set' WITH count(r) AS c MATCH (p:Pad) SET p.v = $i"
				deleteQuery := "MATCH (:Anchor)-[r:R {i: $i}]->() DELETE r"
				countQuery := "MATCH (:Anchor)-[r:R {i: $i}]->() RETURN count(r)"
				if target == "node" {
					setQuery = "MATCH (n:T {i: $i}) SET n.note = 'set' WITH count(n) AS c MATCH (p:Pad) SET p.v = $i"
					deleteQuery = "MATCH (n:T {i: $i}) DETACH DELETE n"
					countQuery = "MATCH (n:T {i: $i}) RETURN count(n)"
				}
				const rounds = 300
				bothOK, revived := 0, 0
				for i := 0; i < rounds; i++ {
					params := map[string]any{"i": int64(i)}
					if target == "node" {
						_, err = setup.Execute(ctx, "CREATE (:T {i: $i})", params)
					} else {
						_, err = setup.Execute(ctx, "MATCH (a:Anchor), (p:Pad {k: 1}) CREATE (a)-[:R {i: $i}]->(p)", params)
					}
					require.NoError(t, err)
					flush()
					writer, peer := NewStorageExecutor(store), NewStorageExecutor(store)
					var wg sync.WaitGroup
					start := make(chan struct{})
					var setErr, deleteErr error
					wg.Add(2)
					go func() {
						defer wg.Done()
						<-start
						_, setErr = writer.Execute(ctx, setQuery, params)
					}()
					go func() {
						defer wg.Done()
						<-start
						for spin := 0; spin < i%24; spin++ {
							runtime.Gosched()
						}
						_, deleteErr = peer.Execute(ctx, deleteQuery, params)
					}()
					close(start)
					wg.Wait()
					for _, err := range []error{setErr, deleteErr} {
						if err != nil {
							require.ErrorIs(t, err, storage.ErrConflict)
						}
					}
					if setErr != nil || deleteErr != nil {
						continue
					}
					bothOK++
					flush()
					result, err := setup.Execute(ctx, countQuery, params)
					require.NoError(t, err)
					if result.Rows[0][0] != int64(0) {
						revived++
						t.Logf("round %d: SET and DELETE both succeeded and the deleted %s is present", i, target)
					}
				}
				t.Logf("wb=%t %s: rounds=%d bothSucceeded=%d revived=%d", writeBehind, target, rounds, bothOK, revived)
				require.Zero(t, revived, "an acknowledged DELETE was undone by a concurrent SET")
			})
		}
	}
}
