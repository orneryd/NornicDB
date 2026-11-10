package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Registered scalars interpose a real peer write before body ownership, or
// queue a typed native preimage update behind it. Cypher refreshes its own
// observed row; an external precomputed native preimage must still conflict.
func TestRelationshipWritersSerializeAndPreserveNativePreimages(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previousParser := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previousParser) })
			for _, explicit := range []bool{false, true} {
				for _, afterStaging := range []bool{false, true} {
					t.Run(fmt.Sprintf("explicit=%t/after-staging=%t", explicit, afterStaging), func(t *testing.T) {
						ctx := t.Context()
						base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour}))
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, base.Close()) })
						store := storage.NewNamespacedEngine(base, "relationship_observation")
						writer := NewStorageExecutor(store)
						_, err = writer.Execute(ctx, "CREATE (a)-[:COUNT {n:0}]->(b)", nil)
						require.NoError(t, err)
						require.NoError(t, base.FlushWriteBehind(ctx))
						if explicit {
							_, err = writer.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						t.Cleanup(func() { _, _ = writer.Execute(ctx, "ROLLBACK", nil) })
						peerCtx, cancelPeer := context.WithTimeout(ctx, 10*time.Second)
						defer cancelPeer()
						done := make(chan error, 1)
						started := false
						t.Cleanup(func() {
							cancelPeer()
							if started {
								select {
								case <-done:
								case <-time.After(5 * time.Second):
									t.Error("queued peer did not finish after cancellation")
								}
							}
						})
						waits := fixtureowner.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 1)
						previous := PluginFunctionLookup
						t.Cleanup(func() { PluginFunctionLookup = previous })
						calls, peerCommits := 0, 0
						PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) {
							if name == "audit.peerIncrement" {
								return func(ctx context.Context, observed *storage.Edge) (int64, error) {
									calls++
									peer, err := store.GetEdge(ctx, observed.ID)
									if err != nil {
										return 0, err
									}
									before := *peer
									peer.Properties = map[string]interface{}{"n": int64(1)}
									writePeer := func() error {
										return store.UpdateEdge(peerCtx, storage.EdgeUpdateRequest{Before: &before, After: peer})
									}
									if afterStaging {
										started = true
										go func() { done <- writePeer() }()
										fixtureowner.WaitForBarrier(t, waits, 5*time.Second, cancelPeer, "the native preimage update did not wait for the Cypher body owner")
									} else {
										if err := writePeer(); err != nil {
											return 0, err
										}
										peerCommits++
									}
									return 1, nil
								}, true
							}
							if previous != nil {
								return previous(name)
							}
							return nil, false
						})
						query := "MATCH ()-[r:COUNT]->() WITH r, audit.peerIncrement(r) AS ignored SET r.n = r.n + 1 RETURN r.n"
						if afterStaging {
							query = "MATCH ()-[r:COUNT]->() SET r.n = r.n + 1 WITH r, audit.peerIncrement(r) AS ignored RETURN r.n"
						}
						result, err := writer.Execute(ctx, query, nil)
						require.NoError(t, err)
						if explicit {
							_, err = writer.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
						require.Equal(t, 1, calls)
						if afterStaging {
							select {
							case err := <-done:
								started = false
								require.ErrorIs(t, err, storage.ErrConflict, "locking must not authorize an external stale precomputed image")
							case <-time.After(5 * time.Second):
								t.Fatal("native peer did not finish after the Cypher commit")
							}
							require.Zero(t, peerCommits)
							require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
						} else {
							require.Equal(t, 1, peerCommits)
							require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, "Cypher increments the refreshed committed row")
						}
						PluginFunctionLookup = previous
						_, err = writer.Execute(ctx, "MATCH ()-[r:COUNT]->() SET r.n = r.n + 1, r.ok = true RETURN r.n", nil)
						require.NoError(t, err)
						require.NoError(t, base.FlushWriteBehind(ctx))
						result, err = writer.Execute(ctx, "MATCH ()-[r:COUNT]->() RETURN r.n AS n, r.ok AS ok", nil)
						require.NoError(t, err)
						want := int64(3)
						if afterStaging {
							want = 2
						}
						require.Equal(t, [][]interface{}{{want, true}}, result.Rows)
					})
				}
			}
		})
	}
}
