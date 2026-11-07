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

// A registered scalar publishes a real peer write after MATCH read r, before
// SET stages it. This drives canonical Cypher mutation callers, not a storage
// helper or a synthetic error-injecting engine.
func TestRelationshipSetRejectsChangedObservedImage(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previousParser := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previousParser) })
			for _, explicit := range []bool{false, true} {
				for _, commitPhase := range []bool{false, true} {
					t.Run(fmt.Sprintf("explicit=%t/after-staging=%t", explicit, commitPhase), func(t *testing.T) {
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
							t.Cleanup(func() { _, _ = writer.Execute(ctx, "ROLLBACK", nil) })
						}
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
									if err := store.UpdateEdge(ctx, storage.EdgeUpdateRequest{Before: &before, After: peer}); err != nil {
										return 0, err
									}
									peerCommits++
									return observed.Properties["n"].(int64) + 1, nil
								}, true
							}
							if previous != nil {
								return previous(name)
							}
							return nil, false
						})
						query := "MATCH ()-[r:COUNT]->() SET r.n = audit.peerIncrement(r) RETURN r.n"
						if commitPhase {
							query = "MATCH ()-[r:COUNT]->() SET r.n = r.n + 1 WITH r, audit.peerIncrement(r) AS ignored RETURN r.n"
						}
						_, err = writer.Execute(ctx, query, nil)
						if explicit && commitPhase {
							require.NoError(t, err)
							_, err = writer.Execute(ctx, "COMMIT", nil)
						}
						require.Equal(t, 1, calls)
						require.Equal(t, 1, peerCommits, "the interposed write must actually succeed")
						require.ErrorIs(t, err, storage.ErrConflict)
						if explicit {
							_, _ = writer.Execute(ctx, "ROLLBACK", nil)
						}
						PluginFunctionLookup = previous
						_, err = writer.Execute(ctx, "MATCH ()-[r:COUNT]->() SET r.n = r.n + 1, r.ok = true RETURN r.n", nil)
						require.NoError(t, err)
						require.NoError(t, base.FlushWriteBehind(ctx))
						result, err := writer.Execute(ctx, "MATCH ()-[r:COUNT]->() RETURN r.n AS n", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
					})
				}
			}
		})
	}
}
