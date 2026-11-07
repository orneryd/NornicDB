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
				t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) {
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
					calls := 0
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
								return observed.Properties["n"].(int64) + 1, nil
							}, true
						}
						if previous != nil {
							return previous(name)
						}
						return nil, false
					})
					_, err = writer.Execute(ctx, "MATCH ()-[r:COUNT]->() SET r.n = audit.peerIncrement(r) RETURN r.n", nil)
					require.Equal(t, 1, calls)
					require.ErrorIs(t, err, storage.ErrConflict)
					if explicit {
						_, _ = writer.Execute(ctx, "ROLLBACK", nil)
					}
					PluginFunctionLookup = previous
					_, err = writer.Execute(ctx, "MATCH ()-[r:COUNT]->() SET r.n = r.n + 1 RETURN r.n", nil)
					require.NoError(t, err)
					require.NoError(t, base.FlushWriteBehind(ctx))
					result, err := writer.Execute(ctx, "MATCH ()-[r:COUNT]->() RETURN r.n AS n", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
				})
			}
		})
	}
}
