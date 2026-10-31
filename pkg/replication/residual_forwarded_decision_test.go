package replication

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The follower role forwards a serialized leaf/bulk command to the real
// standalone leader owner. Native prepared state never crosses this boundary.
type residualForwardingReplicator struct {
	*StandaloneReplicator
	kinds []CommandType
}

// The public Engine-only contract has no native transaction capability. It
// selects the executor's supported direct leaf/bulk route; the wrapped real
// NamespacedEngine still supplies every read and mutation implementation.
type residualDirectEngine struct{ storage.Engine }

func (*residualForwardingReplicator) IsLeader() bool { return false }
func (r *residualForwardingReplicator) Apply(cmd *Command, timeout time.Duration) error {
	r.kinds = append(r.kinds, cmd.Type)
	wire, err := storage.EncodeCommandPayload(cmd)
	if err != nil {
		return err
	}
	var forwarded Command
	if err := storage.DecodeCommandPayload(wire, &forwarded); err != nil {
		return err
	}
	return r.StandaloneReplicator.Apply(&forwarded, timeout)
}

func TestResidualForwardedCypherDecision(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		for _, veto := range []bool{false, true} {
			t.Run(fmt.Sprintf("bulk=%v/veto=%v", bulk, veto), func(t *testing.T) {
				leader, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
				require.NoError(t, err)
				t.Cleanup(func() { _ = leader.Close() })
				follower, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
				require.NoError(t, err)
				t.Cleanup(func() { _ = follower.Close() })
				r, mirror := newAcceptedReplicationFixture(t, leader, follower, &Config{})
				leaderManager, err := multidb.NewDatabaseManager(context.Background(), NewReplicatedEngine(leader, r, time.Second), nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = leaderManager.Close() })
				require.NoError(t, leaderManager.CreateDatabase(context.Background(), multidb.DatabaseCreateRequest{Name: "forwarded"}))
				transport := &residualForwardingReplicator{StandaloneReplicator: r}
				followerManager, err := multidb.NewDatabaseManager(context.Background(), NewReplicatedEngine(follower, transport, time.Second), nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = followerManager.Close() })
				view, err := followerManager.GetStorage("forwarded")
				require.NoError(t, err)
				before := mirror.calls.Load()
				var calls atomic.Int32
				var callbackErr error
				rejected := errors.New("forwarded local-context veto")
				bind := func(local context.Context) context.Context {
					var bound context.Context
					bound = storage.WithPublicationDecision(local, func(actual context.Context, _ storage.PublicationIdentity, action func() error) error {
						calls.Add(1)
						if actual != bound || mirror.calls.Load() != before {
							callbackErr = errors.New("forwarded decision lost its local frame or ran after acceptance")
						}
						if veto {
							return rejected
						}
						return action()
					})
					return bound
				}
				previous := leaderPublicationContextHook.Swap(&bind)
				t.Cleanup(func() { leaderPublicationContextHook.Store(previous) })
				exec := cypher.NewStorageExecutor(residualDirectEngine{view}, cypher.QueryCachePolicy{MaxEntries: 0})
				query, wantKind, count := "CREATE (:Forwarded {id:'one'})", CmdCreateNode, int64(1)
				if bulk {
					query, wantKind, count = "CREATE (:Forwarded {id:'one'}), (:Forwarded {id:'two'})", CmdBulkCreateNodes, 2
				}
				_, err = exec.Execute(context.Background(), query, nil)
				if veto {
					require.ErrorIs(t, err, rejected)
					count = 0
					require.Equal(t, before, mirror.calls.Load())
				} else {
					require.NoError(t, err)
					require.Equal(t, before+1, mirror.calls.Load())
				}
				require.NoError(t, callbackErr)
				require.EqualValues(t, 1, calls.Load())
				require.Equal(t, []CommandType{wantKind}, transport.kinds)
				leaderView, err := leaderManager.GetStorage("forwarded")
				require.NoError(t, err)
				result, err := cypher.NewStorageExecutor(leaderView, cypher.QueryCachePolicy{MaxEntries: 0}).Execute(context.Background(), "MATCH (n:Forwarded) RETURN count(n)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{count}}, result.Rows)
			})
		}
	}
}
