package replication

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
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

func (*residualForwardingReplicator) IsLeader() bool { return false }
func (r *residualForwardingReplicator) Apply(ctx context.Context, cmd *Command, timeout time.Duration) error {
	r.kinds = append(r.kinds, cmd.Type)
	wire, err := storage.EncodeCommandPayload(cmd)
	if err != nil {
		return err
	}
	var forwarded Command
	if err := storage.DecodeCommandPayload(wire, &forwarded); err != nil {
		return err
	}
	return r.StandaloneReplicator.Apply(ctx, &forwarded, timeout)
}

type managedForwardingFixture struct {
	leaderManager *multidb.DatabaseManager
	view          storage.Engine
	mirror        *acceptedMirrorStorage
	transport     *residualForwardingReplicator
}

func newManagedForwardingFixture(t *testing.T) *managedForwardingFixture {
	t.Helper()
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
	return &managedForwardingFixture{leaderManager: leaderManager, view: view, mirror: mirror, transport: transport}
}

func TestManagedForwardedPublicationDecision(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		for _, veto := range []bool{false, true} {
			t.Run(fmt.Sprintf("bulk=%v/veto=%v", bulk, veto), func(t *testing.T) {
				fixture := newManagedForwardingFixture(t)
				view, mirror, transport, leaderManager := fixture.view, fixture.mirror, fixture.transport, fixture.leaderManager
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
				// The follower's managed view is the canonical forwarding owner for
				// the resident leaf/bulk API: its local atomic admission is denied
				// (a follower is not the leader), so the unprepared entity input is
				// forwarded and the leader owns admission and publication. Local
				// Cypher writes have no direct-write fallback and require a local
				// atomic owner, so they are not this boundary's input.
				entry := func(id string) *storage.Node {
					return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Forwarded"}, Properties: map[string]any{"id": id}}
				}
				var err error
				count := int64(1)
				if bulk {
					count = 2
					err = view.BulkCreateNodes(t.Context(), []*storage.Node{entry("one"), entry("two")})
				} else {
					_, err = view.CreateNode(t.Context(), entry("one"))
				}
				wantKind := CmdCreateNode
				if bulk {
					wantKind = CmdBulkCreateNodes
				}
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

// Public Cypher keeps a native atomic snapshot; a follower must reject its
// publication instead of silently switching to the managed direct forwarder.
func TestManagedFollowerCypherWritesRejectBeforeForwarding(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
					fixture := newManagedForwardingFixture(t)
					exec := cypher.NewStorageExecutor(fixture.view, cypher.QueryCachePolicy{MaxEntries: 0})
					t.Cleanup(func() {
						if exec.HasActiveTransaction() {
							_, err := exec.Execute(context.Background(), "ROLLBACK", nil)
							require.NoError(t, err)
						}
					})
					before := fixture.mirror.calls.Load()
					if explicit {
						_, err := exec.Execute(t.Context(), "BEGIN", nil)
						require.NoError(t, err)
					}
					result, err := exec.Execute(t.Context(), "CREATE (:Forwarded {id:'public-cypher'})", nil)
					if explicit {
						require.NoError(t, err, "explicit writes stage in their original snapshot")
						require.NotNil(t, result)
						require.True(t, exec.HasActiveTransaction())
						result, err = exec.Execute(t.Context(), "COMMIT", nil)
					}
					require.ErrorIs(t, err, ErrNotLeader)
					require.Nil(t, result)
					require.False(t, exec.HasActiveTransaction(), "rejected native publication closes its snapshot")
					require.Empty(t, fixture.transport.kinds, "Cypher cannot fall back to direct forwarding")
					require.Equal(t, before, fixture.mirror.calls.Load(), "no accepted replication command")
					leaderView, readErr := fixture.leaderManager.GetStorage("forwarded")
					require.NoError(t, readErr)
					for _, view := range []storage.Engine{leaderView, fixture.view} {
						nodes, readErr := storage.GetStoredNodesByLabel(t.Context(), view, "Forwarded")
						require.NoError(t, readErr)
						require.Empty(t, nodes, "no leader or follower graph writes")
					}
				})
			}
		})
	}
}
