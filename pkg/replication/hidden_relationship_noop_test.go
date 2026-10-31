package replication_test

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type hiddenNoOpFanout struct {
	mu       sync.Mutex
	targets  []*replication.StorageAdapter
	accepted *replication.StandaloneReplicator
}

// Use the real accepted owner while preserving the fixture's Apply fanout.
type hiddenNoOpFanoutStorage struct {
	replication.Storage
	fanout *hiddenNoOpFanout
}

func (s *hiddenNoOpFanoutStorage) ApplyCommand(cmd *replication.Command) error {
	return s.fanout.Apply(cmd, 0)
}
func (r *hiddenNoOpFanout) SubmitAcceptedCommand(ctx context.Context, cmd *replication.Command, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	r.mu.Lock()
	if r.accepted == nil {
		r.accepted = replication.NewStandaloneReplicator(&replication.Config{}, &hiddenNoOpFanoutStorage{Storage: r.targets[0], fanout: r})
		if err := r.accepted.Start(context.Background()); err != nil {
			r.mu.Unlock()
			return nil, err
		}
	}
	accepted := r.accepted
	r.mu.Unlock()
	return accepted.SubmitAcceptedCommand(ctx, cmd, settle)
}

func (r *hiddenNoOpFanout) Start(context.Context) error { return nil }
func (r *hiddenNoOpFanout) Apply(command *replication.Command, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, target := range r.targets {
		if err := target.ApplyCommand(command); err != nil {
			return err
		}
	}
	return nil
}
func (r *hiddenNoOpFanout) ApplyBatch(commands []*replication.Command, timeout time.Duration) error {
	for _, command := range commands {
		if err := r.Apply(command, timeout); err != nil {
			return err
		}
	}
	return nil
}
func (r *hiddenNoOpFanout) IsLeader() bool     { return true }
func (r *hiddenNoOpFanout) LeaderAddr() string { return "leader" }
func (r *hiddenNoOpFanout) LeaderID() string   { return "leader" }
func (r *hiddenNoOpFanout) Health() *replication.HealthStatus {
	return &replication.HealthStatus{IsLeader: true}
}
func (r *hiddenNoOpFanout) WaitForLeader(context.Context) error { return nil }
func (r *hiddenNoOpFanout) Shutdown() error                     { return nil }
func (r *hiddenNoOpFanout) Mode() replication.ReplicationMode   { return replication.ModeStandalone }
func (r *hiddenNoOpFanout) NodeID() string                      { return "leader" }

func TestRepeatedHiddenRelationshipMergeKeepsFollowerConverged(t *testing.T) {
	previousParser := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previousParser) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, buffered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/write-behind=%v", parser, buffered), func(t *testing.T) {
				config.SetParserType(parser)
				var bases [2]*storage.BadgerEngine
				for i := range bases {
					var err error
					bases[i], err = runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
						DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour,
					}))
					require.NoError(t, err)
				}
				leaderAdapter, err := replication.NewStorageAdapterWithWAL(bases[0], t.TempDir())
				require.NoError(t, err)
				followerAdapter, err := replication.NewStorageAdapterWithWAL(bases[1], t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { _ = leaderAdapter.Close(); _ = followerAdapter.Close() })
				fanout := &hiddenNoOpFanout{targets: []*replication.StorageAdapter{leaderAdapter, followerAdapter}}
				replicated := replication.NewReplicatedEngine(bases[0], fanout, time.Second)
				manager, err := multidb.NewDatabaseManager(t.Context(), replicated, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = manager.Close() })
				managed, err := manager.GetStorage("nornic")
				require.NoError(t, err)
				exec := cypher.NewStorageExecutor(managed)
				ctx := context.Background()
				_, err = managed.CreateNode(&storage.Node{
					ID: "a", Labels: []string{"A"}, Properties: map[string]any{"id": "a"},
				})
				require.NoError(t, err)
				_, err = managed.CreateNode(&storage.Node{
					ID: "b", Labels: []string{"B"}, Properties: map[string]any{"id": "b"},
				})
				require.NoError(t, err)
				const merge = "MATCH (a:A), (b:B) MERGE (a)-[r:LINK {key:'same'}]->(b)"
				_, err = exec.Execute(ctx, merge+" ON CREATE SET r.payload='stored'", nil)
				require.NoError(t, err)
				require.NoError(t, bases[0].FlushWriteBehind(nil))
				leaderEdges, err := storage.CollectStoredEdges(ctx, bases[0])
				require.NoError(t, err)
				require.Len(t, leaderEdges, 1)
				fullID := leaderEdges[0].ID
				followerEdges, err := storage.CollectStoredEdges(ctx, bases[1])
				require.NoError(t, err)
				if len(followerEdges) == 0 {
					require.NoError(t, bases[1].CreateEdge(storage.CopyEdge(leaderEdges[0])))
				}
				for _, base := range bases {
					require.NoError(t, base.FlushWriteBehind(nil))
					_, err = base.GetStoredEdge(fullID)
					require.NoError(t, err)
					require.NoError(t, base.SetManualSuppression(storage.StoredEntityEdgeKind, string(fullID), true))
				}

				result, err := exec.Execute(ctx, "UNWIND [1,2] AS i "+merge+
					" ON CREATE SET r.payload='stored' ON MATCH SET r.payload='MATCH-FIRED'", nil)
				require.NoError(t, err)
				require.Zero(t, result.Stats.RelationshipsCreated)
				require.Zero(t, result.Stats.PropertiesSet)

				for _, base := range bases {
					require.NoError(t, base.FlushWriteBehind(nil))
					edge, err := base.GetStoredEdge(fullID)
					require.NoError(t, err)
					require.Equal(t, map[string]any{"key": "same", "payload": "stored"}, edge.Properties)
					_, err = base.GetEdge(fullID)
					require.ErrorIs(t, err, storage.ErrNotFound)
					count, err := storage.NewNamespacedEngine(base, "nornic").EdgeCount()
					require.NoError(t, err)
					require.EqualValues(t, 1, count)
				}
			})
		}
	}
}
