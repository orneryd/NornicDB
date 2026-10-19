package replication

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type commandMatrixReplicator struct {
	Replicator
	adapter   *StorageAdapter
	outcome   string
	pending   *Command
	priorDone chan error
}

func (r *commandMatrixReplicator) Apply(cmd *Command, timeout time.Duration) error {
	outcome := r.outcome
	r.outcome = ""
	switch outcome {
	case "reject":
		return ErrNotLeader
	case "pending", "spare":
		r.pending = cmd
		return context.DeadlineExceeded
	case "applied-timeout":
		if err := r.Replicator.Apply(cmd, timeout); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}
	if r.pending != nil {
		prior := r.pending
		r.pending = nil
		r.priorDone = make(chan error, 1)
		go func() { r.priorDone <- r.adapter.ApplyCommand(prior) }()
		select {
		case err := <-r.priorDone:
			if err != nil {
				return err
			}
		case <-time.After(2 * time.Second):
			return fmt.Errorf("new proposal held admission needed by its consensus predecessor")
		}
	}
	return r.Replicator.Apply(cmd, timeout)
}

func commandMatrixWrite(ctx context.Context, view storage.Engine, exec *cypher.StorageExecutor, route, batch, payload string) error {
	node := func(id string) *storage.Node {
		return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"batch": batch, "payload": payload}}
	}
	edge := func(id string) *storage.Edge {
		return &storage.Edge{ID: storage.EdgeID(id), StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"batch": batch, "payload": payload}}
	}
	switch route {
	case "node":
		_, err := view.CreateNode(node(batch))
		return err
	case "edge":
		return view.CreateEdge(edge(batch))
	case "bulk-node":
		return view.BulkCreateNodes([]*storage.Node{node(batch + "-1"), node(batch + "-2")})
	case "bulk-edge":
		return view.BulkCreateEdges([]*storage.Edge{edge(batch + "-1"), edge(batch + "-2")})
	case "implicit":
		_, err := exec.Execute(ctx, "UNWIND [1,2] AS k CREATE (:Doc {batch:$batch, k:k, payload:$payload}) RETURN count(*)", map[string]any{"batch": batch, "payload": payload})
		return err
	case "explicit":
		if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
			return err
		}
		if _, err := exec.Execute(ctx, "UNWIND [1,2] AS k CREATE (:Doc {batch:$batch, k:k, payload:$payload}) RETURN count(*)", map[string]any{"batch": batch, "payload": payload}); err != nil {
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			return err
		}
		_, err := exec.Execute(ctx, "COMMIT", nil)
		return err
	case "raw":
		var tx *storage.BadgerTransaction
		for engine := view; engine != nil; {
			if beginner, ok := engine.(interface {
				BeginTransaction() (*storage.BadgerTransaction, error)
			}); ok {
				var err error
				tx, err = beginner.BeginTransaction()
				if err != nil {
					return err
				}
				break
			}
			wrapper, ok := engine.(storage.EngineUnwrapper)
			if !ok {
				return storage.ErrNotImplemented
			}
			engine = wrapper.GetInnerEngine()
		}
		defer tx.Rollback()
		if err := tx.SetNamespace("command_matrix"); err != nil {
			return err
		}
		for _, id := range []string{batch + "-1", batch + "-2"} {
			n := node("command_matrix:" + id)
			if _, err := tx.CreateNode(n); err != nil {
				return err
			}
		}
		return view.(interface {
			CommitTransaction(context.Context, *storage.BadgerTransaction) error
		}).CommitTransaction(ctx, tx)
	default:
		return storage.ErrNotImplemented
	}
}

func TestManagedCommandReservationMatrix(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, buffered := range []bool{false, true} {
		for _, route := range []string{"node", "edge", "bulk-node", "bulk-edge", "implicit", "explicit", "raw"} {
			for _, quota := range []string{"count", "bytes"} {
				for _, outcome := range []string{"reject", "pending", "applied-timeout", "spare"} {
					t.Run(fmt.Sprintf("buffered=%v/%s/%s/%s", buffered, route, quota, outcome), func(t *testing.T) {
						base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
						require.NoError(t, err)
						dir := t.TempDir()
						wal, err := storage.NewWAL(dir, storage.DefaultWALConfig())
						require.NoError(t, err)
						journal := storage.NewWALEngine(base, wal)
						adapter, err := NewStorageAdapterWithWAL(journal, t.TempDir())
						require.NoError(t, err)
						defer adapter.Close()
						local := NewStandaloneReplicator(DefaultConfig(), adapter)
						require.NoError(t, local.Start(context.Background()))
						defer local.Shutdown()
						router := &commandMatrixReplicator{Replicator: local, adapter: adapter}
						manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(journal, router, time.Second), nil)
						require.NoError(t, err)
						defer manager.Close()
						require.NoError(t, manager.CreateDatabase("command_matrix"))
						view, err := manager.GetStorage("command_matrix")
						require.NoError(t, err)
						require.NoError(t, view.BulkCreateNodes([]*storage.Node{{ID: "a", Labels: []string{"Identity"}}, {ID: "b", Labels: []string{"Identity"}}}))
						exec := cypher.NewStorageExecutor(view)
						ctx := context.Background()
						baseline, _, _ := manager.GetStorageSize("command_matrix")
						payload := strings.Repeat("p", 1024)
						require.NoError(t, commandMatrixWrite(ctx, view, exec, route, "one", payload))
						capacity, _, _ := manager.GetStorageSize("command_matrix")
						nodes, err := view.NodeCount()
						require.NoError(t, err)
						edges, err := view.EdgeCount()
						require.NoError(t, err)
						_, err = exec.Execute(ctx, "MATCH (n:Doc) DETACH DELETE n", nil)
						require.NoError(t, err)
						_, err = exec.Execute(ctx, "MATCH ()-[r:LINK]->() DELETE r", nil)
						require.NoError(t, err)
						actual, _, _ := manager.GetStorageSize("command_matrix")
						require.Equal(t, baseline, actual)
						multiplier := int64(1)
						if outcome == "spare" {
							multiplier = 2
						}
						limits := &multidb.Limits{}
						if quota == "bytes" {
							limits.Storage.MaxBytes = baseline + multiplier*(capacity-baseline)
						} else if edges > 0 {
							limits.Storage.MaxEdges = multiplier * edges
						} else {
							limits.Storage.MaxNodes = 2 + multiplier*(nodes-2)
						}
						require.NoError(t, manager.SetDatabaseLimits("command_matrix", limits))
						router.outcome = outcome
						err = commandMatrixWrite(ctx, view, exec, route, "one", payload)
						if outcome == "reject" {
							require.ErrorIs(t, err, ErrNotLeader)
						} else {
							require.ErrorIs(t, err, context.DeadlineExceeded)
						}
						second := commandMatrixWrite(ctx, view, exec, route, "two", payload)
						if outcome == "reject" || outcome == "spare" {
							require.NoError(t, second)
						} else {
							require.ErrorIs(t, second, multidb.ErrStorageLimitExceeded, "uncertain commands cannot donate their reserved capacity")
							if router.pending != nil {
								pending := router.pending
								router.pending = nil
								require.NoError(t, adapter.ApplyCommand(pending))
							}
						}
						batch := "one"
						if outcome == "reject" {
							batch = "two"
						}
						query := "MATCH (n:Doc {batch:$batch}) SET n.payload=$payload RETURN count(n)"
						if strings.Contains(route, "edge") {
							query = "MATCH ()-[n:LINK {batch:$batch}]->() SET n.payload=$payload RETURN count(n)"
						}
						_, err = exec.Execute(ctx, query, map[string]any{"batch": batch, "payload": payload})
						require.NoError(t, err, "unchanged retry after outcome resolution")
						tracked, _, _ := manager.GetStorageSize("command_matrix")
						recount, err := multidb.NewDatabaseManager(journal, nil)
						require.NoError(t, err)
						defer recount.Close()
						exact, _, _ := recount.GetStorageSize("command_matrix")
						require.Equal(t, exact, tracked)
						if limits.Storage.MaxBytes > 0 {
							require.LessOrEqual(t, tracked, limits.Storage.MaxBytes)
						}
						require.NoError(t, wal.Sync())
						entries, err := storage.ReadWALEntriesFromDir(dir)
						require.NoError(t, err)
						recovered, recoverErr := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true})
						require.NoError(t, recoverErr)
						defer recovered.Close()
						replayResult := storage.ReplayWALEntries(recovered, entries)
						require.False(t, replayResult.HasCriticalErrors(), "%+v", replayResult.Errors)
						if outcome == "reject" {
							check := cypher.NewStorageExecutor(storage.NewNamespacedEngine(recovered, "command_matrix"))
							q := "MATCH (n:Doc {batch:'one'}) RETURN count(n)"
							if strings.Contains(route, "edge") {
								q = "MATCH ()-[n:LINK {batch:'one'}]->() RETURN count(n)"
							}
							result, err := check.Execute(ctx, q, nil)
							require.NoError(t, err)
							require.EqualValues(t, 0, result.Rows[0][0], "rejected command must not recover")
						}
					})
				}
			}
		}
	}
}
