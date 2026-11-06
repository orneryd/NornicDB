package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

type walMutationOperation struct {
	name string
	run  func(context.Context, Engine) error
}

func walMutationOperations() []walMutationOperation {
	return []walMutationOperation{
		{"CreateNode", func(ctx context.Context, e Engine) error {
			_, err := e.CreateNode(ctx, &Node{ID: "n1", Labels: []string{"Person"}})
			return err
		}},
		{"UpdateNode", func(ctx context.Context, e Engine) error {
			return e.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "n1", Properties: map[string]interface{}{"x": 1}}})
		}},
		{"DeleteNode", func(ctx context.Context, e Engine) error { return e.DeleteNode(ctx, "n1") }},
		{"CreateEdge", func(ctx context.Context, e Engine) error {
			return e.CreateEdge(ctx, &Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "KNOWS"})
		}},
		{"UpdateEdge", func(ctx context.Context, e Engine) error {
			return e.UpdateEdge(ctx, EdgeUpdateRequest{After: &Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "KNOWS"}})
		}},
		{"DeleteEdge", func(ctx context.Context, e Engine) error { return e.DeleteEdge(ctx, "e1") }},
		{"BulkCreateNodes", func(ctx context.Context, e Engine) error {
			return e.BulkCreateNodes(ctx, []*Node{{ID: "n2", Labels: []string{"Person"}}})
		}},
		{"BulkCreateEdges", func(ctx context.Context, e Engine) error {
			return e.BulkCreateEdges(ctx, []*Edge{{ID: "e2", StartNode: "a", EndNode: "b", Type: "KNOWS"}})
		}},
		{"BulkDeleteNodes", func(ctx context.Context, e Engine) error {
			return e.BulkDeleteNodes(ctx, []NodeID{"n2"})
		}},
		{"BulkDeleteEdges", func(ctx context.Context, e Engine) error {
			return e.BulkDeleteEdges(ctx, []EdgeID{"e2"})
		}},
	}
}

// The issued runner, not any transparent decorator, owns acceptance. Exercise
// every consumer of beginNativeMutation, including both issued-view factories.
func TestWALStatementOwnedMutations(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	operations := walMutationOperations()
	for _, factory := range []string{"new", "with_runner"} {
		for _, chain := range []string{"w", "tw", "wt", "ww", "tww", "wtw", "wwt"} {
			t.Run(factory+"/"+chain, func(t *testing.T) {
				owner := &struct{ name string }{t.Name()}
				calls := 0
				var runnerErr error
				type contextKey struct{}
				ctx := context.WithValue(t.Context(), contextKey{}, owner)
				run := func(ctx context.Context, query string, params map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error) {
					calls++
					require.Same(t, owner, ctx.Value(contextKey{}), "caller values reach the authority")
					require.Nil(t, ctx.Value(nativeWALKey{}), "an issued operation carries no independent native journal authority")
					require.Equal(t, RemoteAccessWrite, mode)
					if runnerErr != nil {
						return nil, nil, runnerErr
					}
					return remoteEngineMethodReply(t, query, params)
				}
				remote := NewStatementRemoteEngine(owner, run)
				if factory == "with_runner" {
					remote = (&RemoteEngine{schema: NewSchemaManager()}).WithStatementRunner(owner, run)
				}
				var view Engine = remote
				var journals []*WAL
				for _, kind := range chain {
					if kind == 't' {
						view = NewTracedEngine(view)
						continue
					}
					wal, err := NewWAL(t.TempDir(), nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, wal.Close()) })
					decorator := NewWALEngine(view, wal)
					t.Cleanup(func() {
						decorator.work.BeginStop(nil)
						require.NoError(t, decorator.work.JoinStopped())
					})
					view = decorator
					journals = append(journals, wal)
				}
				for _, operation := range operations {
					before := calls
					runnerErr = nil
					require.NoError(t, operation.run(ctx, view), operation.name)
					require.Greater(t, calls, before, "%s: the owner executes the write", operation.name)
					before = calls
					runnerErr = errors.New("statement owner rejected the write")
					require.ErrorIs(t, operation.run(ctx, view), runnerErr, operation.name)
					require.Greater(t, calls, before, "%s: the owner's rejection is authoritative", operation.name)
					for _, wal := range journals {
						require.Zero(t, wal.Sequence(), "decorators never journal pending owned effects")
						entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
						require.NoError(t, err)
						require.Empty(t, entries, "neither accepted nor rejected owned operations enter a decorator's replay log")
					}
				}
			})
		}
	}
}

func TestWALUnownedRemoteHasNoNativePublicationAuthority(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	calls := 0
	remote := NewStatementRemoteEngine(nil, func(context.Context, string, map[string]interface{}, RemoteAccessMode) ([]string, [][]interface{}, error) {
		calls++
		return nil, nil, nil
	})
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	view := NewWALEngine(NewTracedEngine(remote), wal)
	t.Cleanup(func() {
		view.work.BeginStop(nil)
		require.NoError(t, view.work.JoinStopped())
	})
	for _, operation := range walMutationOperations() {
		require.ErrorIs(t, operation.run(t.Context(), view), ErrInvalidData, operation.name+": remote storage without an issuing owner is not a native WAL publication route")
		require.True(t, view.mutationMu.TryLock(), "refused admission releases its lease")
		view.mutationMu.Unlock()
	}
	require.Zero(t, calls, "unsupported native routing cannot execute an unowned remote write")
	require.Zero(t, wal.Sequence())
}

// Admission's nil context result is an error-only result, never an operation
// context. Every public consumer must stop there, before native/issued dispatch.
func TestWALMutationCanceledAdmissionStopsEveryConsumer(t *testing.T) {
	for _, kind := range []string{"native", "issued", "disabled"} {
		t.Run(kind, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			calls := 0
			var inner Engine
			if kind == "native" {
				inner = newTestMemoryEngine(t)
			} else {
				inner = NewStatementRemoteEngine(t, func(_ context.Context, query string, params map[string]interface{}, _ RemoteAccessMode) ([]string, [][]interface{}, error) {
					calls++
					return remoteEngineMethodReply(t, query, params)
				})
			}
			if kind == "disabled" {
				t.Cleanup(config.WithWALDisabled())
			}
			wal, err := NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wal.Close()) })
			view := NewWALEngine(inner, wal)
			t.Cleanup(func() {
				view.work.BeginStop(nil)
				require.NoError(t, view.work.JoinStopped())
			})
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			for _, operation := range walMutationOperations() {
				require.ErrorIs(t, operation.run(ctx, view), context.Canceled, operation.name)
				require.True(t, view.mutationMu.TryLock(), "canceled admission retains no lease")
				view.mutationMu.Unlock()
			}
			require.Zero(t, calls, "failed admission cannot dispatch a remote operation")
			require.Zero(t, wal.Sequence())
			if kind == "native" {
				count, err := inner.NodeCount(t.Context())
				require.NoError(t, err)
				require.Zero(t, count)
				count, err = inner.EdgeCount(t.Context())
				require.NoError(t, err)
				require.Zero(t, count)
			}
		})
	}
}
