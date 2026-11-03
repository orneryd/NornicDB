package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommitPreparationDiscoveryAdmissionCancellation(t *testing.T) {
	for _, name := range []string{"unique", "flag", "contracts"} {
		t.Run(name, func(t *testing.T) {
			sm := NewSchemaManager()
			require.NoError(t, sm.AddUniqueConstraint("key", "Doc", "key"))
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct {
				value any
				err   error
			}, 1)
			joined := make(chan struct{})
			entered := make(chan struct{}, 1)
			t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
				if owner == &sm.mu {
					select {
					case entered <- struct{}{}:
					default:
					}
				}
			}))
			sm.mu.Lock()
			var once sync.Once
			unlock := func() { once.Do(sm.mu.Unlock) }
			t.Cleanup(func() {
				cancel()
				unlock()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("metadata reader did not join")
				}
			})
			go func() {
				defer close(joined)
				var value any
				var err error
				switch name {
				case "unique":
					value, err = sm.GetConstraints(ctx)
				case "flag":
					value, err = sm.HasAnyConstraintContract(ctx)
				case "contracts":
					value, err = sm.GetConstraintContractsForTarget(ctx, ConstraintEntityNode, "Doc")
				}
				done <- struct {
					value any
					err   error
				}{value, err}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("reader did not reach held metadata admission")
			}
			cancel()
			select {
			case result := <-done:
				require.ErrorIs(t, result.err, context.Canceled)
				require.Zero(t, result.value)
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation did not release metadata admission")
			}
			unlock()
			constraints, err := sm.GetConstraints(t.Context())
			require.NoError(t, err)
			require.Len(t, constraints, 1)
			flag, err := sm.HasAnyConstraintContract(t.Context())
			require.NoError(t, err)
			require.False(t, flag)
			contracts, err := sm.GetConstraintContractsForTarget(t.Context(), ConstraintEntityNode, "Doc")
			require.NoError(t, err)
			require.Empty(t, contracts)
		})
	}
}

func TestCommitPreparationPublicMetadataAdmissionCancellation(t *testing.T) {
	for _, plain := range []bool{false, true} {
		t.Run(map[bool]string{false: "prepare", true: "plain"}[plain], func(t *testing.T) {
			b := newTestEngine(t)
			schema := b.GetSchemaForNamespace("metadata")
			require.NoError(t, schema.AddUniqueConstraint("key", "Doc", "key"))
			_, err := b.CreateNode(t.Context(), &Node{ID: "metadata:n", Labels: []string{"Doc"}, Properties: map[string]any{"key": "n", "payload": "before"}})
			require.NoError(t, err)
			tx, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			node, err := tx.GetNode(t.Context(), "metadata:n")
			require.NoError(t, err)
			before := CopyNode(node)
			node.Properties["payload"] = "unaccepted"
			require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}))
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			joined := make(chan struct{})
			entered := make(chan struct{}, 1)
			t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
				if owner == &schema.mu {
					select {
					case entered <- struct{}{}:
					default:
					}
				}
			}))
			schema.mu.Lock()
			var once sync.Once
			unlock := func() { once.Do(schema.mu.Unlock) }
			t.Cleanup(func() {
				cancel()
				unlock()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("public preparation did not join")
				}
			})
			go func() {
				defer close(joined)
				var err error
				if plain {
					err = tx.CommitContext(ctx)
				} else {
					var p *PreparedEntityCommit
					p, err = tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "metadata-canceled"})
					if p != nil {
						p.Abort()
					}
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("public preparation did not reach metadata admission")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("public preparation ignored metadata cancellation")
			}
			unlock()
			actual, err := b.GetNode(t.Context(), "metadata:n")
			require.NoError(t, err)
			require.Equal(t, "before", actual.Properties["payload"])
			prepareContextRetry(t, b, "metadata:n", "healthy-metadata")
		})
	}
}

func TestCommitPreparationDiscoveryOwnsCloneAndReturnsNoPartial(t *testing.T) {
	sm := NewSchemaManager()
	for i := 0; i < 64; i++ {
		require.NoError(t, sm.AddConstraintContractBundle(ConstraintContract{Name: fmt.Sprintf("c%03d", i), TargetEntityType: "NODE", TargetLabelOrType: "Doc", Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.age > 0", Properties: []string{"age"}}}}, nil, nil, false))
	}
	ctx := newIndexPollCancelContext(t, 16)
	partial, err := sm.GetConstraintContractsForTarget(ctx, ConstraintEntityNode, "Doc")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, partial)
	contracts, err := sm.GetConstraintContractsForTarget(t.Context(), ConstraintEntityNode, "Doc")
	require.NoError(t, err)
	require.Len(t, contracts, 64)
	for i := range contracts {
		require.Equal(t, fmt.Sprintf("c%03d", i), contracts[i].Name)
	}
	contracts[0].Entries[0].Properties[0] = "mutated"
	fresh, err := sm.GetConstraintContractsForTarget(t.Context(), ConstraintEntityNode, "Doc")
	require.NoError(t, err)
	require.Equal(t, "age", fresh[0].Entries[0].Properties[0])
}

func TestCommitPreparationDiscoveryEmptyAllocationBudget(t *testing.T) {
	sm := NewSchemaManager()
	ctx := t.Context()
	require.Zero(t, testing.AllocsPerRun(100, func() { _, err := sm.GetConstraints(ctx); require.NoError(t, err) }))
	require.Zero(t, testing.AllocsPerRun(100, func() { _, err := sm.HasAnyConstraintContract(ctx); require.NoError(t, err) }))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		_, err := sm.GetConstraintContractsForTarget(ctx, ConstraintEntityNode, "Doc")
		require.NoError(t, err)
	}))
}
