package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestPreparedEntityRejectsPriorSnapshotConflictBeforeAdmission(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "initial"}})
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]any{"value": "prepared"}}, ClientNodeEdits))
	require.NoError(t, b.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "concurrent"}}))
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "logical-1"})
	require.ErrorIs(t, err, ErrConflict)
	require.Nil(t, prepared)
	got, err := b.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "concurrent", got.Properties["value"])
}
func TestPreparedEntityImmutableChangesCommitAndDiscard(t *testing.T) {
	for _, property := range []struct {
		name   string
		value  func(string) any
		mutate func(any)
	}{
		{name: "scalar", value: func(value string) any { return value }},
		{name: "typed_list", value: func(value string) any {
			if value == "initial" {
				return []int64{1, 2}
			}
			return []int64{3, 4}
		}, mutate: func(value any) { value.([]int64)[0] = 999 }},
	} {
		for _, commit := range []bool{false, true} {
			t.Run(property.name+"/"+map[bool]string{false: "discard", true: "commit"}[commit], func(t *testing.T) {
				b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
				require.NoError(t, err)
				_, err = b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"value": property.value("initial")}})
				require.NoError(t, err)
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]any{"value": property.value("prepared")}}, ClientNodeEdits))
				prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "logical-1"})
				require.NoError(t, err)
				defer prepared.Abort()
				require.Equal(t, "test", prepared.Namespace())
				require.Equal(t, "logical-1", prepared.LogicalID())
				operations := prepared.Operations()
				require.Len(t, operations, 1)
				require.NotNil(t, operations[0].OldNode)
				if property.mutate != nil {
					property.mutate(operations[0].OldNode.Properties["value"])
					property.mutate(operations[0].Node.Properties["value"])
					require.Equal(t, property.value("initial"), prepared.Operations()[0].OldNode.Properties["value"])
					require.Equal(t, property.value("prepared"), prepared.Operations()[0].Node.Properties["value"])
				}
				operations[0].OldNode.Properties["value"] = "caller changed before-image"
				require.Equal(t, property.value("initial"), prepared.Operations()[0].OldNode.Properties["value"], "public operation images must be detached from accepted ownership")
				payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
				require.NoError(t, err)
				copiedPayload, err := EncodeEntityTransaction(EntityTransactionRequest{
					Namespace: prepared.Namespace(), LogicalID: prepared.LogicalID(),
					Operations: prepared.Operations(), Mode: prepared.Mode(),
				})
				require.NoError(t, err)
				require.Equal(t, copiedPayload, payload)
				_, err = EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared, Namespace: "conflicting"})
				require.ErrorIs(t, err, ErrInvalidData)
				var encoded []byte
				privateAllocs := testing.AllocsPerRun(10, func() {
					var encodeErr error
					encoded, encodeErr = EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
					require.NoError(t, encodeErr)
				})
				publicAllocs := testing.AllocsPerRun(10, func() {
					var encodeErr error
					encoded, encodeErr = EncodeEntityTransaction(EntityTransactionRequest{
						Namespace: prepared.Namespace(), LogicalID: prepared.LogicalID(),
						Operations: prepared.Operations(), Mode: prepared.Mode(),
					})
					require.NoError(t, encodeErr)
				})
				require.NotEmpty(t, encoded)
				require.Less(t, privateAllocs, publicAllocs, "canonical encoding must avoid the detached diagnostic copy")
				t.Logf("prepared encoding allocations: private %.0f, diagnostic-copy %.0f", privateAllocs, publicAllocs)
				changes := prepared.Changes()
				require.Len(t, changes, 1)
				require.Equal(t, property.value("initial"), changes[0].BeforeNode.Properties["value"])
				require.Equal(t, property.value("prepared"), changes[0].AfterNode.Properties["value"])
				changes[0].AfterNode.Properties["value"] = "caller mutation"
				require.Equal(t, property.value("prepared"), prepared.Changes()[0].AfterNode.Properties["value"])
				published := false
				if commit {
					require.NoError(t, prepared.Commit(func() { published = true }))
				} else {
					prepared.Abort()
				}
				require.Equal(t, commit, published)
				got, err := b.GetStoredNode("test:a")
				require.NoError(t, err)
				want := property.value("initial")
				if commit {
					want = property.value("prepared")
				}
				require.Equal(t, want, got.Properties["value"])
			})
		}
	}
}

func TestPreparedEntityRetainsAuthorityAgainstOrdinaryDirectUpdate(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "initial"}})
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]any{"value": "prepared"}}, ClientNodeEdits))
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "logical-1"})
	require.NoError(t, err)
	defer prepared.Abort()
	waiting := make(chan struct{}, 1)
	restore := SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	})
	defer restore()
	done := make(chan error, 1)
	go func() { done <- b.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "concurrent"}}) }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary writer did not join canonical prepared authority")
	}
	select {
	case err := <-done:
		t.Fatalf("writer bypassed prepared authority: %v", err)
	default:
	}
	// Disjoint direct writes still complete while the prepared node is reserved.
	_, err = b.CreateNode(&Node{ID: "test:disjoint", Properties: map[string]any{"value": "other"}})
	require.NoError(t, err)
	require.NoError(t, prepared.Commit(nil))
	require.NoError(t, <-done)
	got, err := b.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "concurrent", got.Properties["value"])
}

func TestPreparedEntityTimeoutCleanupDoesNotWaitOrDiscard(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:late", Properties: map[string]any{"value": "accepted"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "logical-late"})
	require.NoError(t, err)
	defer prepared.Abort()
	receipt, resolve := NewCommitReceipt(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitErr := receipt.Wait(ctx)
	var accepted *AcceptedCommitError
	require.ErrorAs(t, waitErr, &accepted)
	require.Equal(t, CommitRecoveryPending, accepted.State)
	require.NotErrorIs(t, waitErr, context.Canceled)
	require.ErrorIs(t, accepted.Cause(), context.Canceled)
	cleanup := make(chan error, 1)
	go func() { cleanup <- tx.Rollback() }()
	select {
	case err := <-cleanup:
		require.ErrorIs(t, err, ErrTransactionClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("frontend cleanup waited for accepted work")
	}
	require.False(t, tx.IsActive())
	require.ErrorIs(t, tx.UpdateGraphNode(&Node{ID: "test:late"}, ClientNodeEdits), ErrTransactionClosed)
	require.NoError(t, prepared.Commit(nil))
	require.NoError(t, resolve.Resolve(CommitOutcome{State: CommitApplied}))
	require.NoError(t, receipt.Wait(context.Background()))
	got, err := b.GetStoredNode("test:late")
	require.NoError(t, err)
	require.Equal(t, "accepted", got.Properties["value"])
}

func TestNativeSubmitReceiptTimeoutAndPublicCleanup(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:native", Properties: map[string]any{"value": "accepted"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "logical-native"})
	require.NoError(t, err)
	defer prepared.Abort()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	restore := setCommitTailHook(func() { close(entered); <-release })
	defer restore()
	owner, ok := EntityCommitSubmitterFor(b)
	require.True(t, ok)
	receipt, err := owner.Submitter.SubmitEntityCommit(context.Background(), prepared, nil)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native accepted commit did not reach paused tail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitErr := receipt.Wait(ctx)
	var accepted *AcceptedCommitError
	require.ErrorAs(t, waitErr, &accepted)
	require.Equal(t, CommitRecoveryPending, accepted.State)
	require.NotErrorIs(t, waitErr, context.Canceled)
	require.ErrorIs(t, accepted.Cause(), context.Canceled)
	cleanup := make(chan error, 1)
	go func() { cleanup <- tx.Rollback() }()
	select {
	case err := <-cleanup:
		require.ErrorIs(t, err, ErrTransactionClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("public cleanup blocked behind native accepted continuation")
	}
	once.Do(func() { close(release) })
	require.NoError(t, receipt.Wait(context.Background()))
	require.Equal(t, CommitApplied, receipt.Outcome().State)
	got, err := b.GetStoredNode("test:native")
	require.NoError(t, err)
	require.Equal(t, "accepted", got.Properties["value"])
}

// Exercise every mutable property-container form accepted by the prepared wire
// owner through Operations, including before-images and embedding metadata.
func TestPreparedDiagnosticPropertyContainers(t *testing.T) {
	values := func() map[string]any {
		return map[string]any{
			"bytes": []byte{1}, "strings": []string{"one"}, "ints": []int{1},
			"int32s": []int32{1}, "int64s": []int64{1}, "float32s": []float32{1},
			"float64s": []float64{1}, "bools": []bool{true},
			"nested":   []any{map[string]any{"list": []int64{1}}},
			"nil_list": []any(nil), "nil_map": map[string]any(nil),
			"empty_list": []any{}, "empty_map": map[string]any{},
		}
	}
	original := values()
	p := &PreparedEntityCommit{operations: []Operation{{
		Node: &Node{Properties: original, EmbedMeta: original}, OldNode: &Node{Properties: original, EmbedMeta: original},
		Edge: &Edge{Properties: original}, OldEdge: &Edge{Properties: original},
	}}}
	op := p.Operations()[0]
	for _, properties := range []map[string]any{op.Node.Properties, op.Node.EmbedMeta, op.OldNode.Properties, op.OldNode.EmbedMeta, op.Edge.Properties, op.OldEdge.Properties} {
		require.Equal(t, values(), properties)
		properties["nested"].([]any)[0].(map[string]any)["list"].([]int64)[0] = 99
		for _, value := range properties {
			list := reflect.ValueOf(value)
			if list.Kind() == reflect.Slice && list.Len() > 0 {
				list.Index(0).Set(reflect.Zero(list.Type().Elem()))
			}
		}
		properties["extra"] = true
		require.Equal(t, values(), original, "diagnostic mutation changed the accepted snapshot")
	}
	require.Equal(t, values(), p.Operations()[0].Node.Properties)
}
