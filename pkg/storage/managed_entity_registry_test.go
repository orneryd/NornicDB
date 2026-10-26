package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManagedEntityRegistryPreservesObserverAndAdmission(t *testing.T) {
	b := newTestEngine(t)
	var observed atomic.Int32
	var admission atomic.Int32
	rejected := errors.New("managed admission rejected")
	callback := func(ctx context.Context, ops []Operation, mode EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
		admission.Add(1)
		require.Equal(t, []Operation{{Type: OpDeleteNode, NodeID: "test:a"}}, ops)
		return nil, nil, rejected
	}
	b.RegisterManagedEntityPreparer("test", callback, nil)
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) { return func() { observed.Add(1) }, nil }, nil
	}, nil))
	_, err := b.CreateNode(&Node{ID: "test:a"})
	require.NoError(t, err)
	require.Equal(t, int32(1), observed.Load())
	wrapped := NewNamespacedEngine(b, "test")
	_, _, handled, err := PrepareManagedEntityChanges(context.Background(), wrapped, "test", []Operation{{Type: OpDeleteNode, NodeID: "test:a"}}, CommitModeDirect)
	require.True(t, handled, "registered rejection cannot fall back to unadmitted native apply")
	require.ErrorIs(t, err, rejected)
	_, _, handled, err = PrepareManagedEntityChanges(context.Background(), wrapped, "other", nil, CommitModeDirect)
	require.False(t, handled)
	require.NoError(t, err)
	require.Equal(t, int32(1), admission.Load())
	b.RegisterManagedEntityPreparer("test", callback, nil)
	require.NoError(t, b.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "after"}}))
	require.Equal(t, int32(2), observed.Load(), "preparer registration preserves installed observer")
}

func TestManagedEntityRegistrationDoesNotWaitForInitializationCut(t *testing.T) {
	b := newTestEngine(t)
	entered := make(chan struct{})
	registered := make(chan struct{})
	finished := make(chan error, 1)
	var once sync.Once
	release := func() { once.Do(func() { close(registered) }) }
	t.Cleanup(func() {
		release()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("initialization worker did not finish")
		}
	})
	go func() {
		finished <- b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
			close(entered)
			<-registered
			return func(StoredEntityPublication) (func(), error) { return func() {}, nil }, nil
		}, nil)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("initializer did not enter exclusive snapshot cut")
	}
	called := false
	b.RegisterManagedEntityPreparer("test", func(context.Context, []Operation, EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
		called = true
		return nil, nil, nil
	}, nil)
	release()
	select {
	case err := <-finished:
		finished <- err
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("initializer did not complete")
	}
	_, _, handled, err := PrepareManagedEntityChanges(context.Background(), b, "test", nil, CommitModeDirect)
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, called, "observer install preserves concurrent preparer registration")
}

func TestManagedEntityRuntimeAndPreparerFirstOwner(t *testing.T) {
	b := newTestEngine(t)
	first := &struct{ value string }{"first"}
	second := &struct{ value string }{"second"}
	require.Same(t, first, b.GetOrAttachManagedEntityRuntime("test", first))
	require.Same(t, first, b.GetOrAttachManagedEntityRuntime("test", second))
	require.Same(t, first, b.GetOrAttachManagedEntityRuntime("test", nil))
	require.Nil(t, b.GetOrAttachManagedEntityRuntime("other", nil))
	var prepareFirst, prepareSecond, initFirst, initSecond atomic.Int32
	b.RegisterManagedEntityPreparer("test", func(context.Context, []Operation, EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
		prepareFirst.Add(1)
		return nil, nil, nil
	}, func(context.Context) error { initFirst.Add(1); return nil })
	b.RegisterManagedEntityPreparer("test", func(context.Context, []Operation, EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
		prepareSecond.Add(1)
		return nil, nil, nil
	}, func(context.Context) error { initSecond.Add(1); return nil })
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) { return func() {}, nil }, nil
	}, nil))
	require.Same(t, first, b.GetOrAttachManagedEntityRuntime("test", nil), "observer initialization preserves shared runtime")
	require.NoError(t, b.initializeManagedEntityNamespace(context.Background(), "test"))
	_, _, handled, err := PrepareManagedEntityChanges(context.Background(), b, "test", nil, CommitModeDirect)
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, int32(1), prepareFirst.Load())
	require.Zero(t, prepareSecond.Load())
	require.Equal(t, int32(1), initFirst.Load())
	require.Zero(t, initSecond.Load())
}

type registryUsageRuntime struct {
	resets atomic.Int32
	policy string
}

func (r *registryUsageRuntime) ResetManagedEntityUsage(ManagedEntityUsageTransition) { r.resets.Add(1) }

func TestManagedEntityRuntimeResetRetainsBindingUntilCatalogRetirement(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a"})
	require.NoError(t, err)
	runtime := &registryUsageRuntime{policy: "keep-policy"}
	require.Same(t, runtime, b.GetOrAttachManagedEntityRuntime("test", runtime))
	b.RegisterManagedEntityPreparer("test", func(context.Context, []Operation, EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
		return nil, nil, nil
	}, func(context.Context) error { return nil })
	_, _, err = b.DeleteByPrefix("test:a:")
	require.NoError(t, err)
	require.Same(t, runtime, b.GetOrAttachManagedEntityRuntime("test", nil))
	require.Zero(t, runtime.resets.Load())
	_, _, err = b.DeleteByPrefix("test:a")
	require.NoError(t, err)
	require.Same(t, runtime, b.GetOrAttachManagedEntityRuntime("test", nil))
	require.Zero(t, runtime.resets.Load())
	listenerSawReset := false
	b.OnNamespaceReset(func(prefix string) {
		if prefix == "test:" {
			listenerSawReset = b.GetOrAttachManagedEntityRuntime("test", nil) == runtime && runtime.resets.Load() == 1
		}
	})
	_, _, err = b.DeleteByPrefix("test:")
	require.NoError(t, err)
	require.True(t, listenerSawReset, "actual usage reset precedes listeners without detaching runtime")
	require.Equal(t, "keep-policy", runtime.policy)
	_, _, handled, err := PrepareManagedEntityChanges(context.Background(), b, "test", nil, CommitModeDirect)
	require.NoError(t, err)
	require.True(t, handled, "graph clear retains admission policy")
	b.RetireManagedEntityRuntime("test")
	require.Same(t, runtime, b.GetOrAttachManagedEntityRuntime("test", nil), "catalog retirement keeps views on canonical object")
	_, _, handled, err = PrepareManagedEntityChanges(context.Background(), b, "test", nil, CommitModeDirect)
	require.NoError(t, err)
	require.False(t, handled, "catalog retirement releases old callback claim")
	require.Same(t, runtime, b.GetOrAttachManagedEntityRuntime("test", &registryUsageRuntime{policy: "candidate"}))
}

func TestManagedEntityPartialPrefixPublishesExactDeletion(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "apply", true: "reject"}[reject], func(t *testing.T) {
			b := newTestEngine(t)
			for _, id := range []NodeID{"test:part-a", "test:keep"} {
				_, err := b.CreateNode(&Node{ID: id, Properties: map[string]interface{}{"body": "stored"}})
				require.NoError(t, err)
			}
			var prepared, applied int
			rejected := errors.New("reject exact deletion")
			require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
				return func(p StoredEntityPublication) (func(), error) {
					prepared++
					require.Len(t, p.Changes, 1)
					require.NotNil(t, p.Changes[0].BeforeNode)
					require.Equal(t, NodeID("test:part-a"), p.Changes[0].BeforeNode.ID)
					require.Equal(t, "stored", p.Changes[0].BeforeNode.Properties["body"])
					require.Nil(t, p.Changes[0].AfterNode)
					if reject {
						return nil, rejected
					}
					return func() { applied++ }, nil
				}, nil
			}, nil))
			_, _, err := b.DeleteByPrefix("test:part-")
			require.Equal(t, 1, prepared)
			if reject {
				require.ErrorIs(t, err, rejected)
				require.Zero(t, applied)
				_, err = b.GetNode("test:part-a")
				require.NoError(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, applied)
				_, err = b.GetNode("test:part-a")
				require.ErrorIs(t, err, ErrNotFound)
			}
			_, err = b.GetNode("test:keep")
			require.NoError(t, err)
		})
	}
}
