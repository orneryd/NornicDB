// Author: NOR-288 implementation lane (Codex), 2026-10-29.
package replication

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type acceptedFenceNative struct {
	*storage.MemoryEngine
	onSubmit func()
	calls    atomic.Int32
}

func (n *acceptedFenceNative) SubmitEntityCommit(ctx context.Context, p *storage.PreparedEntityCommit, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	n.calls.Add(1)
	if n.onSubmit != nil {
		n.onSubmit()
	}
	return n.MemoryEngine.SubmitEntityCommit(ctx, p, settle)
}

type acceptedFenceStorage struct {
	Storage
	native      *acceptedFenceNative
	t           *testing.T
	observed    atomic.Bool
	joined      chan struct{}
	submitted   chan *Command
	allowSubmit <-chan struct{}
}

func (s *acceptedFenceStorage) ApplyCommand(_ context.Context, command *Command) error {
	if command.accepted != nil && s.observed.CompareAndSwap(false, true) {
		s.native.onSubmit = func() {
			// The storage owner's own test proves submission runs outside its
			// arbitration; this transport only parks the foreign callback.
			close(s.joined)
			if s.submitted != nil {
				s.submitted <- command
				<-s.allowSubmit
			}
		}
	} else {
		s.native.onSubmit = nil
	}
	return s.Storage.ApplyCommand(context.Background(), command)
}

func TestAcceptedConcurrentApplyJoinsInitialNativePublication(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	native := &acceptedFenceNative{MemoryEngine: base}
	fixture := setupTestAdapter(t, adapterFixtureOptions{backing: native})
	allow := make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(allow) }) }
	mirror := &acceptedFenceStorage{Storage: fixture.adapter, native: native, t: t, joined: make(chan struct{}), submitted: make(chan *Command, 1), allowSubmit: allow}
	replicator := NewStandaloneReplicator(DefaultConfig(), mirror)
	require.NoError(t, replicator.Start(t.Context()))
	t.Cleanup(func() { _ = replicator.Shutdown() })
	engine := NewReplicatedEngine(native, replicator, time.Second)
	tx, err := storage.BeginEntityTransaction(base, storage.CommitModeExplicit)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.CreateNode(&storage.Node{ID: "test:concurrent-accepted", Properties: map[string]any{"v": "once"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "concurrent-accepted"})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	t.Cleanup(unblock)
	receipt, err := engine.SubmitEntityCommit(t.Context(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var command *Command
	select {
	case command = <-mirror.submitted:
	case <-ctx.Done():
		t.Fatal("initial native submission did not reach its public boundary")
	}
	const competitors = 8
	results := make(chan error, competitors)
	var entered sync.WaitGroup
	entered.Add(competitors)
	for i := 0; i < competitors; i++ {
		go func() { entered.Done(); results <- fixture.adapter.ApplyCommand(context.Background(), command) }()
	}
	entered.Wait()
	unblock()
	require.NoError(t, receipt.Wait(ctx))
	for i := 0; i < competitors; i++ {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("competing accepted application did not join native publication")
		}
	}
	require.EqualValues(t, 1, native.calls.Load(), "competing application cannot resubmit native input")
	node, err := base.GetStoredNode("test:concurrent-accepted")
	require.NoError(t, err)
	require.Equal(t, "once", node.Properties["v"])
	require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: node.ID, Properties: map[string]any{"v": "following"}}}))
}

func TestAcceptedNativeSubmissionCanReenterCompletion(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	native := &acceptedFenceNative{MemoryEngine: base}
	fixture := setupTestAdapter(t, adapterFixtureOptions{backing: native})
	mirror := &acceptedFenceStorage{Storage: fixture.adapter, native: native, t: t, joined: make(chan struct{})}
	replicator := NewStandaloneReplicator(DefaultConfig(), mirror)
	require.NoError(t, replicator.Start(t.Context()))
	t.Cleanup(func() { _ = replicator.Shutdown() })
	engine := NewReplicatedEngine(native, replicator, time.Second)
	tx, err := storage.BeginEntityTransaction(base, storage.CommitModeExplicit)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.CreateNode(&storage.Node{ID: "test:accepted-fence", Properties: map[string]any{"v": "accepted"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "accepted-fence"})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	receipt, err := engine.SubmitEntityCommit(t.Context(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, receipt.Wait(ctx))
	select {
	case <-mirror.joined:
	case <-ctx.Done():
		t.Fatal("native callback did not finish reentry after accepted submission")
	}
	require.Equal(t, storage.CommitApplied, receipt.Outcome().State)
	require.EqualValues(t, 1, native.calls.Load(), "reentry cannot submit the command again")
	node, err := base.GetStoredNode("test:accepted-fence")
	require.NoError(t, err)
	require.Equal(t, "accepted", node.Properties["v"])
	count, err := base.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: node.ID, Properties: map[string]any{"v": "following"}}}))
}
