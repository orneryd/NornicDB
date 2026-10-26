package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func foundationOwnerProviderHits(work *CapabilityWork, owner string) uint64 {
	var hits uint64
	for site, count := range work.Sites() {
		if strings.HasPrefix(site, "provider:") && strings.Contains(site, owner) {
			hits += count
		}
	}
	return hits
}

type foundationSubmissionBoundary struct {
	Engine
	refusal error
	calls   int
}

func (b *foundationSubmissionBoundary) GetInnerEngine() Engine {
	panic("authoritative submission owner must stop discovery")
}
func (b *foundationSubmissionBoundary) SubmitEntityCommit(context.Context, *PreparedEntityCommit, func(CommitOutcome)) (*CommitReceipt, error) {
	b.calls++
	return nil, b.refusal
}
func (b *foundationSubmissionBoundary) CheckEntityCommitAdmission() error { return b.refusal }

func TestFoundationPhysicalPublicationOwnersUseCursor(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	wal := NewWALEngine(base, nil)
	for name, issued := range map[string]Engine{"native": base, "traced-namespace": NewTracedEngine(NewNamespacedEngine(base, "tenant")), "namespace-traced": NewNamespacedEngine(NewTracedEngine(base), "tenant"), "traced-wal-traced": NewTracedEngine(NewWALEngine(NewTracedEngine(base), nil)), "namespace-wal": NewNamespacedEngine(wal, "tenant")} {
		t.Run(name, func(t *testing.T) {
			work := &CapabilityWork{Trace: true}
			end := ObserveCapabilityWork(work)
			route, found := EntityCommitSubmitterFor(issued)
			end()
			require.True(t, found)
			require.NotNil(t, route.Submitter)
			assert.Positive(t, foundationOwnerProviderHits(work, "EntityCommitSubmitterFor"), "the public route must use the common structural owner")
			if name == "native" || name == "traced-namespace" || name == "namespace-traced" {
				require.False(t, route.RequiresSubmission)
			} else {
				require.True(t, route.RequiresSubmission, "WAL remains an authoritative journal boundary")
			}
			work = &CapabilityWork{Trace: true}
			end = ObserveCapabilityWork(work)
			tx, err := BeginEntityTransaction(issued, CommitModeExplicit)
			end()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			assert.Positive(t, foundationOwnerProviderHits(work, "BeginEntityTransaction"), "native issuance must use the same cursor without granting commit authority")
			require.Equal(t, CommitModeExplicit, tx.commitMode)
			require.NoError(t, tx.SetNamespace("tenant"))
			_, err = tx.CreateNode(&Node{ID: "tenant:" + NodeID(name), Labels: []string{"Owner"}})
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())
			_, err = base.GetStoredNode("tenant:" + NodeID(name))
			require.ErrorIs(t, err, ErrNotFound, "native issuance must retain rollback semantics")
		})
	}
	refusal := errors.New("outer admission refusal")
	outer := &foundationSubmissionBoundary{Engine: base, refusal: refusal}
	route, found := EntityCommitSubmitterFor(outer)
	require.True(t, found)
	require.Same(t, outer, route.Submitter)
	require.True(t, route.RequiresSubmission)
	require.ErrorIs(t, route.CheckAdmission(), refusal)
	_, err := route.Submitter.SubmitEntityCommit(context.Background(), nil, nil)
	require.ErrorIs(t, err, refusal)
	require.Equal(t, 1, outer.calls, "exact refusal cannot retry through the native child")
	outer.refusal = nil
	require.NoError(t, route.CheckAdmission(), "the bound original admission value remains live")
}
