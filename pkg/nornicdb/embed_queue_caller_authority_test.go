package nornicdb_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func embeddingAuthorityFixture(t *testing.T) (*nornicdb.EmbedWorker, *storage.NamespacedEngine, *dlqPublicDocument) {
	t.Helper()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	fixtureowner.Own(t, base)
	base.SetEmbeddingsEnabled(true)
	store := storage.NewNamespacedEngine(base, "authority")
	provider := &dlqPublicDocument{dlqPublicProvider: &dlqPublicProvider{}}
	config := nornicdb.DefaultEmbedWorkerConfig()
	config.DeferWorkerStart = true
	config.NumWorkers = 1
	config.EmbedBatchSize = 8
	config.MaxRetries = 0
	config.BatchDelay = 0
	config.TriggerDebounceDelay = 0
	config.ScanInterval = time.Hour
	config.ProviderRetryBackoff = 20 * time.Millisecond
	config.ProviderRetryBackoffMax = 40 * time.Millisecond
	worker := nornicdb.NewEmbedWorker(provider, store, config)
	t.Cleanup(func() {
		worker.Close()
		require.NoError(t, base.Close())
	})
	return worker, store, provider
}

func embeddingAuthorityNode(t *testing.T, store *storage.NamespacedEngine, id, text string) storage.NodeID {
	t.Helper()
	nodeID := storage.NodeID(id)
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: nodeID, Properties: map[string]any{"content": text}})
	require.NoError(t, err)
	store.AddToPendingEmbeddings(nodeID)
	return nodeID
}

// parkThroughProviderFailure moves nodes into the durable terminal state the
// manual retry administers: the provider fails, the allowance is exhausted, and
// the accepted outcome records the parked marker.
func parkThroughProviderFailure(t *testing.T, worker *nornicdb.EmbedWorker, provider *dlqPublicDocument, ids ...storage.NodeID) {
	t.Helper()
	provider.mu.Lock()
	provider.reject = func(string) bool { return true }
	provider.mu.Unlock()
	worker.StartWorkers()
	require.Eventually(t, func() bool { return worker.Stats().Parked == len(ids) }, 5*time.Second, time.Millisecond)
	provider.mu.Lock()
	provider.calls = nil
	provider.calledAt = nil
	provider.reject = nil
	provider.mu.Unlock()
}

func recordedCallerEmbeddingInputs(provider *dlqPublicDocument) []string {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return append([]string(nil), provider.calls...)
}

// fakeEmbeddingAuthority is the caller operation's authority. It admits the
// whole batch or refuses it, exactly like the server's FGAC revalidation.
type fakeEmbeddingAuthority struct {
	mu      sync.Mutex
	refuse  map[storage.NodeID]bool
	batches [][]storage.NodeID
}

func (a *fakeEmbeddingAuthority) RevalidateEmbeddingJob(_ context.Context, nodes []*storage.Node) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := make([]storage.NodeID, len(nodes))
	for index, node := range nodes {
		ids[index] = node.ID
		if a.refuse[node.ID] {
			a.batches = append(a.batches, ids)
			return nornicdb.ErrEmbeddingSubmissionRefused
		}
	}
	a.batches = append(a.batches, ids)
	return nil
}

func (a *fakeEmbeddingAuthority) revalidatedBatches() [][]storage.NodeID {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([][]storage.NodeID(nil), a.batches...)
}

func parkedEmbeddingTexts(t *testing.T, worker *nornicdb.EmbedWorker) map[storage.NodeID]string {
	t.Helper()
	failures, err := worker.ParkedEmbeddingFailures(t.Context(), 0)
	require.NoError(t, err)
	byNode := make(map[storage.NodeID]string, len(failures))
	for _, failure := range failures {
		byNode[failure.NodeID] = failure.Error
	}
	return byNode
}

// TestEmbeddingCallerAuthorityRefusesOperationBeforeSubmitting proves the
// product contract for a caller-caused retry: when the operation's authority no
// longer admits the batch, the external provider receives none of that input and
// the refusal is recorded. Managed (service) work in the same claim window is
// unrelated and still reaches the provider.
func TestEmbeddingCallerAuthorityRefusesOperationBeforeSubmitting(t *testing.T) {
	worker, store, provider := embeddingAuthorityFixture(t)
	callerA := embeddingAuthorityNode(t, store, "caller-a", "caller a body")
	callerB := embeddingAuthorityNode(t, store, "caller-b", "caller b body")
	parkThroughProviderFailure(t, worker, provider, callerA, callerB)

	// Managed work that no caller operation caused.
	embeddingAuthorityNode(t, store, "service", "service body")

	authority := &fakeEmbeddingAuthority{refuse: map[storage.NodeID]bool{callerB: true}}
	retried, err := worker.RetryParkedEmbeddingFailures(nornicdb.WithEmbeddingJobAuthority(t.Context(), authority), []storage.NodeID{callerA, callerB})
	require.NoError(t, err)
	require.Equal(t, 2, retried)

	worker.TriggerImmediate()
	require.Eventually(t, func() bool {
		parked := parkedEmbeddingTexts(t, worker)
		return len(parked) == 2
	}, 5*time.Second, time.Millisecond)

	calls := recordedCallerEmbeddingInputs(provider)
	require.Equal(t, []string{"content: service body"}, calls, "only managed input may reach the provider while the caller operation is refused")
	require.NotEmpty(t, authority.revalidatedBatches(), "the batch must be revalidated before submission")

	parked := parkedEmbeddingTexts(t, worker)
	for _, id := range []storage.NodeID{callerA, callerB} {
		require.Contains(t, parked, id)
		require.Contains(t, parked[id], "embedding submission refused")
	}
	require.Equal(t, 1, worker.Stats().Processed, "the managed node is the only accepted outcome")
}

// TestEmbeddingCallerAuthorityAdmitsAuthorizedInput proves the admitted branch:
// the caller operation's own input reaches the provider after revalidation.
func TestEmbeddingCallerAuthorityAdmitsAuthorizedInput(t *testing.T) {
	worker, store, provider := embeddingAuthorityFixture(t)
	callerA := embeddingAuthorityNode(t, store, "caller-a", "caller a body")
	callerB := embeddingAuthorityNode(t, store, "caller-b", "caller b body")
	parkThroughProviderFailure(t, worker, provider, callerA, callerB)

	authority := &fakeEmbeddingAuthority{}
	retried, err := worker.RetryParkedEmbeddingFailures(nornicdb.WithEmbeddingJobAuthority(t.Context(), authority), []storage.NodeID{callerA, callerB})
	require.NoError(t, err)
	require.Equal(t, 2, retried)

	worker.TriggerImmediate()
	require.Eventually(t, func() bool { return worker.Stats().Processed == 2 }, 5*time.Second, time.Millisecond)

	require.ElementsMatch(t, []string{"content: caller a body", "content: caller b body"}, recordedCallerEmbeddingInputs(provider))
	require.Empty(t, parkedEmbeddingTexts(t, worker))
	batches := authority.revalidatedBatches()
	require.Len(t, batches, 3, "before the provider batch and before each result")
	require.ElementsMatch(t, []storage.NodeID{callerA, callerB}, batches[0])
	require.ElementsMatch(t, [][]storage.NodeID{{callerA}, {callerB}}, batches[1:])
}

// TestEmbeddingServiceRetryKeepsExistingContract proves the disabled-mode and
// service contract: a retry that records no caller provenance behaves exactly as
// before, because there is no caller operation to revalidate.
func TestEmbeddingServiceRetryKeepsExistingContract(t *testing.T) {
	worker, store, provider := embeddingAuthorityFixture(t)
	first := embeddingAuthorityNode(t, store, "first", "first body")
	second := embeddingAuthorityNode(t, store, "second", "second body")
	parkThroughProviderFailure(t, worker, provider, first, second)

	retried, err := worker.RetryParkedEmbeddingFailures(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, 2, retried)

	worker.TriggerImmediate()
	require.Eventually(t, func() bool { return worker.Stats().Processed == 2 }, 5*time.Second, time.Millisecond)

	require.ElementsMatch(t, []string{"content: first body", "content: second body"}, recordedCallerEmbeddingInputs(provider))
	require.Empty(t, parkedEmbeddingTexts(t, worker))
}

// TestEmbeddingCallerAuthorityRequiresAuthority pins the ownership boundary: a
// caller-caused requeue cannot be recorded without the authority that must
// revalidate it.
func TestEmbeddingCallerAuthorityRequiresAuthority(t *testing.T) {
	worker, store, provider := embeddingAuthorityFixture(t)
	node := embeddingAuthorityNode(t, store, "required", "required body")
	parkThroughProviderFailure(t, worker, provider, node)

	retried, err := worker.RetryParkedEmbeddingFailures(nornicdb.WithEmbeddingJobAuthority(t.Context(), nil), []storage.NodeID{node})
	require.Error(t, err)
	require.Zero(t, retried)
	require.Len(t, parkedEmbeddingTexts(t, worker), 1, "a rejected call must not requeue the failure")
}

// TestEmbeddingCallerAuthoritySurvivesSourceText keeps the revalidation focused
// on authority, not on content: the provider receives the stored source text of
// an admitted caller-caused entity unchanged.
func TestEmbeddingCallerAuthoritySurvivesSourceText(t *testing.T) {
	worker, store, provider := embeddingAuthorityFixture(t)
	node := embeddingAuthorityNode(t, store, "verbatim", "  keep   spacing  ")
	parkThroughProviderFailure(t, worker, provider, node)

	_, err := worker.RetryParkedEmbeddingFailures(nornicdb.WithEmbeddingJobAuthority(t.Context(), &fakeEmbeddingAuthority{}), []storage.NodeID{node})
	require.NoError(t, err)
	worker.TriggerImmediate()
	require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 5*time.Second, time.Millisecond)
	require.Equal(t, []string{"content:   keep   spacing  "}, recordedCallerEmbeddingInputs(provider))
}

// TestEmbeddingCallerAuthorityErrorIsTerminalRefusal documents that the refusal
// text persisted for operators comes from the authoritative error, not from the
// provider.
func TestEmbeddingCallerAuthorityErrorIsTerminalRefusal(t *testing.T) {
	worker, store, provider := embeddingAuthorityFixture(t)
	node := embeddingAuthorityNode(t, store, "refused", "refused body")
	parkThroughProviderFailure(t, worker, provider, node)

	authority := &fakeEmbeddingAuthority{refuse: map[storage.NodeID]bool{node: true}}
	_, err := worker.RetryParkedEmbeddingFailures(nornicdb.WithEmbeddingJobAuthority(t.Context(), authority), []storage.NodeID{node})
	require.NoError(t, err)
	worker.TriggerImmediate()
	require.Eventually(t, func() bool { return worker.Stats().Parked == 1 }, 5*time.Second, time.Millisecond)

	parked := parkedEmbeddingTexts(t, worker)
	require.Contains(t, strings.ToLower(parked[node]), "embedding submission refused")
	require.Empty(t, recordedCallerEmbeddingInputs(provider))
}

func TestEmbeddingCallerAuthorityInterruptedRetryStaysParked(t *testing.T) {
	worker, store, provider := embeddingAuthorityFixture(t)
	id := embeddingAuthorityNode(t, store, "interrupted", "pending caller input")
	node, err := store.GetNode(t.Context(), id)
	require.NoError(t, err)
	// A restarted worker sees the durable marker and has no live caller binding.
	node.EmbedMeta = map[string]any{storage.EmbeddingCallerPendingKey: true}
	require.NoError(t, store.UpdateNodeEmbeddingSidecar(t.Context(), node))
	worker.StartWorkers()
	require.Eventually(t, func() bool { return worker.Stats().Parked == 1 }, 5*time.Second, time.Millisecond)
	require.Empty(t, recordedCallerEmbeddingInputs(provider))
	require.Contains(t, parkedEmbeddingTexts(t, worker)[id], "embedding submission refused")
}
