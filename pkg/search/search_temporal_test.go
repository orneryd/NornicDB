package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type temporalDecisionTestEngine struct {
	storage.Engine
	decide func(context.Context, *storage.Node) (bool, error)
}

func (e *temporalDecisionTestEngine) IsCurrentTemporalNode(ctx context.Context, node *storage.Node, _ time.Time) (bool, error) {
	return e.decide(ctx, node)
}

func (e *temporalDecisionTestEngine) GraphMutationVersion() (uint64, bool) {
	provider, ok := e.Engine.(storage.GraphMutationVersionProvider)
	if !ok {
		return 0, false
	}
	return provider.GraphMutationVersion()
}

func (e *temporalDecisionTestEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	return storage.OpenReadSnapshot(ctx, e.Engine)
}

func newTemporalDecisionTestService(t *testing.T, decide func(context.Context, *storage.Node) (bool, error)) (*nodeSnapshotGateEngine, *storage.NamespacedEngine, *Service) {
	t.Helper()
	base := newNodeSnapshotGateEngine(t)
	require.NoError(t, base.GetSchemaForNamespace("test").AddConstraint(storage.Constraint{
		Name:       "fact_temporal",
		Type:       storage.ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}))
	ns := storage.NewNamespacedEngine(base, "test")
	engine := &temporalDecisionTestEngine{Engine: ns, decide: decide}
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	return base, ns, svc
}

func TestSearchService_BuildIndexesSkipsHistoricalTemporalVersions(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	ns := storage.NewNamespacedEngine(base, "test")
	require.NoError(t, base.GetSchemaForNamespace("test").AddConstraint(storage.Constraint{
		Name:       "fact_temporal",
		Type:       storage.ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}))

	v1Start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	v1End := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	v2Start := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err = ns.CreateNode(t.Context(), &storage.Node{
		ID:     "fact-v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "search-current",
			"valid_from": v1Start,
			"valid_to":   v1End,
			"title":      "historical",
		},
		ChunkEmbeddings: [][]float32{{1, 0}},
	})
	require.NoError(t, err)
	_, err = ns.CreateNode(t.Context(), &storage.Node{
		ID:     "fact-v2",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "search-current",
			"valid_from": v2Start,
			"valid_to":   nil,
			"title":      "current",
		},
		ChunkEmbeddings: [][]float32{{0, 1, 0}},
	})
	require.NoError(t, err)

	svc := NewService(ns, 3, nil)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Equal(t, 1, svc.EmbeddingCount())
	require.Equal(t, 3, svc.VectorIndexDimensions(), "a historical version must not choose vector dimensions")
	require.Empty(t, svc.fulltext().Search("historical", 10))
	require.Len(t, svc.fulltext().Search("current", 10), 1)
}

func TestSearchService_IndexNodeHistoricalVersionDoesNotAdoptDimensions(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	ns := storage.NewNamespacedEngine(base, "test")
	require.NoError(t, base.GetSchemaForNamespace("test").AddConstraint(storage.Constraint{
		Name:       "fact_temporal",
		Type:       storage.ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}))

	v1Start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	v1End := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	v2Start := v1End
	historical := &storage.Node{
		ID:     "fact-v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "search-current",
			"valid_from": v1Start,
			"valid_to":   v1End,
			"title":      "historical",
		},
		ChunkEmbeddings: [][]float32{{1, 0}},
	}
	_, err = ns.CreateNode(t.Context(), historical)
	require.NoError(t, err)
	_, err = ns.CreateNode(t.Context(), &storage.Node{
		ID:     "fact-v2",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "search-current",
			"valid_from": v2Start,
			"valid_to":   nil,
			"title":      "current",
		},
	})
	require.NoError(t, err)

	svc := NewService(ns, 3, nil)
	require.NoError(t, svc.IndexNode(historical))
	require.Zero(t, svc.EmbeddingCount())
	require.Equal(t, 3, svc.VectorIndexDimensions(), "historical embeddings must not choose dimensions")
	require.Empty(t, svc.fulltext().Search("historical", 10), "historical text must not be indexed")
}

func TestSearchService_TemporalDecisionErrorsDoNotPublishPartialIndexes(t *testing.T) {
	wantError := errors.New("temporal index decision failed")
	for _, tc := range []struct {
		name       string
		want       error
		cancel     bool
		decisionFn func(context.Context) (bool, error)
	}{
		{
			name: "provider error during build",
			want: wantError,
			decisionFn: func(context.Context) (bool, error) {
				return false, wantError
			},
		},
		{
			name:   "cancellation during build decision",
			want:   context.Canceled,
			cancel: true,
			decisionFn: func(ctx context.Context) (bool, error) {
				cancel := ctx.Value(temporalDecisionCancelKey{}).(context.CancelFunc)
				cancel()
				return false, ctx.Err()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			if tc.cancel {
				cancelCtx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				ctx = context.WithValue(cancelCtx, temporalDecisionCancelKey{}, cancel)
			}
			base, ns, svc := newTemporalDecisionTestService(t, func(ctx context.Context, _ *storage.Node) (bool, error) {
				return tc.decisionFn(ctx)
			})
			_, err := ns.CreateNode(t.Context(), &storage.Node{
				ID:     "failed-decision",
				Labels: []string{"FactVersion"},
				Properties: map[string]any{
					"fact_key":   "decision",
					"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					"valid_to":   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
					"title":      "must-not-publish",
				},
				ChunkEmbeddings: [][]float32{{1, 0}},
			})
			require.NoError(t, err)

			base.open()
			err = svc.BuildIndexes(ctx)
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, svc.EmbeddingCount())
			require.Equal(t, 3, svc.VectorIndexDimensions())
			require.Zero(t, svc.FulltextDocCount())
			require.False(t, svc.ready.Load(), "failed/canceled population must not be reusable")
		})
	}
}

type temporalDecisionCancelKey struct{}

func TestSearchService_LiveTemporalDecisionErrorPrecedesDimensionAdmission(t *testing.T) {
	want := errors.New("temporal index decision failed")
	_, ns, svc := newTemporalDecisionTestService(t, func(context.Context, *storage.Node) (bool, error) {
		return false, want
	})
	node := &storage.Node{
		ID:     "live-failed-decision",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "live",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			"title":      "must-not-publish",
		},
		ChunkEmbeddings: [][]float32{{1, 0}},
	}
	_, err := ns.CreateNode(t.Context(), node)
	require.NoError(t, err)

	require.ErrorIs(t, svc.IndexNode(node), want)
	require.Zero(t, svc.EmbeddingCount())
	require.Equal(t, 3, svc.VectorIndexDimensions())
	require.Zero(t, svc.FulltextDocCount())
}

func TestSearchService_RefreshNodeTemporalCancellationPrecedesPublication(t *testing.T) {
	_, ns, svc := newTemporalDecisionTestService(t, func(ctx context.Context, _ *storage.Node) (bool, error) {
		cancel := ctx.Value(temporalDecisionCancelKey{}).(context.CancelFunc)
		cancel()
		return false, ctx.Err()
	})
	node := &storage.Node{
		ID:     "refresh-cancel",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "refresh",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			"title":      "must-not-publish",
		},
		ChunkEmbeddings: [][]float32{{1, 0}},
	}
	_, err := ns.CreateNode(t.Context(), node)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx = context.WithValue(ctx, temporalDecisionCancelKey{}, cancel)

	require.ErrorIs(t, svc.RefreshNode(ctx, node.ID), context.Canceled)
	require.Zero(t, svc.EmbeddingCount())
	require.Equal(t, 3, svc.VectorIndexDimensions())
	require.Zero(t, svc.FulltextDocCount())
}

func TestSearchService_QueuedTemporalDecisionCancellationDoesNotEnqueue(t *testing.T) {
	base, _, svc := newTemporalDecisionTestService(t, func(_ context.Context, node *storage.Node) (bool, error) {
		if node.ID == "queued-cancel" {
			return false, context.Canceled
		}
		return true, nil
	})
	built := startGatedBuild(t, base, svc)
	node := &storage.Node{
		ID:     "queued-cancel",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "queued",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			"title":      "must-not-publish",
		},
		ChunkEmbeddings: [][]float32{{1, 0}},
	}
	_, err := svc.engine.CreateNode(t.Context(), node)
	require.NoError(t, err)
	err = svc.IndexNode(node)
	require.ErrorIs(t, err, context.Canceled)
	svc.buildWrites.mu.Lock()
	require.Empty(t, svc.buildWrites.writes)
	require.Zero(t, svc.buildWrites.bytes)
	svc.buildWrites.mu.Unlock()
	require.Zero(t, svc.EmbeddingCount())
	require.Equal(t, 3, svc.VectorIndexDimensions())
	base.open()
	require.NoError(t, <-built)
	require.Zero(t, svc.FulltextDocCount())
}

func TestSearchService_QueuedRefreshNodeActualCancellationDoesNotPublishAndRetries(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	type requestValueKey struct{}
	requestValue := new(int)
	decisions := 0
	base, ns, svc := newTemporalDecisionTestService(t, func(ctx context.Context, node *storage.Node) (bool, error) {
		decisions++
		require.Equal(t, storage.NodeID("queued-refresh-cancel"), node.ID)
		require.Same(t, requestValue, ctx.Value(requestValueKey{}), "admission must retain the request values")
		require.NoError(t, ctx.Err(), "the request must reach temporal admission before cancellation")
		if cancel, ok := ctx.Value(temporalDecisionCancelKey{}).(context.CancelFunc); ok {
			cancel()
			return false, ctx.Err()
		}
		return true, nil
	})
	built := startGatedBuild(t, base, svc)
	joined := false
	defer func() {
		base.open()
		if !joined {
			<-built
		}
	}()
	// Commit after the gated build has opened its empty snapshot. Only the
	// public refresh can publish this node, never the build's baseline.
	node := &storage.Node{
		ID:     "queued-refresh-cancel",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "queued-refresh",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   nil,
			"title":      "queuedrefreshterm",
		},
		ChunkEmbeddings: [][]float32{{1, 0}},
	}
	_, err := ns.CreateNode(t.Context(), node)
	require.NoError(t, err)
	healthy := context.WithValue(t.Context(), requestValueKey{}, requestValue)
	ctx, cancel := context.WithCancel(healthy)
	defer cancel()
	ctx = context.WithValue(ctx, temporalDecisionCancelKey{}, cancel)
	require.True(t, svc.buildWrites.open.Load())
	require.True(t, svc.buildDefersWrites(), "refresh must exercise queued admission, not the live path")

	require.ErrorIs(t, svc.RefreshNode(ctx, node.ID), context.Canceled)
	require.ErrorIs(t, ctx.Err(), context.Canceled, "the actual request was canceled inside temporal admission")
	require.Equal(t, 1, decisions)
	svc.buildWrites.mu.Lock()
	queued, retainedBytes := len(svc.buildWrites.writes), svc.buildWrites.bytes
	svc.buildWrites.mu.Unlock()
	require.Zero(t, queued)
	require.Zero(t, retainedBytes)
	require.Zero(t, svc.EmbeddingCount())
	require.Equal(t, 3, svc.VectorIndexDimensions())
	require.Zero(t, svc.FulltextDocCount())

	base.open()
	buildErr := <-built
	joined = true
	require.NoError(t, buildErr)
	require.Equal(t, 1, decisions, "failed preparation must not leave a reusable decision for replay")
	require.Zero(t, svc.EmbeddingCount())
	require.Equal(t, 3, svc.VectorIndexDimensions())
	require.Zero(t, svc.FulltextDocCount())

	require.NoError(t, svc.RefreshNode(healthy, node.ID))
	require.Equal(t, 2, decisions, "healthy retry must prepare a new temporal decision")
	require.Equal(t, 1, svc.EmbeddingCount())
	require.Equal(t, 2, svc.VectorIndexDimensions())
	require.Equal(t, 1, svc.FulltextDocCount())
	require.Len(t, svc.fulltext().Search("queuedrefreshterm", 10), 1)
}

func TestSearchService_ContinuationTemporalDecisionCancellationPropagates(t *testing.T) {
	_, ns, svc := newTemporalDecisionTestService(t, func(ctx context.Context, _ *storage.Node) (bool, error) {
		cancel := ctx.Value(temporalDecisionCancelKey{}).(context.CancelFunc)
		cancel()
		return false, ctx.Err()
	})
	_, err := ns.CreateNode(t.Context(), &storage.Node{
		ID:     "continuation-cancel",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "continuation",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			"title":      "must-not-publish",
		},
	})
	require.NoError(t, err)
	ctx, cancelRequest := context.WithCancel(t.Context())
	defer cancelRequest()
	ctx = context.WithValue(ctx, temporalDecisionCancelKey{}, cancelRequest)
	_, err = svc.SearchTextContinuation(ctx, "", DefaultSearchOptions(), SearchContinuationRequest{
		Owner: "temporal-cancel",
		Mode:  SearchContinuationID,
		N:     10,
	}, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestSearchService_IndexNodeRemovesClosedTemporalVersion(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	ns := storage.NewNamespacedEngine(base, "test")
	require.NoError(t, base.GetSchemaForNamespace("test").AddConstraint(storage.Constraint{
		Name:       "fact_temporal",
		Type:       storage.ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}))

	start := time.Now().UTC().Add(-2 * time.Hour)
	node := &storage.Node{
		ID:     "fact-live",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "search-live",
			"valid_from": start,
			"valid_to":   nil,
		},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
	}
	_, err = ns.CreateNode(t.Context(), node)
	require.NoError(t, err)

	svc := NewService(ns, 3, nil)
	require.NoError(t, svc.IndexNode(node))
	require.Equal(t, 1, svc.EmbeddingCount())

	closed := storage.CopyNode(node)
	closed.Properties["valid_to"] = time.Now().UTC().Add(-time.Hour)
	require.NoError(t, ns.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: closed}))
	require.NoError(t, svc.IndexNode(closed))
	require.Equal(t, 0, svc.EmbeddingCount())

	// A historical version remains excluded even when its old model's
	// embedding dimension differs from the current search index.
	svc.FixVectorDimensions()
	closed.ChunkEmbeddings = [][]float32{{1, 0}}
	require.NoError(t, svc.IndexNode(closed))
	require.Zero(t, svc.EmbeddingCount())
}

// A transient temporal lookup failure must leave the committed image repairable
// through the existing retry owner, using the original refresh reservation.
func TestStoredRefreshTemporalFailureRetainsRetryOwnership(t *testing.T) {
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded, errors.New("transient temporal read")} {
		t.Run(failure.Error(), func(t *testing.T) {
			var failed bool
			_, ns, svc := newTemporalDecisionTestService(t, func(context.Context, *storage.Node) (bool, error) {
				if failed {
					return false, failure
				}
				return true, nil
			})
			node := &storage.Node{ID: "retry-temporal", Labels: []string{"FactVersion"}, Properties: map[string]any{"title": "oldterm", "fact_key": "retry", "valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "valid_to": nil}, ChunkEmbeddings: [][]float32{{1, 0, 0}}}
			_, err := ns.CreateNode(t.Context(), node)
			require.NoError(t, err)
			require.NoError(t, svc.RefreshNode(t.Context(), node.ID))
			changed := storage.CopyNode(node)
			changed.Properties["title"] = "newterm"
			changed.ChunkEmbeddings = [][]float32{{0, 1, 0}}
			require.NoError(t, ns.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: changed}))
			var retries []RefreshRetry
			svc.SetRefreshRetryOwner(func(work RefreshRetry, err error) {
				require.ErrorIs(t, err, failure)
				retries = append(retries, work)
			})
			failed = true
			mutation := svc.ReserveNodeMutation(node.ID)
			require.ErrorIs(t, svc.RefreshNodeWithMutation(t.Context(), mutation), failure)
			require.Len(t, retries, 1, "installation failure must reach the retry owner")
			require.True(t, retries[0].Current(), "the reservation must remain owned until repaired")
			require.False(t, retries[0].Retired())
			require.Empty(t, svc.fulltext().Search("oldterm", 10), "failed refresh withdraws the replaced text")
			require.Zero(t, svc.EmbeddingCount(), "failed refresh withdraws the replaced vectors")
			failed = false
			require.NoError(t, svc.RefreshNodeWithMutation(t.Context(), retries[0].Node))
			require.True(t, retries[0].Retired())
			require.Len(t, retries, 1)
			require.Empty(t, svc.fulltext().Search("oldterm", 10))
			require.Len(t, svc.fulltext().Search("newterm", 10), 1)
			hits, err := svc.VectorQueryNodes(t.Context(), []float32{0, 1, 0}, VectorQuerySpec{Limit: 10})
			require.NoError(t, err)
			require.Len(t, hits, 1)
		})
	}
}
