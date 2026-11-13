package search

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var populationDocs = VectorIndexTarget{Label: "Doc", Property: "emb"}

func populationNode(id string, vector []float64, text string) *storage.Node {
	return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"emb": vector, "text": text}}
}

// The population lifecycle of a schema vector index: POPULATING from Begin
// until Populate; ONLINE when the index serves every stored node of its label
// as storage holds it; FAILED, with the cause, when a stored vector cannot be
// served; a wait for ONLINE ends at its timeout while populating; Abort makes
// the name's previous record current again. A record exists only while its
// index is POPULATING or FAILED: ONLINE and the committed drop (Retire)
// remove it.
func TestVectorIndexPopulationLifecycle(t *testing.T) {
	ctx := context.Background()
	engine := newNamespacedMemoryEngine(t)
	_, err := engine.CreateNode(ctx, populationNode("a", []float64{1, 0, 0}, "short"))
	require.NoError(t, err)
	// One 3-dimensional vector fits the vector budget; a second does not.
	svc := NewService(engine, 3, &ServiceOptions{VectorMemoryMaxBytes: 40})
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	state := func(name string) string {
		t.Helper()
		state, _ := svc.VectorIndexState(name)
		return state
	}
	require.Equal(t, VectorIndexOnline, state("docs"), "a built service serves a name without a population record")
	require.NoError(t, svc.AwaitVectorIndexOnline(ctx, "docs", time.Millisecond))

	population := svc.BeginVectorIndexPopulation("docs", populationDocs)
	require.Equal(t, VectorIndexPopulating, state("docs"))
	require.ErrorIs(t, svc.AwaitVectorIndexOnline(ctx, "docs", 20*time.Millisecond), ErrVectorIndexNotOnline)
	waited := make(chan error, 1)
	go func() { waited <- svc.AwaitVectorIndexOnline(ctx, "docs", 5*time.Second) }()
	require.NoError(t, population.Populate(ctx, true))
	require.NoError(t, <-waited)
	require.Equal(t, VectorIndexOnline, state("docs"))
	require.Nil(t, svc.VectorIndexPopulationOf("docs"), "an ONLINE index keeps no record")

	// A stored node of the label whose vector the budget refuses: the index
	// cannot serve storage, so its population fails, with the cause.
	_, err = engine.CreateNode(ctx, populationNode("b", []float64{0, 1, 0}, "short"))
	require.NoError(t, err)
	require.ErrorIs(t, svc.RefreshNode(ctx, "b"), ErrIndexMemoryBudgetExceeded)
	failed := svc.BeginVectorIndexPopulation("docs", populationDocs)
	err = failed.Populate(ctx, true)
	require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
	require.ErrorContains(t, err, "node b")
	got, failure := svc.VectorIndexState("docs")
	require.Equal(t, VectorIndexFailed, got)
	require.ErrorIs(t, failure, ErrIndexMemoryBudgetExceeded)
	err = svc.AwaitVectorIndexOnline(ctx, "docs", time.Second)
	require.ErrorIs(t, err, ErrVectorIndexFailed)
	require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
	// Another label's index does not read b.
	other := svc.BeginVectorIndexPopulation("notes", VectorIndexTarget{Label: "Note", Property: "emb"})
	require.NoError(t, other.Populate(ctx, true))

	aborted := svc.BeginVectorIndexPopulation("docs", populationDocs)
	require.Equal(t, VectorIndexPopulating, state("docs"))
	aborted.Abort()
	require.Equal(t, VectorIndexFailed, state("docs"), "an aborted population restores the name's previous record")
	require.Same(t, failed, svc.VectorIndexPopulationOf("docs"))

	// The drop of the FAILED index commits: its record goes with it.
	failed.Retire()
	require.Nil(t, svc.VectorIndexPopulationOf("docs"))
	require.NoError(t, engine.DeleteNode(ctx, "b"))
	require.NoError(t, svc.RefreshNode(ctx, "b"))
	fresh := svc.BeginVectorIndexPopulation("docs", populationDocs)
	require.NoError(t, fresh.Populate(ctx, true))
	require.Equal(t, VectorIndexOnline, state("docs"))
	require.Nil(t, svc.VectorIndexPopulationOf("docs"))

	// A drop and a new creation of the name in one transaction: the new
	// population no longer restores the dropped one.
	failedAgain := svc.BeginVectorIndexPopulation("docs", populationDocs)
	failedAgain.transition(populationFinished, errStoredVectorNotServed)
	recreated := svc.BeginVectorIndexPopulation("docs", populationDocs)
	failedAgain.Retire()
	recreated.Abort()
	require.Nil(t, svc.VectorIndexPopulationOf("docs"), "a retired record is not restored")
}

// Population records do not outlive their indexes: many distinct names
// created and dropped leave none.
func TestVectorIndexPopulationRecordsEndWithTheirIndex(t *testing.T) {
	ctx := context.Background()
	svc := NewService(newNamespacedMemoryEngine(t), 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	for i := range 128 {
		name := fmt.Sprintf("migration_%d", i)
		population := svc.BeginVectorIndexPopulation(name, populationDocs)
		require.NoError(t, population.Populate(ctx, true))
		svc.VectorIndexPopulationOf(name).Retire() // the drop
	}
	svc.vectorIndexPopulations.mu.Lock()
	defer svc.vectorIndexPopulations.mu.Unlock()
	require.Empty(t, svc.vectorIndexPopulations.records)
}

// An index this service holds no population record for takes the state of the
// service's own build: an unfinished build is POPULATING (and waits for
// nothing), a failed one is FAILED with the build's cause, and a replacement
// service whose build succeeds serves the same index ONLINE.
func TestVectorIndexStateFollowsTheServiceBuildWithoutAPopulationRecord(t *testing.T) {
	ctx := context.Background()
	engine := newNamespacedMemoryEngine(t)
	_, err := engine.CreateNode(ctx, populationNode("a", []float64{1, 0, 0}, "short"))
	require.NoError(t, err)
	_, err = engine.CreateNode(ctx, populationNode("b", []float64{0, 1, 0}, "short"))
	require.NoError(t, err)
	// One 3-dimensional vector fits the vector budget; a second does not.
	svc := NewService(engine, 3, &ServiceOptions{VectorMemoryMaxBytes: 40})
	t.Cleanup(func() { _ = svc.Close() })

	// No build has completed: the service cannot serve the index, so it is not
	// ONLINE, and a query waits for nothing and reads storage.
	state, failure := svc.VectorIndexState("docs")
	require.Equal(t, VectorIndexPopulating, state)
	require.NoError(t, failure)
	require.NoError(t, svc.AwaitVectorIndexOnline(ctx, "docs", time.Millisecond))

	// The build fails: the index is FAILED with that build's cause, and a query
	// is refused as a failed population's is.
	require.ErrorIs(t, svc.BuildIndexes(ctx), ErrIndexMemoryBudgetExceeded)
	state, failure = svc.VectorIndexState("docs")
	require.Equal(t, VectorIndexFailed, state)
	require.ErrorIs(t, failure, ErrIndexMemoryBudgetExceeded)
	err = svc.AwaitVectorIndexOnline(ctx, "docs", time.Second)
	require.ErrorIs(t, err, ErrVectorIndexFailed)
	require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)

	// A replacement service whose build succeeds serves the same index ONLINE.
	replaced := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = replaced.Close() })
	require.NoError(t, replaced.BuildIndexes(ctx))
	state, failure = replaced.VectorIndexState("docs")
	require.Equal(t, VectorIndexOnline, state)
	require.NoError(t, failure)
	require.NoError(t, replaced.AwaitVectorIndexOnline(ctx, "docs", time.Millisecond))
}

// A query of an ONLINE index waits for nothing: no timer, no allocation.
func TestVectorIndexOnlineWaitAllocatesNothing(t *testing.T) {
	ctx := context.Background()
	svc := NewService(newNamespacedMemoryEngine(t), 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	require.NoError(t, svc.BeginVectorIndexPopulation("docs", populationDocs).Populate(ctx, true))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		_ = svc.AwaitVectorIndexOnline(ctx, "docs", time.Second)
	}), "an ONLINE index")
	require.Zero(t, testing.AllocsPerRun(100, func() {
		_ = svc.AwaitVectorIndexOnline(ctx, "never_created", time.Second)
	}), "a name without a population")
}

// The population reads storage, not the pending registry: a stored node the
// service holds stale with nothing pending (a payload write that missed a
// later stored change) is refreshed, and a text the BM25 budget refuses does
// not fail a vector index.
func TestVectorIndexPopulationMeasuresStorage(t *testing.T) {
	ctx := context.Background()
	engine := newNamespacedMemoryEngine(t)
	_, err := engine.CreateNode(ctx, populationNode("a", []float64{1, 0, 0}, "short"))
	require.NoError(t, err)
	svc := NewService(engine, 3, &ServiceOptions{BM25MemoryMaxBytes: 4096})
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	served := func() []VectorQueryHit {
		t.Helper()
		hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0, 0}, VectorQuerySpec{Label: "Doc", Property: "emb", Limit: 10})
		require.NoError(t, err)
		return hits
	}
	stored, err := engine.GetNode(ctx, "a")
	require.NoError(t, err)
	changed := storage.CopyNode(stored)
	changed.Properties["emb"] = []float64{0.6, 0.8, 0}
	changed.Properties["text"] = strings.Repeat("payload ", 1024)
	require.NoError(t, engine.UpdateNode(ctx, storage.NodeUpdateRequest{After: changed}))
	require.InDelta(t, 1.0, served()[0].Score, 1e-6, "before the population, the service holds the earlier vector with nothing pending")

	require.NoError(t, svc.BeginVectorIndexPopulation("docs", populationDocs).Populate(ctx, true))
	hits := served()
	require.Len(t, hits, 1)
	// Scores are Neo4j's (1 + cosine) / 2: the stored [0.6, 0.8, 0] scores 0.8.
	require.InDelta(t, 0.8, hits[0].Score, 1e-6, "the population serves the stored vector")
	require.Empty(t, buildWriteSearchIDs(t, svc, "short"), "the replaced text is withdrawn")
	require.Empty(t, buildWriteSearchIDs(t, svc, "payload"), "the text beyond the BM25 budget is refused")
}

// Review of #1753 after round 11 (r4508329102): a population asks the
// installer's own decision whether it indexes a node (shouldIndexNode), with
// the properties that decision reads. The installer does not index a
// historical temporal version, so an index on a temporal label is ONLINE
// without one, and a version served while it was current is withdrawn once a
// newer version has closed it.
func TestVectorIndexPopulationFollowsTheTemporalDecision(t *testing.T) {
	ctx := context.Background()
	newStore := func(t *testing.T) *storage.NamespacedEngine {
		t.Helper()
		base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, base.Close()) })
		require.NoError(t, base.GetSchemaForNamespace("test").AddConstraint(storage.Constraint{
			Name:       "fact_temporal",
			Type:       storage.ConstraintTemporal,
			Label:      "FactVersion",
			Properties: []string{"fact_key", "valid_from", "valid_to"},
		}))
		return storage.NewNamespacedEngine(base, "test")
	}
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	version := func(id string, from time.Time, to any, emb []float64) *storage.Node {
		return &storage.Node{ID: storage.NodeID(id), Labels: []string{"FactVersion"}, Properties: map[string]any{
			"fact_key": "k", "valid_from": from, "valid_to": to, "emb": emb,
		}}
	}
	newService := func(t *testing.T, store storage.Engine) *Service {
		t.Helper()
		svc := NewService(store, 3, nil)
		t.Cleanup(func() { require.NoError(t, svc.Close()) })
		require.NoError(t, svc.BuildIndexes(ctx))
		return svc
	}
	populate := func(t *testing.T, svc *Service) {
		t.Helper()
		population := svc.BeginVectorIndexPopulation("facts", VectorIndexTarget{Label: "FactVersion", Property: "emb"})
		require.NoError(t, population.Populate(ctx, true))
		state, failure := svc.VectorIndexState("facts")
		require.Equal(t, VectorIndexOnline, state, "%v", failure)
	}
	held := func(svc *Service, id string) []string {
		svc.indexMu.RLock()
		defer svc.indexMu.RUnlock()
		return svc.indexVectorKeysLocked(id, "emb")
	}

	t.Run("a historical version is not required", func(t *testing.T) {
		store := newStore(t)
		_, err := store.CreateNode(ctx, version("v1", start, end, []float64{1, 0, 0}))
		require.NoError(t, err)
		_, err = store.CreateNode(ctx, version("v2", end, nil, []float64{0, 1, 0}))
		require.NoError(t, err)
		svc := newService(t, store)
		populate(t, svc)
		require.Empty(t, held(svc, "v1"))
		require.Len(t, held(svc, "v2"), 1)
	})

	t.Run("a version served while current is withdrawn once closed", func(t *testing.T) {
		store := newStore(t)
		_, err := store.CreateNode(ctx, version("v1", start, nil, []float64{1, 0, 0}))
		require.NoError(t, err)
		svc := newService(t, store)
		require.Len(t, held(svc, "v1"), 1)
		// A newer version closes v1 in storage; the search service is not told.
		stored, err := store.GetNode(ctx, "v1")
		require.NoError(t, err)
		closed := storage.CopyNode(stored)
		closed.Properties["valid_to"] = end
		require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: closed}))
		_, err = store.CreateNode(ctx, version("v2", end, nil, []float64{0, 1, 0}))
		require.NoError(t, err)
		populate(t, svc)
		require.Empty(t, held(svc, "v1"), "the installer does not index the closed version")
		require.Len(t, held(svc, "v2"), 1)
	})
}
