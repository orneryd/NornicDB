package search

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A fine-grained vector page admits its indexed candidates by reading the
// authorized image of each candidate node. Repeated owners use bounded
// batches; mostly distinct owners use point reads to avoid batch result maps.
// The admissions must keep the
// candidate order, each entry's own source visibility, the absence of an
// unreadable or missing node as a skip rather than an error, cancellation, and
// the graph-version guard that owns page validity.

// indexedVectorReadSpy records the node images a fine-grained read resolves, so
// a test can tell one batched candidate read from one point read per candidate.
// It also runs one graph mutation inside the first batch read, so a test can
// observe the version guard a bounded read must still trip.
type indexedVectorReadSpy struct {
	storage.Engine

	mu            sync.Mutex
	point         []storage.NodeID
	batches       [][]storage.NodeID
	mutateOnBatch func()
}

func (spy *indexedVectorReadSpy) GetInnerEngine() storage.Engine { return spy.Engine }

func (spy *indexedVectorReadSpy) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	spy.mu.Lock()
	spy.point = append(spy.point, id)
	mutate := spy.mutateOnBatch
	spy.mutateOnBatch = nil
	spy.mu.Unlock()
	if mutate != nil {
		mutate()
	}
	return spy.Engine.GetNode(ctx, id)
}

func (spy *indexedVectorReadSpy) BatchGetNodes(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	spy.mu.Lock()
	spy.batches = append(spy.batches, append([]storage.NodeID(nil), ids...))
	mutate := spy.mutateOnBatch
	spy.mutateOnBatch = nil
	spy.mu.Unlock()
	if mutate != nil {
		mutate()
	}
	return spy.Engine.BatchGetNodes(ctx, ids)
}

// GraphMutationVersionInNamespace keeps the namespace's revision capability
// visible through this spy. A scoped read captures the graph version from the
// engine it reads through, so a decorator that dropped the capability would
// silently disable the guard.
func (spy *indexedVectorReadSpy) GraphMutationVersionInNamespace(namespace string) (uint64, bool) {
	provider, ok := spy.Engine.(storage.NamespaceGraphMutationVersionProvider)
	if !ok {
		return 0, false
	}
	return provider.GraphMutationVersionInNamespace(namespace)
}

func (spy *indexedVectorReadSpy) reset() {
	spy.mu.Lock()
	spy.point, spy.batches = nil, nil
	spy.mu.Unlock()
}

func (spy *indexedVectorReadSpy) mutateOnNextRead(mutate func()) {
	spy.mu.Lock()
	spy.mutateOnBatch = mutate
	spy.mu.Unlock()
}

// reads returns the intercepted reads as database-local node IDs: point reads
// and batches in request order.
func (spy *indexedVectorReadSpy) reads() (point []string, batches [][]string) {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	point = make([]string, 0, len(spy.point))
	for _, id := range spy.point {
		point = append(point, indexedVectorReadLocalID(id))
	}
	batches = make([][]string, 0, len(spy.batches))
	for _, batch := range spy.batches {
		ids := make([]string, 0, len(batch))
		for _, id := range batch {
			ids = append(ids, indexedVectorReadLocalID(id))
		}
		batches = append(batches, ids)
	}
	return point, batches
}

func (spy *indexedVectorReadSpy) pointReads(id string) int {
	point, _ := spy.reads()
	count := 0
	for _, read := range point {
		if read == id {
			count++
		}
	}
	return count
}

func (spy *indexedVectorReadSpy) batched(id string) bool {
	_, batches := spy.reads()
	for _, batch := range batches {
		for _, read := range batch {
			if read == id {
				return true
			}
		}
	}
	return false
}

// indexedVectorReadLocalID drops the namespace a namespaced engine prefixes to
// the IDs its inner engine reads.
func indexedVectorReadLocalID(id storage.NodeID) string {
	return strings.TrimPrefix(string(id), "work:")
}

type fineGrainedVectorReadFixture struct {
	service *Service
	spy     *indexedVectorReadSpy
	store   *storage.NamespacedEngine
	// reader may read every property; scoped reads one property by name, which
	// is the projection that narrows a node's chunk vectors.
	reader auth.FineGrainedRequest
	scoped auth.FineGrainedRequest
}

// newFineGrainedVectorReadFixture holds one Doc corpus whose vector sources
// differ in what a principal may see: every source readable, traversable
// without vector access, and not traversable at all.
func newFineGrainedVectorReadFixture(t *testing.T) *fineGrainedVectorReadFixture {
	t.Helper()
	ctx := t.Context()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	spy := &indexedVectorReadSpy{Engine: base}
	store := storage.NewNamespacedEngine(spy, "work")
	for _, node := range []*storage.Node{
		// Every vector source is readable: main, chunk and named entries.
		{ID: "alpha", Labels: []string{"Doc", "Vectorized"},
			ChunkEmbeddings: [][]float32{{1, .1}, {1, .15}},
			NamedEmbeddings: map[string][]float32{"title": {1, .1}},
			Properties:      map[string]any{"content": "alpha"}},
		// Three readable chunk vectors and no named one.
		{ID: "beta", Labels: []string{"Doc", "Vectorized"},
			ChunkEmbeddings: [][]float32{{1, .2}, {1, .25}, {1, .3}},
			Properties:      map[string]any{"content": "beta", "summary": "beta summary"}},
		// One main vector, a named vector and a property the narrow principal
		// may not read.
		{ID: "sealed", Labels: []string{"Doc", "Vectorized"},
			ChunkEmbeddings: [][]float32{{1, .5}},
			NamedEmbeddings: map[string][]float32{"title": {1, .5}},
			Properties:      map[string]any{"content": "sealed", "secret": "sealed secret"}},
		// Traversable without graph.node.embedding.read: no vector entry visible.
		{ID: "plain", Labels: []string{"Doc"},
			ChunkEmbeddings: [][]float32{{1, .6}},
			Properties:      map[string]any{"content": "plain"}},
		// Vectorized but not traversable: the node itself is invisible.
		{ID: "internal", Labels: []string{"Internal", "Vectorized"},
			ChunkEmbeddings: [][]float32{{1, .01}}},
	} {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	service := NewService(store, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.SetIndexFlags(false, true)
	require.NoError(t, service.BuildIndexes(ctx))

	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "work", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "docs", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Doc"}}},
			{ID: "vectors", Action: "graph.node.embedding.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Vectorized"}}},
			{ID: "all-properties", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "content", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"content"}}},
		},
		Assignments: map[string][]string{
			"reader": {"work", "docs", "vectors", "all-properties"},
			"scoped": {"work", "docs", "vectors", "content"},
		},
	})
	require.NoError(t, err)
	request := func(role string) auth.FineGrainedRequest {
		return auth.FineGrainedRequest{
			Access: policy.ForRoles([]string{role}), DatabaseID: func(string) string { return "work-id" },
		}
	}
	return &fineGrainedVectorReadFixture{
		service: service, spy: spy, store: store,
		reader: request("reader"), scoped: request("scoped"),
	}
}

func indexedVectorMatchIDs(candidates []indexResult) []string {
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.MatchID)
	}
	return ids
}

func TestFineGrainedVectorCandidatesReadOneBatchPerPage(t *testing.T) {
	fixture := newFineGrainedVectorReadFixture(t)
	scoped, scope, err := fixture.service.withIndexedSearchScope(auth.WithFineGrainedRequest(t.Context(), &fixture.reader))
	require.NoError(t, err)
	require.NotNil(t, scope)

	candidates := []indexResult{
		{ID: "alpha", MatchID: nodeMainVectorKey("alpha"), Score: .99},
		{ID: "internal", MatchID: nodeMainVectorKey("internal"), Score: .98},
		{ID: "beta", MatchID: nodeChunkVectorKey("beta", 1), Score: .97},
		{ID: "beta", MatchID: nodeChunkVectorKey("beta", 2), Score: .96},
		{ID: "beta", MatchID: nodeChunkVectorKey("beta", 9), Score: .95},
		{ID: "sealed", MatchID: nodeMainVectorKey("sealed"), Score: .94},
		{ID: "sealed", MatchID: nodeChunkVectorKey("sealed", 1), Score: .93},
		{ID: "beta", MatchID: nodeNamedVectorKey("beta", "title"), Score: .92},
		{ID: "alpha", MatchID: nodeNamedVectorKey("alpha", "title"), Score: .91},
		{ID: "alpha", MatchID: nodeNamedVectorKey("alpha", "absent"), Score: .9},
		{ID: "sealed", MatchID: nodePropertyVectorKey("sealed", "secret"), Score: .89},
		{ID: "plain", MatchID: nodeMainVectorKey("plain"), Score: .88},
		{ID: "ghost", MatchID: nodeMainVectorKey("ghost"), Score: .87},
		// Entries the candidate does not own are never read.
		{ID: "unrequested", MatchID: nodeMainVectorKey("alpha"), Score: .86},
		{ID: "unrequested", MatchID: "", Score: .85},
	}

	fixture.spy.reset()
	admitted, err := fixture.service.filterIndexedVectors(scoped, candidates)
	require.NoError(t, err)

	require.Equal(t, []string{
		nodeMainVectorKey("alpha"),
		nodeChunkVectorKey("beta", 1),
		nodeChunkVectorKey("beta", 2),
		nodeMainVectorKey("sealed"),
		nodeNamedVectorKey("alpha", "title"),
		nodePropertyVectorKey("sealed", "secret"),
	}, indexedVectorMatchIDs(admitted), "admission keeps the candidate order and each entry's own source visibility")

	point, batches := fixture.spy.reads()
	require.Empty(t, point, "an authorized vector page must not point-read its candidates")
	require.Equal(t, [][]string{{"alpha", "internal", "beta", "sealed", "plain", "ghost"}}, batches,
		"one batch carries each distinct candidate once, in candidate order")
}

func TestFineGrainedVectorCandidatesKeepPerEntrySourceVisibility(t *testing.T) {
	fixture := newFineGrainedVectorReadFixture(t)
	scoped, scope, err := fixture.service.withIndexedSearchScope(auth.WithFineGrainedRequest(t.Context(), &fixture.scoped))
	require.NoError(t, err)
	require.NotNil(t, scope)

	// A property-scoped grant narrows the projected image to the properties it
	// names, so the same node admits its readable property and named vector
	// entries while its chunk entry stays out of reach.
	candidates := []indexResult{
		{ID: "sealed", MatchID: nodeMainVectorKey("sealed"), Score: .99},
		{ID: "sealed", MatchID: nodeNamedVectorKey("sealed", "title"), Score: .98},
		{ID: "sealed", MatchID: nodePropertyVectorKey("sealed", "content"), Score: .97},
		{ID: "sealed", MatchID: nodePropertyVectorKey("sealed", "secret"), Score: .96},
		{ID: "alpha", MatchID: nodeMainVectorKey("alpha"), Score: .95},
		{ID: "alpha", MatchID: nodeNamedVectorKey("alpha", "title"), Score: .94},
	}

	fixture.spy.reset()
	admitted, err := fixture.service.filterIndexedVectors(scoped, candidates)
	require.NoError(t, err)
	require.Equal(t, []string{
		nodeNamedVectorKey("sealed", "title"),
		nodePropertyVectorKey("sealed", "content"),
		nodeNamedVectorKey("alpha", "title"),
	}, indexedVectorMatchIDs(admitted))

	point, batches := fixture.spy.reads()
	require.Empty(t, point)
	require.Equal(t, [][]string{{"sealed", "alpha"}}, batches, "one image per distinct candidate serves every entry")
}

func TestFineGrainedVectorCandidatesReadDistinctOwnersWithoutBatchMaps(t *testing.T) {
	fixture := newFineGrainedVectorReadFixture(t)
	scoped, scope, err := fixture.service.withIndexedSearchScope(auth.WithFineGrainedRequest(t.Context(), &fixture.reader))
	require.NoError(t, err)
	require.NotNil(t, scope)

	// A page larger than one read bound crosses batches in candidate order.
	total := 2*indexedVectorReadBatchSize + 5
	candidates := make([]indexResult, 0, total)
	expectedIDs := make([]string, 0, total)
	for index := range total {
		id := fmt.Sprintf("missing-%04d", index)
		matchID := nodeMainVectorKey(id)
		switch index {
		case 0:
			id, matchID = "alpha", nodeMainVectorKey("alpha")
		case total - 1:
			id, matchID = "beta", nodeChunkVectorKey("beta", 1)
		}
		candidates = append(candidates, indexResult{ID: id, MatchID: matchID, Score: 1 - float64(index)/float64(total)})
		expectedIDs = append(expectedIDs, id)
	}

	fixture.spy.reset()
	admitted, err := fixture.service.filterIndexedVectors(scoped, candidates)
	require.NoError(t, err)
	require.Equal(t, []string{nodeMainVectorKey("alpha"), nodeChunkVectorKey("beta", 1)}, indexedVectorMatchIDs(admitted))

	point, batches := fixture.spy.reads()
	require.Empty(t, batches, "unique owners avoid batch result maps")
	require.Equal(t, expectedIDs, point, "point reads preserve candidate order")
}

func TestFineGrainedVectorCandidateReadsObserveCancellation(t *testing.T) {
	fixture := newFineGrainedVectorReadFixture(t)
	live, scope, err := fixture.service.withIndexedSearchScope(auth.WithFineGrainedRequest(t.Context(), &fixture.reader))
	require.NoError(t, err)
	require.NotNil(t, scope)
	cancelled, cancel := context.WithCancel(live)
	cancel()

	fixture.spy.reset()
	admitted, err := fixture.service.filterIndexedVectors(cancelled, []indexResult{
		{ID: "alpha", MatchID: nodeMainVectorKey("alpha"), Score: .99},
		{ID: "beta", MatchID: nodeChunkVectorKey("beta", 1), Score: .98},
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, admitted)
	point, batches := fixture.spy.reads()
	require.Empty(t, point)
	require.Empty(t, batches, "a canceled page reads no candidate image")
}

func TestFineGrainedVectorVisibilityReusesOnlyMatchingRevisionAndOwner(t *testing.T) {
	fixture := newFineGrainedVectorReadFixture(t)
	fixture.reader.Fingerprint = "reader-policy"
	query := func() ([]indexResult, error) {
		ctx, _, err := fixture.service.withIndexedSearchScope(auth.WithFineGrainedRequest(t.Context(), &fixture.reader))
		if err != nil {
			return nil, err
		}
		return fixture.service.filterIndexedVectors(ctx, []indexResult{
			{ID: "alpha", MatchID: nodeMainVectorKey("alpha")},
			{ID: "forged", MatchID: nodeMainVectorKey("alpha")},
			{ID: "internal", MatchID: nodeMainVectorKey("internal")},
		})
	}
	first, err := query()
	require.NoError(t, err)
	require.Equal(t, []string{nodeMainVectorKey("alpha")}, indexedVectorMatchIDs(first))
	fixture.spy.reset()
	second, err := query()
	require.NoError(t, err)
	require.Equal(t, indexedVectorMatchIDs(first), indexedVectorMatchIDs(second))
	point, batches := fixture.spy.reads()
	require.Empty(t, point)
	require.Empty(t, batches, "stable policy and graph reuse candidate authorization")

	_, err = fixture.store.CreateNode(t.Context(), &storage.Node{ID: "later", Labels: []string{"Doc"}})
	require.NoError(t, err)
	fixture.spy.reset()
	third, err := query()
	require.NoError(t, err)
	require.Equal(t, indexedVectorMatchIDs(first), indexedVectorMatchIDs(third))
	point, batches = fixture.spy.reads()
	require.NotZero(t, len(point)+len(batches), "a graph revision rechecks authorization")

	// A caller-supplied fingerprint is insufficient to identify a policy: the
	// effective rules differ and must not reuse the previous decisions.
	fixture.scoped.Fingerprint = fixture.reader.Fingerprint
	narrow, _, err := fixture.service.withIndexedSearchScope(auth.WithFineGrainedRequest(t.Context(), &fixture.scoped))
	require.NoError(t, err)
	fixture.spy.reset()
	scoped, err := fixture.service.filterIndexedVectors(narrow, []indexResult{
		{ID: "sealed", MatchID: nodeMainVectorKey("sealed")},
		{ID: "sealed", MatchID: nodeNamedVectorKey("sealed", "title")},
	})
	require.NoError(t, err)
	require.Equal(t, []string{nodeNamedVectorKey("sealed", "title")}, indexedVectorMatchIDs(scoped))
	point, batches = fixture.spy.reads()
	require.NotZero(t, len(point)+len(batches), "a different policy with the same fingerprint must be read")
}

func TestFineGrainedVectorVisibilityDoesNotCacheDecayReads(t *testing.T) {
	fixture := newFineGrainedVectorReadFixture(t)
	fixture.reader.Fingerprint = "reader-policy"
	owner, ok := storage.UnwrapEngine(fixture.store).(*storage.BadgerEngine)
	require.True(t, ok)
	owner.SetDecayEnabled(true)
	query := func() error {
		ctx, _, err := fixture.service.withIndexedSearchScope(auth.WithFineGrainedRequest(t.Context(), &fixture.reader))
		if err != nil {
			return err
		}
		_, err = fixture.service.filterIndexedVectors(ctx, []indexResult{{ID: "alpha", MatchID: nodeMainVectorKey("alpha")}})
		return err
	}
	require.NoError(t, query())
	fixture.spy.reset()
	require.NoError(t, query())
	point, batches := fixture.spy.reads()
	require.NotZero(t, len(point)+len(batches), "time-based decay requires fresh authorization")
}

func TestFineGrainedVectorVisibilityDoesNotCacheManualSuppressionReads(t *testing.T) {
	fixture := newFineGrainedVectorReadFixture(t)
	fixture.reader.Fingerprint = "reader-policy"
	owner, ok := storage.UnwrapEngine(fixture.store).(*storage.BadgerEngine)
	require.True(t, ok)
	require.NoError(t, owner.SetManualSuppression(t.Context(), storage.StoredEntityNodeKind, "work:beta", true))
	query := func() error {
		ctx, _, err := fixture.service.withIndexedSearchScope(auth.WithFineGrainedRequest(t.Context(), &fixture.reader))
		if err != nil {
			return err
		}
		_, err = fixture.service.filterIndexedVectors(ctx, []indexResult{{ID: "alpha", MatchID: nodeMainVectorKey("alpha")}})
		return err
	}
	require.NoError(t, query())
	fixture.spy.reset()
	require.NoError(t, query())
	point, batches := fixture.spy.reads()
	require.NotZero(t, len(point)+len(batches), "query-local reveal can change manual suppression visibility")
}

func TestFineGrainedVectorSearchUsesAuthorizedCandidateReads(t *testing.T) {
	fixture := newFineGrainedVectorReadFixture(t)
	response, err := fixture.service.Search(auth.WithFineGrainedRequest(t.Context(), &fixture.reader), "",
		[]float32{1, 0}, &SearchOptions{Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "beta"}, orderedIDs(response.Results),
		"the visible page keeps its ranking and discloses no unreadable candidate")
	require.True(t, fixture.spy.batched("internal") || fixture.spy.pointReads("internal") > 0,
		"the candidate filter checks an invisible candidate through the projected graph")
}

func TestFineGrainedVectorSearchPreservesGraphVersionGuard(t *testing.T) {
	fixture := newFineGrainedVectorReadFixture(t)
	before, supported := fixture.store.GraphMutationVersion()
	require.True(t, supported, "the fixture's graph reports its mutation version")

	var mutationErr error
	fixture.spy.mutateOnNextRead(func() {
		_, mutationErr = fixture.store.CreateNode(t.Context(), &storage.Node{ID: "late", Labels: []string{"Doc"},
			ChunkEmbeddings: [][]float32{{1, 0}}})
	})
	response, err := fixture.service.Search(auth.WithFineGrainedRequest(t.Context(), &fixture.reader), "",
		[]float32{1, 0}, &SearchOptions{Limit: 2})
	require.NoError(t, mutationErr)
	require.ErrorIs(t, err, resultstream.ErrInvalidated,
		"a graph change during the candidate read invalidates the page it would have described")
	require.Nil(t, response)
	after, supported := fixture.store.GraphMutationVersion()
	require.True(t, supported)
	require.Greater(t, after, before)
	point, batches := fixture.spy.reads()
	require.True(t, len(point)+len(batches) > 0, "the mutation belongs to an authorized candidate read")
}
