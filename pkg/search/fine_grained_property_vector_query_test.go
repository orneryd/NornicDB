package search

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A restricted property-vector query admits the indexed population before
// scoring, so unreadable vectors cannot affect ranking work or results. Stable
// policy and graph revisions may reuse those admission decisions.

// fineGrainedPropertyVectorFixture is one Doc population whose "vec" property
// vectors only a PublicVector grant may read: the three highest scores and a
// sixty-node tail sit on Doc nodes outside it.
type fineGrainedPropertyVectorFixture struct {
	service *Service
	spy     *indexedVectorReadSpy
	visible map[string]struct{}
	hidden  []string
	tail    []string
	wide    auth.FineGrainedRequest
	narrow  auth.FineGrainedRequest
}

func newFineGrainedPropertyVectorFixture(t *testing.T) *fineGrainedPropertyVectorFixture {
	t.Helper()
	ctx := t.Context()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	spy := &indexedVectorReadSpy{Engine: base}
	store := storage.NewNamespacedEngine(spy, "work")

	nodes := []*storage.Node{
		// The highest-scoring property vectors, on nodes whose "vec" property
		// is outside the request's grant.
		{ID: "sealed-a", Labels: []string{"Doc"}, Properties: map[string]any{"vec": []float32{1, 0}}},
		{ID: "sealed-b", Labels: []string{"Doc"}, Properties: map[string]any{"vec": []float32{1, .01}}},
		{ID: "sealed-c", Labels: []string{"Doc"}, Properties: map[string]any{"vec": []float32{1, .02}}},
		// A readable property vector beside a named vector of the queried
		// name: the candidate loop reads the named source first and the
		// supplement contributes the property score.
		{ID: "mixed-named", Labels: []string{"Doc", "PublicVector"},
			NamedEmbeddings: map[string][]float32{"vec": {0, 1}},
			Properties:      map[string]any{"vec": []float32{1, .05}}},
		{ID: "visible-a", Labels: []string{"Doc", "PublicVector"}, Properties: map[string]any{"vec": []float32{1, .1}}},
		{ID: "visible-b", Labels: []string{"Doc", "PublicVector"}, Properties: map[string]any{"vec": []float32{1, .2}}},
		{ID: "visible-c", Labels: []string{"Doc", "PublicVector"}, Properties: map[string]any{"vec": []float32{1, .3}}},
		// A named vector under another name leaves the property vector the
		// source precedence reads.
		{ID: "mixed-other-name", Labels: []string{"Doc", "PublicVector"},
			NamedEmbeddings: map[string][]float32{"title": {1, .4}},
			Properties:      map[string]any{"vec": []float32{1, .6}}},
	}
	tail := make([]string, 0, 60)
	for i := range 60 {
		id := fmt.Sprintf("far-%03d", i)
		tail = append(tail, id)
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"},
			Properties: map[string]any{"vec": []float32{1, float32(40 + i)}}})
	}
	for _, node := range nodes {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}

	service := NewService(store, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.SetIndexFlags(false, true)
	require.NoError(t, service.BuildIndexes(ctx))

	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "docs", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Doc"}}},
			{ID: "embeddings", Action: "graph.node.embedding.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Doc"}}},
			{ID: "vector-source", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"PublicVector"}, Properties: []string{"vec"}}},
		},
		Assignments: map[string][]string{
			"wide":   {"access", "docs", "embeddings", "vector-source"},
			"narrow": {"access", "docs", "embeddings"},
		},
	})
	require.NoError(t, err)
	request := func(role, fingerprint string) auth.FineGrainedRequest {
		return auth.FineGrainedRequest{
			Access: policy.ForRoles([]string{role}), DatabaseID: func(string) string { return "work-id" },
			Fingerprint: fingerprint,
		}
	}
	visible := map[string]struct{}{
		"mixed-named": {}, "visible-a": {}, "visible-b": {}, "visible-c": {}, "mixed-other-name": {},
	}
	return &fineGrainedPropertyVectorFixture{
		service: service, spy: spy, visible: visible,
		hidden: []string{"sealed-a", "sealed-b", "sealed-c"}, tail: tail,
		wide: request("wide", "policy-wide"), narrow: request("narrow", "policy-narrow"),
	}
}

func propertyVectorQueryIDs(hits []VectorQueryHit) []string {
	ids := make([]string, 0, len(hits))
	for _, hit := range hits {
		ids = append(ids, hit.ID)
	}
	return ids
}

// readIDs returns every node image the fine-grained read resolved, point reads
// and batches together, as database-local IDs.
func (fixture *fineGrainedPropertyVectorFixture) readIDs() []string {
	point, batches := fixture.spy.reads()
	ids := append([]string{}, point...)
	for _, batch := range batches {
		ids = append(ids, batch...)
	}
	return ids
}

func TestFineGrainedPropertyVectorQueryAdmitsBeforeScoring(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_VECTOR_GPU_ENABLED", "false")
	ctx := t.Context()
	fixture := newFineGrainedPropertyVectorFixture(t)
	query := []float32{1, 0}
	spec := VectorQuerySpec{Label: "Doc", Property: "vec", Limit: 3}

	fixture.spy.reset()
	hits, err := fixture.service.VectorQueryNodes(auth.WithFineGrainedRequest(ctx, &fixture.wide), query, spec)
	require.NoError(t, err)

	// The three highest-scoring vectors belong to nodes outside the grant, so
	// the page is the three best vectors the request may read: complete,
	// ordered, and free of the hidden IDs.
	require.Equal(t, []string{"mixed-named", "visible-a", "visible-b"}, propertyVectorQueryIDs(hits))
	for i := 1; i < len(hits); i++ {
		require.Greater(t, hits[i-1].Score, hits[i].Score, "hits are ordered by descending score")
	}
	for _, hidden := range append(append([]string{}, fixture.hidden...), fixture.tail...) {
		require.NotContains(t, propertyVectorQueryIDs(hits), hidden)
	}

	// The visible page is the unrestricted page's visible prefix: the score
	// scale, the per-node source the query reads (named before property) and
	// the order are the same with and without the scope.
	reference, err := fixture.service.VectorQueryNodesWithOptions(ctx, query,
		VectorQuerySpec{Label: "Doc", Property: "vec", Limit: len(fixture.visible) + len(fixture.hidden) + len(fixture.tail)},
		&SearchOptions{CandidateTarget: len(fixture.visible) + len(fixture.hidden) + len(fixture.tail)})
	require.NoError(t, err)
	unrestricted := make([]VectorQueryHit, 0, spec.Limit)
	for _, hit := range reference {
		if _, ok := fixture.visible[hit.ID]; !ok {
			continue
		}
		unrestricted = append(unrestricted, hit)
		if len(unrestricted) == spec.Limit {
			break
		}
	}
	require.Len(t, unrestricted, spec.Limit)
	for i := range hits {
		require.Equal(t, unrestricted[i].ID, hits[i].ID)
		require.InDelta(t, unrestricted[i].Score, hits[i].Score, 1e-9)
	}

	// Cold admission checks the entire indexed population before any vector
	// score is computed; its cost is amortized by a graph/policy-bound view.
	readIDs := fixture.readIDs()
	require.Contains(t, readIDs, fixture.tail[len(fixture.tail)-1])
	fixture.spy.reset()
	repeated, err := fixture.service.VectorQueryNodes(auth.WithFineGrainedRequest(ctx, &fixture.wide), query, spec)
	require.NoError(t, err)
	require.Equal(t, hits, repeated)
	warmReads := fixture.readIDs()
	require.LessOrEqual(t, len(warmReads), 10, "warm reads should resolve only result candidates: %v", warmReads)
	for _, id := range warmReads {
		require.NotContains(t, fixture.tail, id, "unchanged policy and graph should reuse population admission")
	}
}

// A request cancelled while the page is authorized ends with the context's
// error rather than a partial page.
func TestFineGrainedPropertyVectorQueryStopsOnCancellation(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_VECTOR_GPU_ENABLED", "false")
	fixture := newFineGrainedPropertyVectorFixture(t)
	ctx, cancel := context.WithCancel(auth.WithFineGrainedRequest(t.Context(), &fixture.wide))
	defer cancel()

	fixture.spy.reset()
	// The first authorized read of the page cancels the request.
	fixture.spy.mutateOnNextRead(cancel)
	hits, err := fixture.service.VectorQueryNodes(ctx, []float32{1, 0}, VectorQuerySpec{Label: "Doc", Property: "vec", Limit: 3})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, hits)
}

// Admission decisions are bound to the policy that made them: a later, narrower
// policy re-reads the population instead of replaying the wider page, and the
// wider request still gets its own page afterwards.
func TestFineGrainedPropertyVectorQueryKeepsPoliciesApart(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_VECTOR_GPU_ENABLED", "false")
	ctx := t.Context()
	fixture := newFineGrainedPropertyVectorFixture(t)
	query := []float32{1, 0}
	spec := VectorQuerySpec{Label: "Doc", Property: "vec", Limit: 3}

	wide, err := fixture.service.VectorQueryNodes(auth.WithFineGrainedRequest(ctx, &fixture.wide), query, spec)
	require.NoError(t, err)
	require.Equal(t, []string{"mixed-named", "visible-a", "visible-b"}, propertyVectorQueryIDs(wide))

	// Without the "vec" property grant no property vector is readable: the
	// only source left is mixed-named's named vector under the queried name.
	narrow, err := fixture.service.VectorQueryNodes(auth.WithFineGrainedRequest(ctx, &fixture.narrow), query, spec)
	require.NoError(t, err)
	require.Equal(t, []string{"mixed-named"}, propertyVectorQueryIDs(narrow))
	require.InDelta(t, 0.5, narrow[0].Score, 1e-9, "named vector cosine 0 on the Neo4j scale")

	repeated, err := fixture.service.VectorQueryNodes(auth.WithFineGrainedRequest(ctx, &fixture.wide), query, spec)
	require.NoError(t, err)
	require.Equal(t, propertyVectorQueryIDs(wide), propertyVectorQueryIDs(repeated))
	for i := range wide {
		require.InDelta(t, wide[i].Score, repeated[i].Score, 1e-9)
	}
}

func TestFineGrainedPropertyVectorQueryBackfillsAfterWhere(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_VECTOR_GPU_ENABLED", "false")
	fixture := newFineGrainedPropertyVectorFixture(t)
	ctx := auth.WithFineGrainedRequest(t.Context(), &fixture.wide)
	hits, err := fixture.service.VectorQueryNodesWithOptions(ctx, []float32{1, 0},
		VectorQuerySpec{Label: "Doc", Property: "vec", Limit: 1},
		&SearchOptions{Where: func(_ context.Context, node *storage.Node) (bool, error) {
			return node.ID == "visible-c", nil
		}})
	require.NoError(t, err)
	require.Equal(t, []string{"visible-c"}, propertyVectorQueryIDs(hits), "Where must backfill past a higher ranked visible hit")
}
