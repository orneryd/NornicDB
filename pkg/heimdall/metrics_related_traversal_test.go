package heimdall

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/relatedwalk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// relatedWalkSearcher is an instrumented SemanticSearcher: it returns edges in
// the exact order it is given and records every read of the related-node walk.
type relatedWalkSearcher struct {
	edges     map[string][]*GraphEdge
	nodes     map[string]*NodeData
	search    []*SemanticSearchResult
	missing   map[string]bool
	onNode    func(loaded int)
	edgeErr   map[string]error
	edgeReads []string
	nodeReads []string
}

func (s *relatedWalkSearcher) HybridSearch(context.Context, string, []float32, []string, int) ([]*SemanticSearchResult, error) {
	return s.search, nil
}

func (s *relatedWalkSearcher) Search(context.Context, string, []string, int) ([]*SemanticSearchResult, error) {
	return s.search, nil
}

func (s *relatedWalkSearcher) Neighbors(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s *relatedWalkSearcher) GetEdgesForNode(_ context.Context, nodeID string) ([]*GraphEdge, error) {
	s.edgeReads = append(s.edgeReads, nodeID)
	return s.edges[nodeID], s.edgeErr[nodeID]
}

func (s *relatedWalkSearcher) GetNode(_ context.Context, nodeID string) (*NodeData, error) {
	s.nodeReads = append(s.nodeReads, nodeID)
	if s.onNode != nil {
		s.onNode(len(s.nodeReads))
	}
	if s.missing[nodeID] {
		return nil, errors.New("node not found")
	}
	node := s.nodes[nodeID]
	if node == nil {
		return nil, errors.New("node not found")
	}
	return node, nil
}

func newRelatedWalkSearcher() *relatedWalkSearcher {
	return &relatedWalkSearcher{
		edges:   map[string][]*GraphEdge{},
		nodes:   map[string]*NodeData{},
		missing: map[string]bool{},
		edgeErr: map[string]error{},
	}
}

func (s *relatedWalkSearcher) addNode(id string, properties map[string]interface{}, labels ...string) {
	if len(labels) == 0 {
		labels = []string{"Doc"}
	}
	s.nodes[id] = &NodeData{ID: id, Labels: labels, Properties: properties}
}

func (s *relatedWalkSearcher) addEdge(id, edgeType, from, to string) {
	s.edges[from] = append(s.edges[from], &GraphEdge{ID: id, Type: edgeType, SourceID: from, TargetID: to})
	s.edges[to] = append(s.edges[to], &GraphEdge{ID: id, Type: edgeType, SourceID: from, TargetID: to})
}

func TestQueryExecutorRelatedNodesPreservesSearcherOrder(t *testing.T) {
	searcher := newRelatedWalkSearcher()
	searcher.addNode("root", nil)
	searcher.addNode("a", map[string]interface{}{"title": "A"}, "First")
	searcher.addNode("c", map[string]interface{}{"title": "C"})
	searcher.addNode("b", map[string]interface{}{"title": "B"})
	// Deliberately interleaved and not sorted by direction: the walk must follow
	// the searcher's own GetEdgesForNode order and derive direction per edge.
	searcher.edges["root"] = []*GraphEdge{
		{ID: "e1", Type: "OUT", SourceID: "root", TargetID: "a"},
		{ID: "e2", Type: "IN", SourceID: "c", TargetID: "root"},
		{ID: "e3", Type: "OUT2", SourceID: "root", TargetID: "b"},
	}

	exec := NewQueryExecutorWithSearch(nil, searcher, nil, time.Second)
	related := exec.getRelatedNodes(context.Background(), "root", 1)

	require.Len(t, related, 3)
	assert.Equal(t, []string{"a", "c", "b"}, relatedIDs(related))
	assert.Equal(t, []string{"outgoing", "incoming", "outgoing"}, relatedDirections(related))
	assert.Equal(t, []string{"OUT", "IN", "OUT2"}, []string{related[0].Relationship, related[1].Relationship, related[2].Relationship})
	assert.Equal(t, "First", related[0].Type)
	assert.Equal(t, "A", related[0].Title)
	assert.Equal(t, []int{1, 1, 1}, relatedDistances(related))
}

func TestQueryExecutorRelatedNodesDepthBoundary(t *testing.T) {
	searcher := newRelatedWalkSearcher()
	for _, id := range []string{"root", "a", "b", "c"} {
		searcher.addNode(id, nil)
	}
	searcher.addEdge("e1", "LINK", "root", "a")
	searcher.addEdge("e2", "LINK", "a", "b")
	searcher.addEdge("e3", "LINK", "b", "c")

	for _, tc := range []struct {
		depth     int
		want      []string
		edgeReads []string
	}{
		{depth: 1, want: []string{"a"}, edgeReads: []string{"root"}},
		{depth: 2, want: []string{"a", "b"}, edgeReads: []string{"root", "a"}},
		{depth: 3, want: []string{"a", "b", "c"}, edgeReads: []string{"root", "a", "b"}},
	} {
		fresh := newRelatedWalkSearcher()
		fresh.edges = searcher.edges
		fresh.nodes = searcher.nodes
		exec := NewQueryExecutorWithSearch(nil, fresh, nil, time.Second)
		related := exec.getRelatedNodes(context.Background(), "root", tc.depth)
		assert.Equal(t, tc.want, relatedIDs(related), "depth=%d", tc.depth)
		assert.Equal(t, tc.edgeReads, fresh.edgeReads, "depth=%d", tc.depth)
	}
}

func TestQueryExecutorRelatedNodesSkipMissingWithoutExpanding(t *testing.T) {
	searcher := newRelatedWalkSearcher()
	searcher.addNode("root", nil)
	searcher.addNode("gone", nil)
	searcher.addNode("child", nil)
	searcher.addNode("kept", nil)
	searcher.addEdge("e1", "LINK", "root", "gone")
	searcher.addEdge("e2", "LINK", "gone", "child")
	searcher.addEdge("e3", "LINK", "root", "kept")
	searcher.missing["gone"] = true

	exec := NewQueryExecutorWithSearch(nil, searcher, nil, time.Second)
	related := exec.getRelatedNodes(context.Background(), "root", 3)

	require.Len(t, related, 1)
	assert.Equal(t, "kept", related[0].ID)
	assert.NotContains(t, searcher.edgeReads, "gone", "a skipped missing node is not expanded")
	assert.NotContains(t, searcher.nodeReads, "child")
}

func TestQueryExecutorRelatedNodesCapInsideOneAdjacency(t *testing.T) {
	searcher := newRelatedWalkSearcher()
	searcher.addNode("root", nil)
	for i := 0; i < 130; i++ {
		id := fmt.Sprintf("n-%03d", i)
		searcher.addNode(id, nil)
		searcher.edges["root"] = append(searcher.edges["root"], &GraphEdge{
			ID:       fmt.Sprintf("e-%03d", i),
			Type:     "LINK",
			SourceID: "root",
			TargetID: id,
		})
	}

	exec := NewQueryExecutorWithSearch(nil, searcher, nil, time.Second)
	related := exec.getRelatedNodes(context.Background(), "root", 2)

	require.Len(t, related, 100, "the 100-record cap applies inside a single adjacency")
	assert.Equal(t, "n-000", related[0].ID)
	assert.Equal(t, "n-099", related[99].ID)
	assert.Equal(t, []string{"root"}, searcher.edgeReads, "no adjacency is read after the cap")
	assert.Len(t, searcher.nodeReads, 100, "no node is loaded after the cap")
}

func TestQueryExecutorRelatedNodesCancellationPublishesNothing(t *testing.T) {
	searcher := newRelatedWalkSearcher()
	searcher.addNode("root", nil)
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("n-%02d", i)
		searcher.addNode(id, nil)
		searcher.edges["root"] = append(searcher.edges["root"], &GraphEdge{
			ID:       fmt.Sprintf("e-%02d", i),
			Type:     "LINK",
			SourceID: "root",
			TargetID: id,
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	searcher.onNode = func(loaded int) {
		if loaded == 5 {
			cancel()
		}
	}

	exec := NewQueryExecutorWithSearch(nil, searcher, nil, time.Second)
	related := exec.getRelatedNodes(ctx, "root", 2)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Nil(t, related, "a canceled walk must not publish a partial collection")
}

func TestQueryExecutorRelatedNodesDeadlinePublishesNothing(t *testing.T) {
	searcher := newRelatedWalkSearcher()
	searcher.addNode("root", nil)
	searcher.addNode("a", nil)
	searcher.addEdge("e1", "LINK", "root", "a")

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	exec := NewQueryExecutorWithSearch(nil, searcher, nil, time.Second)
	assert.Nil(t, exec.getRelatedNodes(ctx, "root", 2))
}

// TestQueryExecutorDiscoverRelatedNodesAreCanonical checks the exported Discover
// surface: depth 1 publishes no related nodes, depth 2 publishes the direct
// neighbor of the search hit with the stored identity.
func TestQueryExecutorDiscoverRelatedNodesAreCanonical(t *testing.T) {
	searcher := newRelatedWalkSearcher()
	searcher.addNode("root", map[string]interface{}{"title": "Root"}, "Doc")
	searcher.addNode("a", map[string]interface{}{"title": "Child"}, "Child")
	searcher.addNode("b", map[string]interface{}{"title": "Grandchild"})
	searcher.addEdge("e1", "LINK", "root", "a")
	searcher.addEdge("e2", "NEXT", "a", "b")
	searcher.search = []*SemanticSearchResult{{
		ID:         "root",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"title": "Root", "content": strings.Repeat("root document ", 40)},
		Score:      0.9,
	}}

	exec := NewQueryExecutorWithSearch(&coverageQueryDB{}, searcher, nil, time.Second)
	direct, err := exec.Discover(context.Background(), "root document", nil, 5, 1)
	require.NoError(t, err)
	require.Len(t, direct.Results, 1)
	assert.Empty(t, direct.Results[0].Related, "depth 1 publishes no related nodes")

	deeper, err := exec.Discover(context.Background(), "root document", nil, 5, 2)
	require.NoError(t, err)
	require.Len(t, deeper.Results, 1)
	related := deeper.Results[0].Related
	require.Len(t, related, 2)
	assert.Equal(t, "a", related[0].ID)
	assert.Equal(t, 1, related[0].Distance)
	assert.Equal(t, "outgoing", related[0].Direction)
	assert.Equal(t, "LINK", related[0].Relationship)
	assert.Equal(t, "Child", related[0].Type)
	assert.Equal(t, "Child", related[0].Title)
	assert.Equal(t, "b", related[1].ID)
	assert.Equal(t, 2, related[1].Distance)
	assert.Equal(t, "NEXT", related[1].Relationship)
}

// TestQueryExecutorRelatedNodesDiscardsAnErroredEdgeList preserves the
// query-executor contract: a node whose edge list fails contributes no neighbor,
// even though the searcher returned rows with the error.
func TestQueryExecutorRelatedNodesDiscardsAnErroredEdgeList(t *testing.T) {
	searcher := newRelatedWalkSearcher()
	for _, id := range []string{"root", "a", "b"} {
		searcher.addNode(id, nil)
	}
	searcher.addEdge("e1", "LINK", "root", "a")
	searcher.addEdge("e2", "LINK", "root", "b")
	searcher.edgeErr["root"] = errors.New("edges read failed")

	exec := NewQueryExecutorWithSearch(nil, searcher, nil, time.Second)
	related := exec.getRelatedNodes(context.Background(), "root", 2)

	assert.Empty(t, related, "the errored edge list is discarded")
	assert.Equal(t, []string{"root"}, searcher.edgeReads)
	assert.Empty(t, searcher.nodeReads)
}

// countingStepVisitor counts the records an adapter reports to the walk.
type countingStepVisitor struct {
	steps int
}

func (v *countingStepVisitor) Step(_, _ string, _ relatedwalk.Direction) bool {
	v.steps++
	return true
}

// TestRelatedSearcherSourceReportsUnusableEdges is the searcher half of the
// bounded cancellation: a nil edge still reaches the walk, so a list of unusable
// edges cannot bypass the amortized poll.
func TestRelatedSearcherSourceReportsUnusableEdges(t *testing.T) {
	searcher := newRelatedWalkSearcher()
	searcher.edges["root"] = make([]*GraphEdge, 20000)
	visitor := &countingStepVisitor{}

	require.NoError(t, relatedSearcherSource{searcher: searcher}.EachIncidentEdge(context.Background(), "root", visitor))
	assert.Equal(t, 20000, visitor.steps)
}

// TestQueryExecutorRelatedNodesCancellationBoundsWideAdjacency is the query
// executor's fail-first case: every neighbor is missing, so nothing is published,
// the 100-record cap never applies and only the amortized poll can bound the
// node reads after the cancellation.
func TestQueryExecutorRelatedNodesCancellationBoundsWideAdjacency(t *testing.T) {
	const (
		width    = 20480
		cancelAt = 100
		bounds   = 256 // relatedwalk's documented amortized poll interval
	)
	searcher := newRelatedWalkSearcher()
	searcher.addNode("root", nil)
	for i := 0; i < width; i++ {
		id := fmt.Sprintf("missing-%05d", i)
		searcher.edges["root"] = append(searcher.edges["root"], &GraphEdge{
			ID:       fmt.Sprintf("e-%05d", i),
			Type:     "LINK",
			SourceID: "root",
			TargetID: id,
		})
		searcher.missing[id] = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	searcher.onNode = func(loaded int) {
		if loaded == cancelAt {
			cancel()
		}
	}

	exec := NewQueryExecutorWithSearch(nil, searcher, nil, time.Second)
	related := exec.getRelatedNodes(ctx, "root", 2)

	assert.Equal(t, []string{"root"}, searcher.edgeReads, "no adjacency is read after the cancellation")
	require.GreaterOrEqual(t, len(searcher.nodeReads), cancelAt)
	assert.LessOrEqual(t, len(searcher.nodeReads), cancelAt+bounds,
		"post-cancellation node reads must stay inside the documented poll interval")
	assert.Nil(t, related, "a canceled walk must not publish a partial collection")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func relatedIDs(related []RelatedNode) []string {
	ids := make([]string, 0, len(related))
	for _, node := range related {
		ids = append(ids, node.ID)
	}
	return ids
}

func relatedDirections(related []RelatedNode) []string {
	directions := make([]string, 0, len(related))
	for _, node := range related {
		directions = append(directions, node.Direction)
	}
	return directions
}

func relatedDistances(related []RelatedNode) []int {
	distances := make([]int, 0, len(related))
	for _, node := range related {
		distances = append(distances, node.Distance)
	}
	return distances
}
