package relatedwalk

import (
	"context"
	"errors"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stepCollector records the adapter's edge stream in order.
type stepCollector struct {
	steps []fakeEdge
}

func (c *stepCollector) Step(neighborID, relationship string, direction Direction) bool {
	c.steps = append(c.steps, fakeEdge{neighbor: neighborID, relationship: relationship, direction: direction})
	return true
}

// controlledAdjacencyEngine replaces the two adjacency reads of a real engine,
// so a test can control the exact rows and error each list reports.
type controlledAdjacencyEngine struct {
	storage.Engine
	out      []*storage.Edge
	outErr   error
	in       []*storage.Edge
	inErr    error
	outCalls int
	inCalls  int
	onOut    func()
}

func (e *controlledAdjacencyEngine) GetOutgoingEdges(context.Context, storage.NodeID) ([]*storage.Edge, error) {
	e.outCalls++
	if e.onOut != nil {
		e.onOut()
	}
	return e.out, e.outErr
}

func (e *controlledAdjacencyEngine) GetIncomingEdges(context.Context, storage.NodeID) ([]*storage.Edge, error) {
	e.inCalls++
	return e.in, e.inErr
}

func newStorageTestEngine(t *testing.T) storage.Engine {
	t.Helper()
	ctx := t.Context()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "walk")
	for _, id := range []storage.NodeID{"root", "a", "c"} {
		_, err := engine.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(ctx, &storage.Edge{ID: "e1", StartNode: "root", EndNode: "a", Type: "OUT"}))
	require.NoError(t, engine.CreateEdge(ctx, &storage.Edge{ID: "e2", StartNode: "c", EndNode: "root", Type: "IN"}))
	require.NoError(t, engine.CreateEdge(ctx, &storage.Edge{ID: "e3", StartNode: "root", EndNode: "root", Type: "SELF"}))
	return engine
}

func TestStorageSourceReportsOutgoingBeforeIncoming(t *testing.T) {
	engine := newStorageTestEngine(t)
	collector := &stepCollector{}

	require.NoError(t, StorageSource{Engine: engine}.EachIncidentEdge(context.Background(), "root", collector))
	// A self loop is incident from both sides, so the storage engine returns it
	// in the outgoing and in the incoming list; the walk's visited set publishes
	// it at most once (see TestWalkOverStorageEngine).
	require.Len(t, collector.steps, 4)
	assert.Equal(t, fakeEdge{neighbor: "a", relationship: "OUT", direction: Outgoing}, collector.steps[0])
	assert.Equal(t, fakeEdge{neighbor: "root", relationship: "SELF", direction: Outgoing}, collector.steps[1])
	assert.Equal(t, fakeEdge{neighbor: "c", relationship: "IN", direction: Incoming}, collector.steps[2])
	assert.Equal(t, fakeEdge{neighbor: "root", relationship: "SELF", direction: Incoming}, collector.steps[3])
}

func TestStorageSourceHonorsVisitorStop(t *testing.T) {
	engine := newStorageTestEngine(t)
	stopping := &stoppingVisitor{limit: 1}

	require.NoError(t, StorageSource{Engine: engine}.EachIncidentEdge(context.Background(), "root", stopping))
	assert.Equal(t, 1, stopping.seen)
}

func TestStorageSourceNilEngine(t *testing.T) {
	collector := &stepCollector{}
	require.NoError(t, StorageSource{}.EachIncidentEdge(context.Background(), "root", collector))
	assert.Empty(t, collector.steps)
}

// TestStorageSourceReportsRowsAlongsideReadErrors preserves the pre-merge
// storage contract: the server router and MCP walked both directions and used
// the rows an errored read returned instead of dropping that direction.
func TestStorageSourceReportsRowsAlongsideReadErrors(t *testing.T) {
	outErr := errors.New("outgoing adjacency read failed")
	inErr := errors.New("incoming adjacency read failed")
	engine := &controlledAdjacencyEngine{
		Engine: newStorageTestEngine(t),
		out: []*storage.Edge{
			{ID: "e1", StartNode: "root", EndNode: "a", Type: "OUT"},
			nil,
		},
		outErr: outErr,
		in: []*storage.Edge{
			{ID: "e2", StartNode: "c", EndNode: "root", Type: "IN"},
		},
		inErr: inErr,
	}
	collector := &stepCollector{}

	err := StorageSource{Engine: engine}.EachIncidentEdge(context.Background(), "root", collector)
	require.ErrorIs(t, err, outErr)
	assert.Equal(t, 1, engine.outCalls, "the outgoing list is read once")
	assert.Equal(t, 1, engine.inCalls, "an outgoing read error does not skip the incoming list")
	assert.Equal(t, []fakeEdge{
		{neighbor: "a", relationship: "OUT", direction: Outgoing},
		{neighbor: "", relationship: "", direction: Outgoing},
		{neighbor: "c", relationship: "IN", direction: Incoming},
	}, collector.steps, "rows survive their read error and unusable records are reported")
}

// TestStorageSourceReportsUnusableRecordsSoThePollCountsThem is the adapter half
// of the bounded cancellation: a record the adapter cannot interpret still
// reaches the walk, so an adjacency made of such records cannot bypass the poll.
func TestStorageSourceReportsUnusableRecordsSoThePollCountsThem(t *testing.T) {
	const width = 20000
	engine := &controlledAdjacencyEngine{
		Engine: newStorageTestEngine(t),
		out:    make([]*storage.Edge, width),
		in: []*storage.Edge{
			{ID: "e2", StartNode: "c", EndNode: "root", Type: "IN"},
		},
	}
	collector := &stepCollector{}

	require.NoError(t, StorageSource{Engine: engine}.EachIncidentEdge(context.Background(), "root", collector))
	require.Len(t, collector.steps, width+1)
	assert.Equal(t, fakeEdge{neighbor: "", relationship: "", direction: Outgoing}, collector.steps[0])
	assert.Equal(t, fakeEdge{neighbor: "c", relationship: "IN", direction: Incoming}, collector.steps[width])
}

// TestWalkOverStorageSourceDoesNotPublishRowsOfACanceledRead proves that the rows
// a read reports next to a cancellation error are never published as a partial
// collection: the walk observes the cancellation at its next boundary and neither
// publishes nor reads another node.
func TestWalkOverStorageSourceDoesNotPublishRowsOfACanceledRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine := &controlledAdjacencyEngine{
		Engine: newStorageTestEngine(t),
		out: []*storage.Edge{
			{ID: "e1", StartNode: "root", EndNode: "a", Type: "OUT"},
		},
		outErr: context.Canceled,
		onOut:  cancel,
		in: []*storage.Edge{
			{ID: "e2", StartNode: "c", EndNode: "root", Type: "IN"},
		},
	}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](ctx, StorageSource{Engine: engine}, accept, "root",
		Options{MaxHops: 3, Limit: 100})
	require.Nil(t, records)
	require.ErrorIs(t, err, context.Canceled)
	// Both rows of the one adjacency that was already being enumerated are
	// examined, then the boundary poll stops the walk before publication.
	assert.Equal(t, []string{"a", "c"}, accept.loads)
	assert.Equal(t, 1, engine.outCalls)
	assert.Equal(t, 1, engine.inCalls)
}

// TestWalkOverStorageEngine covers the canonical walk through the real storage
// adapter, including cycle and self-loop handling.
func TestWalkOverStorageEngine(t *testing.T) {
	engine := newStorageTestEngine(t)
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "e4", StartNode: "a", EndNode: "root", Type: "BACK"}))

	accept := &recordingAcceptor{}
	records, err := Walk[testRecord](context.Background(), StorageSource{Engine: engine}, accept, "root",
		Options{MaxHops: 2, Limit: 100})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "c"}, recordIDs(records))
	require.Equal(t, []string{"outgoing", "incoming"}, recordDirections(records))
	require.Equal(t, []int{1, 1}, recordDistances(records))
}

type stoppingVisitor struct {
	limit int
	seen  int
}

func (v *stoppingVisitor) Step(_, _ string, _ Direction) bool {
	v.seen++
	return v.seen < v.limit
}
