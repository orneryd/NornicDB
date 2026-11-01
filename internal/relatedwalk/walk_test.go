package relatedwalk

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testRecord is the consumer record shape used by these tests.
type testRecord struct {
	ID           string
	Relationship string
	Direction    string
	Distance     int
	Path         []string
}

// fakeEdge is one incident edge of the fake source.
type fakeEdge struct {
	neighbor     string
	relationship string
	direction    Direction
}

// fakeSource is an ordered adjacency model with read accounting.
type fakeSource struct {
	adjacency map[string][]fakeEdge
	errs      map[string]error
	reads     []string
	steps     int
	onStep    func(*fakeSource)
}

func (s *fakeSource) EachIncidentEdge(ctx context.Context, nodeID string, visit StepVisitor) error {
	s.reads = append(s.reads, nodeID)
	if err := s.errs[nodeID]; err != nil {
		return err
	}
	for _, edge := range s.adjacency[nodeID] {
		s.steps++
		if s.onStep != nil {
			s.onStep(s)
		}
		if !visit.Step(edge.neighbor, edge.relationship, edge.direction) {
			return nil
		}
	}
	return nil
}

// recordingAcceptor records the neighbors it sees and applies one missing-node
// policy. A rejected neighbor is neither published nor expanded, which is what
// the server router and the query executor do with a node they cannot load.
type recordingAcceptor struct {
	missing    map[string]bool
	emitMissed bool
	rejected   map[string]bool
	loads      []string
	seen       []Neighbor
	onDiscover func(ctx context.Context, n *Neighbor)
}

func (a *recordingAcceptor) Discover(ctx context.Context, n *Neighbor) (testRecord, bool, bool) {
	a.loads = append(a.loads, n.ID)
	a.seen = append(a.seen, *n)
	if a.onDiscover != nil {
		a.onDiscover(ctx, n)
	}
	if a.missing[n.ID] {
		if !a.emitMissed {
			return testRecord{}, false, false
		}
		return testRecord{ID: n.ID, Distance: n.Distance, Direction: n.Direction.String()}, true, true
	}
	if a.rejected[n.ID] {
		return testRecord{}, false, false
	}
	return testRecord{
		ID:           n.ID,
		Relationship: n.Relationship,
		Direction:    n.Direction.String(),
		Distance:     n.Distance,
		Path:         n.Path,
	}, true, true
}

func (a *recordingAcceptor) ids() []string {
	ids := make([]string, 0, len(a.seen))
	for _, n := range a.seen {
		ids = append(ids, n.ID)
	}
	return ids
}

// countingContext counts how often the walk asks the context for its Done
// channel and its error.
type countingContext struct {
	context.Context
	doneCalls int
	errCalls  int
}

func (c *countingContext) Done() <-chan struct{} {
	c.doneCalls++
	return c.Context.Done()
}

func (c *countingContext) Err() error {
	c.errCalls++
	return c.Context.Err()
}

func TestWalkPublishesOutgoingBeforeIncomingAtFirstDiscovery(t *testing.T) {
	src := &fakeSource{adjacency: map[string][]fakeEdge{
		"root": {
			{neighbor: "a", relationship: "OUT", direction: Outgoing},
			{neighbor: "b", relationship: "OUT", direction: Outgoing},
			{neighbor: "c", relationship: "IN", direction: Incoming},
		},
		"a": {
			{neighbor: "root", relationship: "BACK", direction: Outgoing}, // cycle
			{neighbor: "d", relationship: "OUT", direction: Outgoing},
		},
		"b": {},
		"c": {
			{neighbor: "e", relationship: "OUT", direction: Outgoing},
		},
	}}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 3, Limit: 100})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, recordIDs(records))
	require.Equal(t, []string{"outgoing", "outgoing", "incoming", "outgoing", "outgoing"}, recordDirections(records))
	require.Equal(t, []int{1, 1, 1, 2, 2}, recordDistances(records))
}

func TestWalkPublishesSelfLoopOnce(t *testing.T) {
	src := &fakeSource{adjacency: map[string][]fakeEdge{
		"root": {{neighbor: "a", relationship: "OUT", direction: Outgoing}},
		"a": {
			{neighbor: "a", relationship: "SELF", direction: Outgoing},
			{neighbor: "a", relationship: "SELF", direction: Incoming},
		},
	}}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 3, Limit: 100})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, recordIDs(records))
	require.Equal(t, 1, records[0].Distance)
}

func TestWalkFirstDiscoveryWinsDistance(t *testing.T) {
	// n is discovered at distance 1 through the incoming edge from root and
	// again at distance 2 through m. The first discovery wins.
	src := &fakeSource{adjacency: map[string][]fakeEdge{
		"root": {
			{neighbor: "m", relationship: "OUT", direction: Outgoing},
			{neighbor: "n", relationship: "IN", direction: Incoming},
		},
		"m": {
			{neighbor: "n", relationship: "OUT", direction: Outgoing},
		},
		"n": {},
	}}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 3, Limit: 100})
	require.NoError(t, err)
	require.Equal(t, []string{"m", "n"}, recordIDs(records))
	require.Equal(t, []int{1, 1}, recordDistances(records))
	require.Equal(t, []string{"outgoing", "incoming"}, recordDirections(records))
}

func TestWalkDepthBoundaries(t *testing.T) {
	src := &fakeSource{adjacency: map[string][]fakeEdge{
		"root": {{neighbor: "a", relationship: "OUT", direction: Outgoing}},
		"a":    {{neighbor: "b", relationship: "OUT", direction: Outgoing}},
		"b":    {{neighbor: "c", relationship: "OUT", direction: Outgoing}},
		"c":    {},
	}}

	for _, tc := range []struct {
		maxHops int
		want    []string
		reads   []string
	}{
		{maxHops: 1, want: []string{"a"}, reads: []string{"root"}},
		{maxHops: 2, want: []string{"a", "b"}, reads: []string{"root", "a"}},
		{maxHops: 3, want: []string{"a", "b", "c"}, reads: []string{"root", "a", "b"}},
	} {
		adjacency := make(map[string][]fakeEdge, len(src.adjacency))
		for node, edges := range src.adjacency {
			adjacency[node] = edges
		}
		source := &fakeSource{adjacency: adjacency}
		accept := &recordingAcceptor{}
		records, err := Walk[testRecord](context.Background(), source, accept, "root",
			Options{MaxHops: tc.maxHops, Limit: 100})
		require.NoError(t, err)
		require.Equal(t, tc.want, recordIDs(records), "MaxHops=%d", tc.maxHops)
		require.Equal(t, tc.reads, source.reads, "MaxHops=%d", tc.maxHops)
	}
}

func TestWalkLimitAppliesInsideOneAdjacency(t *testing.T) {
	edges := make([]fakeEdge, 0, 60)
	for i := 0; i < 60; i++ {
		edges = append(edges, fakeEdge{neighbor: fmt.Sprintf("neighbor-%02d", i), relationship: "LINK", direction: Outgoing})
	}
	src := &fakeSource{adjacency: map[string][]fakeEdge{"root": edges}}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 1, Limit: 50})
	require.NoError(t, err)
	require.Len(t, records, 50)
	require.Equal(t, "neighbor-00", records[0].ID)
	require.Equal(t, "neighbor-49", records[49].ID)
	require.Len(t, accept.loads, 50, "the cap must stop node loads inside the adjacency")
	require.Equal(t, []string{"root"}, src.reads)
}

func TestWalkStopsAtCapWithoutFurtherReads(t *testing.T) {
	// Exactly the cap is reachable through the outgoing adjacency; the walk must
	// not read the incoming adjacency or expand any neighbor afterwards.
	outgoing := make([]fakeEdge, 0, 50)
	for i := 0; i < 50; i++ {
		outgoing = append(outgoing, fakeEdge{neighbor: fmt.Sprintf("out-%02d", i), relationship: "OUT", direction: Outgoing})
	}
	incoming := make([]fakeEdge, 0, 5)
	for i := 0; i < 5; i++ {
		incoming = append(incoming, fakeEdge{neighbor: fmt.Sprintf("in-%02d", i), relationship: "IN", direction: Incoming})
	}
	src := &fakeSource{adjacency: map[string][]fakeEdge{"root": append(append([]fakeEdge{}, outgoing...), incoming...)}}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 3, Limit: 50})
	require.NoError(t, err)
	require.Len(t, records, 50)
	require.Equal(t, []string{"root"}, src.reads)
	require.Len(t, accept.loads, 50)
	for _, id := range accept.loads {
		require.NotContains(t, id, "in-")
	}
}

func TestWalkCapBoundaryDoesNotExpandFurther(t *testing.T) {
	// The cap is reached exactly at the end of the first adjacency; the walk
	// must not expand the 50 queued neighbors for a second hop.
	outgoing := make([]fakeEdge, 0, 50)
	for i := 0; i < 50; i++ {
		outgoing = append(outgoing, fakeEdge{neighbor: fmt.Sprintf("n-%02d", i), relationship: "OUT", direction: Outgoing})
	}
	adjacency := map[string][]fakeEdge{"root": outgoing}
	for i := 0; i < 50; i++ {
		adjacency[fmt.Sprintf("n-%02d", i)] = []fakeEdge{{neighbor: fmt.Sprintf("deep-%02d", i), relationship: "OUT", direction: Outgoing}}
	}
	src := &fakeSource{adjacency: adjacency}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 5, Limit: 50})
	require.NoError(t, err)
	require.Len(t, records, 50)
	require.Equal(t, []string{"root"}, src.reads)
}

func TestWalkRejectedNeighborIsNotPublished(t *testing.T) {
	// A rejected neighbor is examined but never published and never expanded, so
	// the walk cannot stop on the result cap; it stops when the queue is empty.
	src := &fakeSource{adjacency: map[string][]fakeEdge{
		"root": {
			{neighbor: "gone", relationship: "OUT", direction: Outgoing},
			{neighbor: "kept", relationship: "OUT", direction: Outgoing},
		},
		"kept": {},
	}}
	accept := &recordingAcceptor{rejected: map[string]bool{"gone": true}}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 2, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"kept"}, recordIDs(records))
	require.NotContains(t, src.reads, "gone")
}

func TestWalkMissingNodePolicySkipDoesNotExpand(t *testing.T) {
	src := &fakeSource{adjacency: map[string][]fakeEdge{
		"root": {
			{neighbor: "gone", relationship: "OUT", direction: Outgoing},
			{neighbor: "kept", relationship: "OUT", direction: Outgoing},
		},
		"gone": {
			{neighbor: "hidden-child", relationship: "OUT", direction: Outgoing},
		},
		"kept": {},
	}}
	accept := &recordingAcceptor{missing: map[string]bool{"gone": true}}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 2, Limit: 100})
	require.NoError(t, err)
	require.Equal(t, []string{"kept"}, recordIDs(records))
	require.NotContains(t, src.reads, "gone", "a skipped missing node must not be expanded")
	require.NotContains(t, accept.loads, "hidden-child")
}

func TestWalkMissingNodePolicyEmitStillExpands(t *testing.T) {
	src := &fakeSource{adjacency: map[string][]fakeEdge{
		"root": {
			{neighbor: "gone", relationship: "OUT", direction: Outgoing},
		},
		"gone": {
			{neighbor: "child", relationship: "OUT", direction: Outgoing},
		},
		"child": {},
	}}
	accept := &recordingAcceptor{missing: map[string]bool{"gone": true}, emitMissed: true}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 2, Limit: 100})
	require.NoError(t, err)
	require.Equal(t, []string{"gone", "child"}, recordIDs(records))
	require.Contains(t, src.reads, "gone")
	require.Equal(t, []int{1, 2}, recordDistances(records))
}

func TestWalkOptionalPaths(t *testing.T) {
	src := &fakeSource{adjacency: map[string][]fakeEdge{
		"root": {{neighbor: "a", relationship: "OUT", direction: Outgoing}},
		"a":    {{neighbor: "b", relationship: "OUT", direction: Outgoing}},
		"b":    {},
	}}

	t.Run("disabled", func(t *testing.T) {
		pathIDCalls := 0
		accept := &recordingAcceptor{}
		records, err := Walk[testRecord](context.Background(), src, accept, "root",
			Options{MaxHops: 3, Limit: 10, PathID: func(id string) string { pathIDCalls++; return "/" + id }})
		require.NoError(t, err)
		require.Len(t, records, 2)
		for _, record := range records {
			require.Nil(t, record.Path)
		}
		require.Zero(t, pathIDCalls, "PathID must not run when paths are disabled")
	})

	t.Run("enabled", func(t *testing.T) {
		accept := &recordingAcceptor{}
		records, err := Walk[testRecord](context.Background(), src, accept, "root",
			Options{MaxHops: 3, Limit: 10, Paths: true, PathID: func(id string) string { return "/" + id }})
		require.NoError(t, err)
		require.Equal(t, []string{"/root", "/a"}, records[0].Path)
		require.Equal(t, []string{"/root", "/a", "/b"}, records[1].Path)
		// Earlier published paths stay intact when later paths are built.
		require.Equal(t, []string{"/root", "/a"}, records[0].Path)
	})
}

func TestWalkCancellationPublishesNothing(t *testing.T) {
	t.Run("canceled before the walk", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		accept := &recordingAcceptor{}
		records, err := Walk[testRecord](ctx, &fakeSource{}, accept, "root", Options{MaxHops: 2, Limit: 10})
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, records)
	})

	t.Run("canceled while an adjacency is enumerated", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		edges := make([]fakeEdge, 0, 20)
		for i := 0; i < 20; i++ {
			edges = append(edges, fakeEdge{neighbor: fmt.Sprintf("n-%02d", i), relationship: "OUT", direction: Outgoing})
		}
		src := &fakeSource{adjacency: map[string][]fakeEdge{"root": edges}}
		src.onStep = func(s *fakeSource) {
			if s.steps == 5 {
				cancel()
			}
		}
		accept := &recordingAcceptor{}
		records, err := Walk[testRecord](ctx, src, accept, "root", Options{MaxHops: 3, Limit: 100})
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, records, "a canceled walk must not publish a partial collection")
	})

	t.Run("deadline exceeded", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		accept := &recordingAcceptor{}
		records, err := Walk[testRecord](ctx, &fakeSource{}, accept, "root", Options{MaxHops: 2, Limit: 10})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, context.Canceled)
		require.Nil(t, records)
	})
}

// TestWalkCancellationBoundsWideRejectedAdjacency is the fail-first case for a
// result cap that cannot bound work: every neighbor is rejected, so no record is
// ever published and the cap never applies. Only a poll inside the adjacency can
// stop the walk before it loads every neighbor.
func TestWalkCancellationBoundsWideRejectedAdjacency(t *testing.T) {
	const (
		width    = 20480
		cancelAt = 100
		bounds   = cancelPollMask + 1
	)
	adjacency := make(map[string][]fakeEdge, width+1)
	edges := make([]fakeEdge, 0, width)
	accept := &recordingAcceptor{rejected: make(map[string]bool, width)}
	for i := 0; i < width; i++ {
		id := fmt.Sprintf("n-%05d", i)
		edges = append(edges, fakeEdge{neighbor: id, relationship: "OUT", direction: Outgoing})
		// Every rejected neighbor would be expanded: a walk that ignores the
		// cancellation while enumerating the root adjacency reads all of them.
		adjacency[id] = []fakeEdge{{neighbor: "deep-" + id, relationship: "OUT", direction: Outgoing}}
		accept.rejected[id] = true
	}
	adjacency["root"] = edges
	src := &fakeSource{adjacency: adjacency}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accept.onDiscover = func(context.Context, *Neighbor) {
		if len(accept.loads) == cancelAt {
			cancel()
		}
	}

	records, err := Walk[testRecord](ctx, src, accept, "root", Options{MaxHops: 5, Limit: width})
	require.Equal(t, []string{"root"}, src.reads, "no adjacency is read after the cancellation")
	require.GreaterOrEqual(t, len(accept.loads), cancelAt)
	require.LessOrEqual(t, len(accept.loads), cancelAt+bounds,
		"post-cancellation node loads must stay inside the documented poll interval")
	require.Less(t, len(accept.loads), width)
	require.Nil(t, records)
	require.ErrorIs(t, err, context.Canceled)
}

// TestWalkCancellationBoundsWideDuplicateAndEmptyAdjacency covers the records a
// walk examines without publishing or expanding them (duplicates, empty edges
// and empty neighbor ids). They must advance the poll just like a fresh record.
func TestWalkCancellationBoundsWideDuplicateAndEmptyAdjacency(t *testing.T) {
	const (
		width    = 20480
		cancelAt = 100
		bounds   = cancelPollMask + 1
	)
	edges := make([]fakeEdge, 0, width)
	for i := 0; i < width; i++ {
		switch i % 3 {
		case 0:
			edges = append(edges, fakeEdge{direction: Outgoing})
		case 1:
			edges = append(edges, fakeEdge{neighbor: "seen", relationship: "OUT", direction: Outgoing})
		default:
			edges = append(edges, fakeEdge{neighbor: "", relationship: "IN", direction: Incoming})
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{adjacency: map[string][]fakeEdge{"root": edges}}
	src.onStep = func(s *fakeSource) {
		if s.steps == cancelAt {
			cancel()
		}
	}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](ctx, src, accept, "root", Options{MaxHops: 3, Limit: width})
	require.Equal(t, []string{"root"}, src.reads, "no adjacency is read after the cancellation")
	require.Len(t, accept.loads, 1, "only the single fresh neighbor is loaded")
	require.GreaterOrEqual(t, src.steps, cancelAt)
	require.LessOrEqual(t, src.steps, cancelAt+bounds,
		"examined records include duplicates and empty edges inside the poll interval")
	require.Less(t, src.steps, width)
	require.Nil(t, records)
	require.ErrorIs(t, err, context.Canceled)
}

// TestWalkBackgroundContextNeverPollsCancel proves the nil Done fastpath: an
// uncancelable context is read once and its Err is never consulted per record.
func TestWalkBackgroundContextNeverPollsCancel(t *testing.T) {
	ctx := &countingContext{Context: context.Background()}
	edges := make([]fakeEdge, 0, 1000)
	for i := 0; i < 1000; i++ {
		edges = append(edges, fakeEdge{neighbor: fmt.Sprintf("n-%04d", i), relationship: "OUT", direction: Outgoing})
	}
	src := &fakeSource{adjacency: map[string][]fakeEdge{"root": edges}}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](ctx, src, accept, "root", Options{MaxHops: 1, Limit: 10})
	require.NoError(t, err)
	require.Len(t, records, 10)
	require.Equal(t, 1, ctx.doneCalls, "Done is read once and cached")
	require.Zero(t, ctx.errCalls, "a healthy walk must not consult ctx.Err")
}

// TestWalkHealthyCancelableContextNeverReadsErr proves the amortized poll costs
// one select on the cached Done channel and never an Err call per record.
func TestWalkHealthyCancelableContextNeverReadsErr(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &countingContext{Context: base}
	edges := make([]fakeEdge, 0, 1024)
	for i := 0; i < 1024; i++ {
		edges = append(edges, fakeEdge{neighbor: fmt.Sprintf("n-%04d", i), relationship: "OUT", direction: Outgoing})
	}
	src := &fakeSource{adjacency: map[string][]fakeEdge{"root": edges}}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](ctx, src, accept, "root", Options{MaxHops: 1, Limit: 512})
	require.NoError(t, err)
	require.Len(t, records, 512)
	require.Equal(t, 1, ctx.doneCalls, "Done is read once and cached")
	require.Zero(t, ctx.errCalls, "the poll selects on Done instead of calling ctx.Err")
}

func TestWalkIgnoresSourceReadFailures(t *testing.T) {
	src := &fakeSource{
		adjacency: map[string][]fakeEdge{
			"root": {
				{neighbor: "broken", relationship: "OUT", direction: Outgoing},
				{neighbor: "kept", relationship: "OUT", direction: Outgoing},
			},
			"broken": {{neighbor: "unreachable", relationship: "OUT", direction: Outgoing}},
			"kept":   {},
		},
		errs: map[string]error{"broken": errors.New("adjacency read failed")},
	}
	accept := &recordingAcceptor{}

	records, err := Walk[testRecord](context.Background(), src, accept, "root",
		Options{MaxHops: 3, Limit: 100})
	require.NoError(t, err)
	require.Equal(t, []string{"broken", "kept"}, recordIDs(records))
	require.Contains(t, src.reads, "broken")
}

func TestWalkRejectsUnusableInputs(t *testing.T) {
	accept := &recordingAcceptor{}
	src := &fakeSource{}

	records, err := Walk[testRecord](context.Background(), src, accept, "", Options{MaxHops: 1, Limit: 10})
	require.NoError(t, err)
	require.Nil(t, records)

	records, err = Walk[testRecord](context.Background(), src, accept, "root", Options{MaxHops: 0, Limit: 10})
	require.NoError(t, err)
	require.Nil(t, records)

	records, err = Walk[testRecord](context.Background(), src, accept, "root", Options{MaxHops: 1, Limit: 0})
	require.NoError(t, err)
	require.Nil(t, records)

	require.Empty(t, src.reads)
}

func recordIDs(records []testRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return ids
}

func recordDirections(records []testRecord) []string {
	directions := make([]string, 0, len(records))
	for _, record := range records {
		directions = append(directions, record.Direction)
	}
	return directions
}

func recordDistances(records []testRecord) []int {
	distances := make([]int, 0, len(records))
	for _, record := range records {
		distances = append(distances, record.Distance)
	}
	return distances
}
