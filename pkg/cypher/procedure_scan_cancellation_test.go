package cypher

// Procedure population scans must end where their caller ends.
//
// db.index.fulltext.queryNodes / queryRelationships, the
// db.index.vector.queryRelationships fallback scan and db.temporal.asOf each
// walk a storage population. A caller that cancels — or whose deadline
// expires — has to stop that walk instead of receiving its cancellation only
// after the rest of the population has been read, scored and ordered.
//
// These regressions drive the public Execute path on both parsers. The
// cancellation is the case's own real CancelFunc (or real deadline), released
// from the storage boundary the owner enumerates: the node or edge stream the
// procedure reads, or the label read db.temporal.asOf's fallback scans. No case
// pre-cancels its context (the statement gate would answer that), and no case
// asserts on a private owner: what fails is the population the storage
// boundary delivered to the procedure's own work.
//
// Every case also holds the control: the same statement on the same fixture
// returns its exact rows and columns both before and after the canceled call.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	procedureScanNodes         = 40
	procedureScanRelationships = 30
	// procedureScanCancelAfter is how many entities the population read
	// delivers before the caller's cancellation is released.
	procedureScanCancelAfter = 5
	// procedureScanDeadline is the case's real deadline. A deadline case
	// waits for it at the storage boundary, so expiry never depends on
	// machine speed.
	procedureScanDeadline = 50 * time.Millisecond

	procedureScanNodesQuery = "CALL db.index.fulltext.queryNodes('default', 'hello')"
	procedureScanNodesAll   = "CALL db.index.fulltext.queryNodes('default', '*')"
	procedureScanRelsQuery  = "CALL db.index.fulltext.queryRelationships('default', 'hello')"
	procedureScanRelsAll    = "CALL db.index.fulltext.queryRelationships('default', '*')"
	procedureScanVectorRels = "CALL db.index.vector.queryRelationships('rel_embeddings', 30, [1, 0])"
	procedureScanAsOf       = "CALL db.temporal.asOf('Doc', 'temporal_key', 'temporal-003', 'valid_from', 'valid_to', '2024-06-01T00:00:00Z')"
	procedureScanAsOfPast   = "CALL db.temporal.asOf('Doc', 'temporal_key', 'temporal-003', 'valid_from', 'valid_to', '2025-06-01T00:00:00Z')"
)

// procedureScanMode is one canceled call: a real CancelFunc or a real deadline.
type procedureScanMode struct {
	name  string
	want  error
	waits bool
}

// procedureScanModes are the two ways a caller ends a statement early.
var procedureScanModes = []procedureScanMode{
	{"cancel", context.Canceled, false},
	{"deadline", context.DeadlineExceeded, true},
}

// procedureScanTrigger releases the caller's cancellation at one chosen
// arrival of the storage boundary. A cancel case calls the caller's own
// CancelFunc; a deadline case waits for the caller's own deadline, so the
// release point is deterministic in both modes.
type procedureScanTrigger struct {
	cancel   context.CancelFunc
	done     <-chan struct{}
	waits    bool
	after    int64
	arrivals atomic.Int64
	fired    atomic.Bool
}

// arrive counts one entity the population read is about to deliver and
// releases the cancellation at the configured arrival.
func (trigger *procedureScanTrigger) arrive() {
	if trigger == nil {
		return
	}
	if trigger.arrivals.Add(1) != trigger.after {
		return
	}
	if !trigger.fired.CompareAndSwap(false, true) {
		return
	}
	if trigger.waits {
		<-trigger.done
		return
	}
	trigger.cancel()
}

// procedureScanStreamBoundary is the node and edge population read the
// procedures enumerate. It reports how many entities it delivered, so a case
// can tell enumeration that ended at the caller's cancellation from
// enumeration that ran to the end of the fixture. AllNodes/AllEdges are
// counted too: a procedure that enumerates the whole population instead of
// streaming it takes the bulk read, which these cases reject.
// acceptedNodes and acceptedEdges count, per stream, the entities the boundary
// accepted for delivery after the cancellation release point, so a case can
// hold each stream to its own exact delivered prefix.
type procedureScanStreamBoundary struct {
	*storage.NamespacedEngine
	trigger       *procedureScanTrigger
	delivered     atomic.Int64
	acceptedNodes atomic.Int64
	acceptedEdges atomic.Int64
	bulkNodes     atomic.Int64
	bulkEdges     atomic.Int64
}

func (boundary *procedureScanStreamBoundary) StreamNodes(ctx context.Context, visit func(*storage.Node) error) error {
	return boundary.NamespacedEngine.StreamNodes(ctx, boundary.nodeVisitor(ctx, visit))
}

func (boundary *procedureScanStreamBoundary) StreamNodesWithOptions(ctx context.Context, options storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	return boundary.NamespacedEngine.StreamNodesWithOptions(ctx, options, boundary.nodeVisitor(ctx, visit))
}

func (boundary *procedureScanStreamBoundary) nodeVisitor(ctx context.Context, visit func(*storage.Node) error) func(*storage.Node) error {
	return func(node *storage.Node) error {
		if err := boundary.accept(ctx, &boundary.acceptedNodes); err != nil {
			return err
		}
		return visit(node)
	}
}

func (boundary *procedureScanStreamBoundary) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	return boundary.NamespacedEngine.StreamEdges(ctx, func(edge *storage.Edge) error {
		if err := boundary.accept(ctx, &boundary.acceptedEdges); err != nil {
			return err
		}
		return visit(edge)
	})
}

// accept is the one instrumentation owner of the node and edge boundary
// streams. It answers the caller's own context error, counts one delivered
// entity, releases the caller's cancellation at the configured arrival and
// counts that delivery for the entity's own stream. StreamNodes and StreamEdges
// keep their typed storage kernel and visitor call and reach this owner first.
func (boundary *procedureScanStreamBoundary) accept(ctx context.Context, accepted *atomic.Int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	boundary.delivered.Add(1)
	boundary.trigger.arrive()
	accepted.Add(1)
	return nil
}

func (boundary *procedureScanStreamBoundary) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	boundary.bulkNodes.Add(1)
	return boundary.NamespacedEngine.AllNodes(ctx)
}

func (boundary *procedureScanStreamBoundary) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	boundary.bulkEdges.Add(1)
	return boundary.NamespacedEngine.AllEdges(ctx)
}

// procedureScanProbe is the property value the temporal fixture hands the
// fallback scan. valuesEqual formats a property it cannot compare deeply, so
// every node the scan still examines after the caller canceled is counted here;
// the label read observes the same cancellation, so the fallback must stop
// before it formats more than the first candidate.
type procedureScanProbe struct {
	index    int
	examined *atomic.Int64
}

func (probe procedureScanProbe) String() string {
	if probe.examined != nil {
		probe.examined.Add(1)
	}
	return fmt.Sprintf("procedure-scan-probe-%06d", probe.index)
}

// procedureScanLabelBoundary is the label read db.temporal.asOf's fallback
// scans. With a trigger it releases the caller's cancellation at that read and
// returns a population whose every key value is a probe, so the case can count
// the nodes the fallback scan still examined.
type procedureScanLabelBoundary struct {
	storage.Engine
	trigger    *procedureScanTrigger
	population []*storage.Node
	examined   atomic.Int64
	labelReads atomic.Int64
}

func (boundary *procedureScanLabelBoundary) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	boundary.labelReads.Add(1)
	if boundary.trigger == nil {
		return boundary.Engine.GetNodesByLabel(ctx, label)
	}
	boundary.trigger.arrive()
	return boundary.population, nil
}

// procedureScanFixture is the shared graph: Doc nodes with fulltext text and a
// temporal key, LINKS relationships with fulltext text and a two-dimensional
// embedding, and the relationship vector index the fallback scan reads.
type procedureScanFixture struct {
	store   *storage.NamespacedEngine
	nodeIDs []string
	edgeIDs []string
}

func newProcedureScanFixture(t *testing.T) *procedureScanFixture {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "procedure-scan")
	fixture := &procedureScanFixture{store: store}
	for index := 0; index < procedureScanNodes; index++ {
		id := fmt.Sprintf("doc-%03d", index)
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:     storage.NodeID(id),
			Labels: []string{"Doc"},
			Properties: map[string]interface{}{
				"name":         fmt.Sprintf("hello document %03d", index),
				"temporal_key": fmt.Sprintf("temporal-%03d", index),
				"valid_from":   "2024-01-01T00:00:00Z",
				"valid_to":     "2024-12-31T00:00:00Z",
			},
		})
		require.NoError(t, err)
		fixture.nodeIDs = append(fixture.nodeIDs, id)
	}
	for index := 0; index < procedureScanRelationships; index++ {
		id := fmt.Sprintf("rel-%03d", index)
		err := store.CreateEdge(t.Context(), &storage.Edge{
			ID:        storage.EdgeID(id),
			StartNode: storage.NodeID(fixture.nodeIDs[index]),
			EndNode:   storage.NodeID(fixture.nodeIDs[(index+1)%procedureScanNodes]),
			Type:      "LINKS",
			Properties: map[string]interface{}{
				"text": fmt.Sprintf("hello relationship %03d", index),
				"emb":  []float32{1, float32(index%7) / 10},
			},
		})
		require.NoError(t, err)
		fixture.edgeIDs = append(fixture.edgeIDs, id)
	}
	require.NoError(t, store.GetSchema().AddVectorIndex("rel_embeddings", "LINKS", "emb", 2, "cosine", storage.ConstraintEntityRelationship))
	return fixture
}

// procedureScanProbePopulation is the temporal case's scan population: one
// probe-bearing node per fixture node.
func procedureScanProbePopulation(examined *atomic.Int64) []*storage.Node {
	population := make([]*storage.Node, 0, procedureScanNodes)
	for index := 0; index < procedureScanNodes; index++ {
		population = append(population, &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%03d", index)),
			Labels: []string{"Doc"},
			Properties: map[string]interface{}{
				"temporal_key": procedureScanProbe{index: index, examined: examined},
				"valid_from":   "2024-01-01T00:00:00Z",
				"valid_to":     "2024-12-31T00:00:00Z",
			},
		})
	}
	return population
}

func procedureScanExecutor(store storage.Engine) *StorageExecutor {
	// No result cache: every case has to reach its own population read.
	return NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

func procedureScanRowIDs(t *testing.T, result *ExecuteResult) []string {
	t.Helper()
	ids := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.NotEmpty(t, row)
		switch entity := row[0].(type) {
		case *storage.Node:
			ids = append(ids, string(entity.ID))
		case *storage.Edge:
			ids = append(ids, string(entity.ID))
		default:
			require.FailNowf(t, "unexpected procedure row", "row value %T", row[0])
		}
	}
	return ids
}

// procedureScanRequireHealthy is the control: the statement returns its exact
// columns and every fixture entity.
func procedureScanRequireHealthy(t *testing.T, exec *StorageExecutor, query string, columns, wantIDs []string) {
	t.Helper()
	result, err := exec.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, columns, result.Columns)
	require.ElementsMatch(t, wantIDs, procedureScanRowIDs(t, result))
}

// procedureScanCaseContext builds the case's caller context and the trigger
// released at its `after`-th storage arrival. A cancel case owns the real
// CancelFunc; a deadline case owns a real deadline and hands the trigger the
// caller's own Done channel to wait for.
func procedureScanCaseContext(t *testing.T, waits bool, after int64) (context.Context, *procedureScanTrigger) {
	t.Helper()
	if waits {
		ctx, cancel := context.WithTimeout(t.Context(), procedureScanDeadline)
		t.Cleanup(cancel)
		return ctx, &procedureScanTrigger{cancel: cancel, done: ctx.Done(), waits: true, after: after}
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return ctx, &procedureScanTrigger{cancel: cancel, done: ctx.Done(), after: after}
}

// procedureScanRequireStreamStopped runs one canceled call over the node or
// edge stream and holds it to the three product outcomes: the caller's
// cancellation reached the population read, the statement answered with it and
// published nothing, and the read delivered no entity past the cancellation.
func procedureScanRequireStreamStopped(t *testing.T, fixture *procedureScanFixture, query string, mode procedureScanMode, nodes bool) {
	t.Helper()
	ctx, trigger := procedureScanCaseContext(t, mode.waits, procedureScanCancelAfter)
	boundary := &procedureScanStreamBoundary{NamespacedEngine: fixture.store, trigger: trigger}
	exec := procedureScanExecutor(boundary)

	result, err := exec.Execute(ctx, query, nil)
	require.True(t, trigger.fired.Load(),
		"the caller's cancellation must reach the procedure's population read")
	require.ErrorIs(t, err, mode.want)
	require.Nil(t, result, "a canceled procedure call publishes no partial rows")
	require.Equal(t, int64(procedureScanCancelAfter), boundary.delivered.Load(),
		"the population read must deliver exactly the entities up to the caller's cancellation")
	if nodes {
		require.Zero(t, boundary.bulkNodes.Load(),
			"the node population must be read on the caller's context")
	} else {
		require.Zero(t, boundary.bulkEdges.Load(),
			"the edge population must be read on the caller's context")
	}
}

func TestProcedureScanCancellationStopsWork(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{
		{"nornic", config.WithNornicParser},
		{"antlr", config.WithANTLRParser},
	} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			fixture := newProcedureScanFixture(t)

			t.Run("fulltext.queryNodes", func(t *testing.T) {
				healthy := procedureScanExecutor(&procedureScanStreamBoundary{NamespacedEngine: fixture.store})
				procedureScanRequireHealthy(t, healthy, procedureScanNodesQuery, []string{"node", "score"}, fixture.nodeIDs)
				procedureScanRequireHealthy(t, healthy, procedureScanNodesAll, []string{"node", "score"}, fixture.nodeIDs)

				for _, mode := range procedureScanModes {
					t.Run(mode.name+"/match", func(t *testing.T) {
						procedureScanRequireStreamStopped(t, fixture, procedureScanNodesQuery, mode, true)
					})
					t.Run(mode.name+"/wildcard", func(t *testing.T) {
						procedureScanRequireStreamStopped(t, fixture, procedureScanNodesAll, mode, true)
					})
				}

				after := procedureScanExecutor(&procedureScanStreamBoundary{NamespacedEngine: fixture.store})
				procedureScanRequireHealthy(t, after, procedureScanNodesQuery, []string{"node", "score"}, fixture.nodeIDs)
			})

			t.Run("fulltext.queryRelationships", func(t *testing.T) {
				healthy := procedureScanExecutor(&procedureScanStreamBoundary{NamespacedEngine: fixture.store})
				procedureScanRequireHealthy(t, healthy, procedureScanRelsQuery, []string{"relationship", "score"}, fixture.edgeIDs)
				procedureScanRequireHealthy(t, healthy, procedureScanRelsAll, []string{"relationship", "score"}, fixture.edgeIDs)

				for _, mode := range procedureScanModes {
					t.Run(mode.name+"/match", func(t *testing.T) {
						procedureScanRequireStreamStopped(t, fixture, procedureScanRelsQuery, mode, false)
					})
					t.Run(mode.name+"/wildcard", func(t *testing.T) {
						procedureScanRequireStreamStopped(t, fixture, procedureScanRelsAll, mode, false)
					})
				}
			})

			t.Run("vector.queryRelationships fallback", func(t *testing.T) {
				healthy := procedureScanExecutor(&procedureScanStreamBoundary{NamespacedEngine: fixture.store})
				procedureScanRequireHealthy(t, healthy, procedureScanVectorRels, []string{"relationship", "score"}, fixture.edgeIDs)

				for _, mode := range procedureScanModes {
					t.Run(mode.name, func(t *testing.T) {
						procedureScanRequireStreamStopped(t, fixture, procedureScanVectorRels, mode, false)
					})
				}
			})

			t.Run("temporal.asOf fallback", func(t *testing.T) {
				healthyBoundary := &procedureScanLabelBoundary{Engine: fixture.store}
				healthy := procedureScanExecutor(healthyBoundary)
				procedureScanRequireHealthy(t, healthy, procedureScanAsOf, []string{"node"}, []string{"doc-003"})
				procedureScanRequireHealthy(t, healthy, procedureScanAsOfPast, []string{"node"}, nil)

				for _, mode := range procedureScanModes {
					t.Run(mode.name, func(t *testing.T) {
						// The first label read of this statement is the executor's own label
						// lookup; the fallback's read is the second, so that is where the
						// caller's cancellation is released.
						ctx, trigger := procedureScanCaseContext(t, mode.waits, 2)
						boundary := &procedureScanLabelBoundary{Engine: fixture.store, trigger: trigger}
						boundary.population = procedureScanProbePopulation(&boundary.examined)
						exec := procedureScanExecutor(boundary)

						result, err := exec.Execute(ctx, procedureScanAsOf, nil)
						require.True(t, trigger.fired.Load(),
							"the caller's cancellation must reach the fallback's label read")
						require.ErrorIs(t, err, mode.want)
						require.Nil(t, result, "a canceled procedure call publishes no partial rows")
						require.NotZero(t, boundary.labelReads.Load(),
							"the fallback reads its label population")
						require.LessOrEqual(t, boundary.examined.Load(), int64(1),
							"the fallback scan must stop examining nodes once the caller canceled")
					})
				}
			})
		})
	}
}
