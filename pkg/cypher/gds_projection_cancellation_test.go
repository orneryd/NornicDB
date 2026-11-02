package cypher

// Author: PR144 bounded investigation lane, 2026-11-02: gds.graph.project
// streams the projected graph on the statement's context.
//
// Fail-first baseline (source pin): at 1cd157594d861207862a8c4ddcca7f9e16610553
// buildGraphProjection created its own context.Background()
// (pkg/cypher/gds_projection.go:471), and callGdsGraphProject published the
// projection without consulting the statement's context. Against that source
// the public workload was reproduced fail-first: it continued after cancellation
// and published its projection. The deterministic component controls below
// were added after the context-signature migration; the gate control is also
// verified by removing only that gate through a Go source overlay.
//
// The cases cover two different levels:
//
//   - TestGDSGraphProjectStopsOnRequestCancellation is public end-to-end
//     coverage. It runs the supported CALL through StorageExecutor.Execute on
//     both parsers (config.WithNornicParser and config.WithANTLRParser) over a
//     node-heavy and an edge-heavy workload. The cancellation is the caller's
//     real CancelFunc, released while the statement is running; the only timing
//     gate is that Execute returns within 100ms of the actual cancel().
//   - TestGDSGraphProjectComponentCancellation and
//     TestGDSGraphProjectRefusesPublicationAfterCancel are component coverage of
//     the canonical private entry, callGdsGraphProject, on the deterministic
//     procedure-scan fixture. They cancel at an exact arrival of the population
//     read, with the caller's real CancelFunc or real deadline, and assert the
//     exact delivered prefix. The second cancels after a complete, successful
//     relationship stream and holds the prepublication gate alone.
//
// Every case also holds the healthy control over the same fixture: the
// statement publishes its exact counts and is listed once.

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

// gdsProjectionSeededGraph creates `nodes` nodes of label P (each with age) and
// `edges` relationships of type K between consecutive nodes, in the gds
// database.
func gdsProjectionSeededGraph(t *testing.T, nodes, edges int) *storage.NamespacedEngine {
	t.Helper()
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gds")
	ids := make([]storage.NodeID, nodes)
	started := time.Now()
	for i := 0; i < nodes; i++ {
		id := storage.NodeID(fmt.Sprintf("n%07d", i))
		ids[i] = id
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"age": int64(i)}})
		require.NoError(t, err)
	}
	for i := 0; i < edges; i++ {
		err := engine.CreateEdge(t.Context(), &storage.Edge{
			ID:         storage.EdgeID(fmt.Sprintf("e%07d", i)),
			Type:       "K",
			StartNode:  ids[i%nodes],
			EndNode:    ids[(i+1)%nodes],
			Properties: map[string]interface{}{"weight": float64(i)},
		})
		require.NoError(t, err)
	}
	t.Logf("seeded %d nodes / %d relationships in %s", nodes, edges, time.Since(started))
	return engine
}

// gdsOwnProjections removes only the projections these cases publish, under
// the catalog's own lock. It never resets the shared catalog map.
func gdsOwnProjections(t *testing.T, exec *StorageExecutor, names ...string) {
	t.Helper()
	database := exec.databaseName()
	t.Cleanup(func() {
		projectionsMu.Lock()
		defer projectionsMu.Unlock()
		for _, name := range names {
			delete(graphProjections, gdsCatalogKey(database, name))
		}
	})
}

// gdsProjectionPublished reports whether the catalog holds this executor's
// database's projection called name, under the catalog's own lock.
func gdsProjectionPublished(exec *StorageExecutor, name string) bool {
	database := exec.databaseName()
	projectionsMu.RLock()
	defer projectionsMu.RUnlock()
	_, exists := graphProjections[gdsCatalogKey(database, name)]
	return exists
}

// gdsCancellationOutcome is what one cancelled request actually did.
type gdsCancellationOutcome struct {
	total       time.Duration // how long Execute ran
	afterCancel time.Duration // from the actual cancel() to Execute's return; meaningful only when released
	released    bool          // the caller's context was cancelled before Execute returned
	err         error
	published   bool
}

// gdsCancelAfter runs one statement on a caller context that a timer worker
// cancels after delay, and reports the cancellation as it actually happened.
//
// The timer worker is stopped and joined before this function returns, so no
// goroutine outlives the call. afterCancel runs from the instant cancel() was
// called to the exact instant Execute returned, not from the requested delay.
// A statement that returned before the timer fired is reported as not
// released: that run exercised the healthy path and must not be read as a
// cancellation.
func gdsCancelAfter(t *testing.T, exec *StorageExecutor, graphName, query string, delay time.Duration) gdsCancellationOutcome {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The timer worker owns the cancel() call. canceledAt and fired are read
	// only after the worker has been joined, which is what orders them.
	var (
		canceledAt time.Time
		fired      bool
	)
	stop, workerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(workerDone)
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			fired = true
			canceledAt = time.Now()
			cancel()
		case <-stop:
		}
	}()

	started := time.Now()
	_, err := exec.Execute(ctx, query, nil)
	returnedAt := time.Now()
	close(stop)
	<-workerDone

	outcome := gdsCancellationOutcome{total: returnedAt.Sub(started), err: err}
	if fired && !canceledAt.After(returnedAt) {
		outcome.released = true
		outcome.afterCancel = returnedAt.Sub(canceledAt)
	}
	listed, listErr := exec.Execute(context.Background(), fmt.Sprintf("CALL gds.graph.list('%s')", graphName), nil)
	require.NoError(t, listErr)
	outcome.published = len(listed.Rows) > 0
	return outcome
}

// TestGDSGraphProjectCancellationTimer is the control for gdsCancelAfter: a
// statement that completes before its timer fires is reported as not released
// and keeps its healthy outcome, never as a cancellation that returned late.
// The worker is joined, so the call does not wait for the pending timer.
func TestGDSGraphProjectCancellationTimer(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	outcome := gdsCancelAfter(t, exec, "gds-timer-control", "RETURN 1 AS one", time.Hour)
	require.False(t, outcome.released, "a statement that returned before the timer fired was not cancelled")
	require.NoError(t, outcome.err, "the statement's healthy outcome is reported as it happened")
	require.False(t, outcome.published, "the control statement publishes no projection")
	require.Less(t, outcome.total, time.Minute, "the timer worker is joined without waiting for the pending timer")
}

// TestGDSGraphProjectStopsOnRequestCancellation is the public end-to-end
// coverage: CALL gds.graph.project through StorageExecutor.Execute, on both
// parsers, over a node-heavy and an edge-heavy workload.
//
// Each parser first runs the healthy control: the same statement over the same
// workload, which publishes its exact counts. The next statement is canceled
// after a fixed 1ms, while these large populations are still being processed.
// The case
// makes no claim about which stream the projection is reading then; it requires
// only that a cancelled request answers with context.Canceled, publishes
// nothing, returns within 100ms of the actual cancel(), and that the name is
// free for a complete retry with the exact counts.
func TestGDSGraphProjectStopsOnRequestCancellation(t *testing.T) {
	const (
		stopBound   = 100 * time.Millisecond
		cancelDelay = time.Millisecond
	)
	for _, workload := range []struct {
		key         string
		name        string
		nodes, edge int
	}{
		{"nodes", "node-heavy workload", 100000, 200},
		{"relationships", "edge-heavy workload", 200, 20000},
	} {
		workload := workload
		t.Run(workload.name, func(t *testing.T) {
			exec := NewStorageExecutor(gdsProjectionSeededGraph(t, workload.nodes, workload.edge))

			for _, parser := range []struct {
				name  string
				apply func() func()
			}{
				{"nornic", config.WithNornicParser},
				{"antlr", config.WithANTLRParser},
			} {
				parser := parser
				t.Run(parser.name, func(t *testing.T) {
					restore := parser.apply()
					defer restore()

					healthyName := workload.key + "-" + parser.name + "-healthy"
					graphName := workload.key + "-" + parser.name
					gdsOwnProjections(t, exec, healthyName, graphName)

					// The healthy control: the exact counts over this workload.
					started := time.Now()
					healthy, err := exec.Execute(context.Background(), fmt.Sprintf("CALL gds.graph.project('%s', 'P', 'K')", healthyName), nil)
					require.NoError(t, err, "healthy control")
					full := time.Since(started)
					require.Equal(t, int64(workload.nodes), healthy.Rows[0][3], "healthy nodeCount")
					require.Equal(t, int64(workload.edge), healthy.Rows[0][4], "healthy relationshipCount")
					t.Logf("healthy projection of %d nodes / %d relationships took %s", workload.nodes, workload.edge, full)

					// The same statement, cancelled while it is running.
					outcome := gdsCancelAfter(t, exec, graphName,
						fmt.Sprintf("CALL gds.graph.project('%s', 'P', 'K')", graphName), cancelDelay)
					t.Logf("cancelled %s into the healthy run: returned after %s, %s after the actual cancellation (released=%v); err=%v, published=%v",
						cancelDelay, outcome.total, outcome.afterCancel, outcome.released, outcome.err, outcome.published)
					require.True(t, outcome.released, "the workload must exercise actual cancellation; a completed healthy run cannot qualify this case")
					require.Error(t, outcome.err, "a cancelled request must not succeed (%s after the cancellation)", outcome.afterCancel)
					require.ErrorIs(t, outcome.err, context.Canceled, "the cancellation must surface as one")
					require.False(t, outcome.published, "a cancelled projection must not be published")
					require.LessOrEqual(t, outcome.afterCancel, stopBound,
						"the projection kept working %s after the request was cancelled, instead of stopping within %s", outcome.afterCancel, stopBound)

					// The name is free and a complete projection still works.
					result, err := exec.Execute(context.Background(), fmt.Sprintf("CALL gds.graph.project('%s', 'P', 'K')", graphName), nil)
					require.NoError(t, err, "retry after cancellation")
					require.Equal(t, int64(workload.nodes), result.Rows[0][3], "retried nodeCount")
					require.Equal(t, int64(workload.edge), result.Rows[0][4], "retried relationshipCount")
					listed, err := exec.Execute(context.Background(), fmt.Sprintf("CALL gds.graph.list('%s')", graphName), nil)
					require.NoError(t, err)
					require.Len(t, listed.Rows, 1, "the retried projection is listed once")
				})
			}
		})
	}
}

// TestGDSGraphProjectComponentCancellation is component coverage: it calls the
// canonical private procedure entry, callGdsGraphProject, on the deterministic
// procedure-scan fixture, and releases the caller's real CancelFunc or real
// deadline at an exact arrival of the population read. It asserts the exact
// delivered prefix, the cancellation's identity, no projection in the catalog,
// and a healthy retry with the exact counts.
func TestGDSGraphProjectComponentCancellation(t *testing.T) {
	for _, point := range []struct {
		key          string
		name         string
		after        int64
		nodes, edges int64
	}{
		{"five-nodes", "after five nodes", procedureScanCancelAfter, 5, 0},
		{"node-population-and-five-relationships", "after the node population and five relationships", procedureScanNodes + 5, procedureScanNodes, 5},
	} {
		point := point
		t.Run(point.name, func(t *testing.T) {
			fixture := newProcedureScanFixture(t)
			for _, mode := range procedureScanModes {
				mode := mode
				t.Run(mode.name, func(t *testing.T) {
					graphName := fmt.Sprintf("component-%s-%s", point.key, mode.name)
					ctx, trigger := procedureScanCaseContext(t, mode.waits, point.after)
					boundary := &procedureScanStreamBoundary{NamespacedEngine: fixture.store, trigger: trigger}
					exec := procedureScanExecutor(boundary)
					gdsOwnProjections(t, exec, graphName)

					result, err := exec.callGdsGraphProject(ctx, []interface{}{graphName, "Doc", "LINKS"})
					require.True(t, trigger.fired.Load(), "the caller's cancellation must be released at the population read")
					require.ErrorIs(t, err, mode.want)
					require.Nil(t, result, "a cancelled procedure call returns no result")
					require.Equal(t, point.nodes, boundary.acceptedNodes.Load(),
						"the node stream must deliver exactly the nodes up to the caller's cancellation")
					require.Equal(t, point.edges, boundary.acceptedEdges.Load(),
						"the relationship stream must deliver exactly the relationships up to the caller's cancellation")
					require.Equal(t, point.after, boundary.delivered.Load(),
						"the population read must deliver exactly the entities up to the caller's cancellation")
					require.Zero(t, boundary.bulkNodes.Load(), "the node population must be read on the caller's context")
					require.Zero(t, boundary.bulkEdges.Load(), "the edge population must be read on the caller's context")
					require.False(t, gdsProjectionPublished(exec, graphName), "a cancelled projection must not be published")

					// The healthy retry publishes the exact counts over the same
					// fixture.
					retryExec := procedureScanExecutor(&procedureScanStreamBoundary{NamespacedEngine: fixture.store})
					retry, err := retryExec.callGdsGraphProject(t.Context(), []interface{}{graphName, "Doc", "LINKS"})
					require.NoError(t, err, "retry after cancellation")
					require.Equal(t, int64(procedureScanNodes), retry.Rows[0][3], "retried nodeCount")
					require.Equal(t, int64(procedureScanRelationships), retry.Rows[0][4], "retried relationshipCount")
					listed, err := retryExec.Execute(t.Context(), fmt.Sprintf("CALL gds.graph.list('%s')", graphName), nil)
					require.NoError(t, err)
					require.Len(t, listed.Rows, 1, "the retried projection is listed once")
				})
			}
		})
	}
}

// gdsPostEdgeStreamBoundary is the minimal test-only boundary for the
// prepublication gate: it lets the relationship stream deliver every
// relationship normally and return nil, then cancels the caller's context
// before returning. The projection finishes building on a successful storage
// read, so only the statement's own gate can refuse to publish it. The case
// makes no claim about stage timing.
type gdsPostEdgeStreamBoundary struct {
	*storage.NamespacedEngine
	cancel context.CancelFunc
	edges  atomic.Int64
}

func (boundary *gdsPostEdgeStreamBoundary) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	if err := boundary.NamespacedEngine.StreamEdges(ctx, func(edge *storage.Edge) error {
		boundary.edges.Add(1)
		return visit(edge)
	}); err != nil {
		return err
	}
	boundary.cancel()
	return nil
}

// TestGDSGraphProjectRefusesPublicationAfterCancel is component coverage of the
// prepublication gate alone: the caller's cancellation is released after the
// relationship stream has delivered every fixture relationship and returned
// normally, so the storage work succeeded and the projection is fully built.
// The statement must still answer with the cancellation, return no result, and
// publish nothing; the same name then accepts a healthy retry.
func TestGDSGraphProjectRefusesPublicationAfterCancel(t *testing.T) {
	fixture := newProcedureScanFixture(t)
	graphName := "component-post-edge-stream"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	boundary := &gdsPostEdgeStreamBoundary{NamespacedEngine: fixture.store, cancel: cancel}
	exec := procedureScanExecutor(boundary)
	gdsOwnProjections(t, exec, graphName)

	result, err := exec.callGdsGraphProject(ctx, []interface{}{graphName, "Doc", "LINKS"})
	require.Equal(t, int64(procedureScanRelationships), boundary.edges.Load(),
		"the relationship stream must deliver every relationship normally before the caller's cancellation")
	t.Logf("post-stream cancellation: relationships=%d err=%v result_present=%v published=%v", boundary.edges.Load(), err, result != nil, gdsProjectionPublished(exec, graphName))
	require.ErrorIs(t, err, context.Canceled, "the gate must answer with the caller's cancellation")
	require.Nil(t, result, "a cancelled procedure call returns no result")
	require.False(t, gdsProjectionPublished(exec, graphName), "a cancelled projection must not be published")

	retryExec := procedureScanExecutor(&procedureScanStreamBoundary{NamespacedEngine: fixture.store})
	retry, err := retryExec.callGdsGraphProject(t.Context(), []interface{}{graphName, "Doc", "LINKS"})
	require.NoError(t, err, "retry after cancellation")
	require.Equal(t, int64(procedureScanNodes), retry.Rows[0][3], "retried nodeCount")
	require.Equal(t, int64(procedureScanRelationships), retry.Rows[0][4], "retried relationshipCount")
	require.True(t, gdsProjectionPublished(retryExec, graphName), "the healthy retry publishes its projection")
}
