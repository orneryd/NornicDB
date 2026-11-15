package cypher

// Report 38.9: LIMIT 1 on a pattern whose start and end are the same variable
// reads every match. The reported population is 1,000 labelled nodes with one
// SELF relationship each; MATCH (p:P)-[:SELF]->(p) RETURN p.id LIMIT 1 made
// 1,000 adjacency reads, while the control MATCH (p:P)-[:SELF]->(q) RETURN
// p.id LIMIT 1 made one.
//
// A pattern that names one variable at both ends admits only a path that ends
// at the node it started from, so the traversal's own endpoint identity
// decides the row and LIMIT may stop the start-side walk once one such path
// exists. These probes observe the public Execute path only, through the
// package's adjacencyReadSpy: calls is the number of adjacent-edge requests the
// traversal made, and the spy below adds the node each counted request walked,
// so a returned row is tied to the loop the traversal actually read instead of
// to a scan order. A traversal without an early cap walks the population with
// the parallel expander, so the observation is serialized.
//
// The probe executors' query result cache is disabled so every execution reads
// adjacency instead of answering a previous execution's rows.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// repeatedEndpointPopulation is the reported fixtures' size: one SELF
// relationship per labelled node.
const repeatedEndpointPopulation = 1000

// repeatedEndpointStatement is the reported statement and
// repeatedEndpointControl its different-endpoint control.
const (
	repeatedEndpointStatement = "MATCH (p:P)-[:SELF]->(p) RETURN p.id LIMIT 1"
	repeatedEndpointControl   = "MATCH (p:P)-[:SELF]->(q) RETURN p.id LIMIT 1"
)

// repeatedEndpointReadSpy is the package's adjacencyReadSpy (one call per
// adjacent-edge request) plus the node each counted request walked. Its lock
// covers the embedded counters too, because an uncapped traversal expands the
// population from several goroutines at once.
type repeatedEndpointReadSpy struct {
	adjacencyReadSpy
	mu    sync.Mutex
	nodes []storage.NodeID
}

func newRepeatedEndpointReadSpy(store storage.Engine) *repeatedEndpointReadSpy {
	spy := &repeatedEndpointReadSpy{}
	spy.adjacencyReadSpy.Engine = store
	return spy
}

func (s *repeatedEndpointReadSpy) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes = append(s.nodes, request.Node)
	return s.adjacencyReadSpy.ReadAdjacentEdges(ctx, request)
}

func (s *repeatedEndpointReadSpy) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adjacencyReadSpy.reset()
	s.nodes = nil
}

// readIndexes is the population index of every node an adjacency read walked.
func (s *repeatedEndpointReadSpy) readIndexes(t *testing.T) []int64 {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	indexes := make([]int64, 0, len(s.nodes))
	for _, node := range s.nodes {
		indexes = append(indexes, repeatedEndpointNodeIndex(t, node))
	}
	return indexes
}

// repeatedEndpointStore builds the reported population: nodes node-0000 ...
// node-0999 labelled P with an integer id, and one SELF relationship per node,
// edge-0000 ... edge-0999. With loops every relationship joins a node to
// itself; without, each joins node i to node i+1 (wrapping at the last node),
// so no SELF relationship of that population is a loop.
func repeatedEndpointStore(t testing.TB, loops bool) storage.Engine {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2cost")
	ctx := context.Background()
	for index := 0; index < repeatedEndpointPopulation; index++ {
		_, err := store.CreateNode(ctx, &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("node-%04d", index)),
			Labels:     []string{"P"},
			Properties: map[string]interface{}{"id": int64(index)},
		})
		require.NoError(t, err)
	}
	for index := 0; index < repeatedEndpointPopulation; index++ {
		end := index
		if !loops {
			end = (index + 1) % repeatedEndpointPopulation
		}
		require.NoError(t, store.CreateEdge(ctx, &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("edge-%04d", index)),
			StartNode: storage.NodeID(fmt.Sprintf("node-%04d", index)),
			EndNode:   storage.NodeID(fmt.Sprintf("node-%04d", end)),
			Type:      "SELF",
		}))
	}
	return store
}

// repeatedEndpointNodeIndex is the population index of the node an adjacency
// read walked. A read addresses the namespaced node ID, so the node's own
// node-NNNN ID is its suffix.
func repeatedEndpointNodeIndex(t *testing.T, id storage.NodeID) int64 {
	t.Helper()
	name := string(id)
	if marker := strings.LastIndex(name, "node-"); marker >= 0 {
		name = name[marker+len("node-"):]
	}
	index, err := strconv.ParseInt(name, 10, 64)
	require.NoError(t, err, "adjacency read of %q", string(id))
	return index
}

// TestRepeatedEndpointLimitStopsAtTheSelfLoop is the reported statement: a
// population where every node owns a self-loop must answer LIMIT 1 from the
// loop the one adjacency read found, not by reading all 1,000 matches.
func TestRepeatedEndpointLimitStopsAtTheSelfLoop(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		spy := newRepeatedEndpointReadSpy(repeatedEndpointStore(t, true))
		exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0, TTL: 0})

		result, err := exec.Execute(context.Background(), repeatedEndpointStatement, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		id, ok := result.Rows[0][0].(int64)
		require.True(t, ok, "p.id is an integer: %v", result.Rows[0][0])
		require.Equal(t, 1, spy.calls, "LIMIT 1 must stop the start-side walk after the first self-loop")
		read := spy.readIndexes(t)
		require.Len(t, read, 1)
		require.Equal(t, id, read[0], "the row must be the loop of the node the traversal read")
	})
}

// TestRepeatedEndpointLimitWindow keeps the LIMIT window's contract on the same
// population: two rows come from two self-loops, and SKIP declines the early
// cap (pipelineMatchHint admits no early limit when a RETURN carries a nonzero
// SKIP), so the window's rows are still rows of real self-loops.
func TestRepeatedEndpointLimitWindow(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		spy := newRepeatedEndpointReadSpy(repeatedEndpointStore(t, true))
		exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		ctx := context.Background()

		result, err := exec.Execute(ctx, "MATCH (p:P)-[:SELF]->(p) RETURN p.id LIMIT 2", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)
		require.Equal(t, 2, spy.calls, "LIMIT 2 must stop the start-side walk after two self-loops")
		read := spy.readIndexes(t)
		require.Len(t, read, 2)
		require.NotEqual(t, result.Rows[0][0], result.Rows[1][0], "two rows are two self-loops")
		for position, row := range result.Rows {
			id, ok := row[0].(int64)
			require.True(t, ok)
			require.Equal(t, id, read[position],
				"row %d must be the loop of the node its read walked", position)
		}

		spy.reset()
		skipped, err := exec.Execute(ctx, "MATCH (p:P)-[:SELF]->(p) RETURN p.id SKIP 1 LIMIT 1", nil)
		require.NoError(t, err)
		require.Len(t, skipped.Rows, 1)
		id, ok := skipped.Rows[0][0].(int64)
		require.True(t, ok)
		require.Contains(t, spy.readIndexes(t), id,
			"a SKIP window row must be a self-loop of a node the traversal read")
	})
}

// TestRepeatedEndpointRejectsNonLoops is the genuine rejection the identity
// rule owes: with no self-loop in the population the statement returns no row,
// however many candidates the walk visits, and the control still stops at the
// first relationship.
func TestRepeatedEndpointRejectsNonLoops(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		spy := newRepeatedEndpointReadSpy(repeatedEndpointStore(t, false))
		exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		ctx := context.Background()

		result, err := exec.Execute(ctx, repeatedEndpointStatement, nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows, "a population of non-loop SELF relationships matches no (p)-[:SELF]->(p) row")
		require.Equal(t, repeatedEndpointPopulation, spy.calls,
			"every candidate is walked and rejected, so the empty result is not a LIMIT artifact")

		spy.reset()
		control, err := exec.Execute(ctx, repeatedEndpointControl, nil)
		require.NoError(t, err)
		require.Len(t, control.Rows, 1)
		require.Equal(t, 1, spy.calls, "the different-endpoint control stops at its first relationship")
	})
}

// TestRepeatedEndpointChainKeepsTheIdentityRule covers the chain owner that
// states the same rule for a pattern whose two ends are positions of one chain
// (chainRepeatedNodes). The fixture holds two 2-cycles with distinct edges,
// p0 -> q0 -> p0 and p1 -> q1 -> p1, plus a two-hop path p0 -> q1 -> p2 that
// ends at a different P node: only the paths back to their own start node are
// rows, so the statement answers ids 0 and 1 and never 2.
func TestRepeatedEndpointChainKeepsTheIdentityRule(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2chain")
		ctx := context.Background()
		create := func(id string, labels []string, properties map[string]interface{}) {
			_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(id), Labels: labels, Properties: properties})
			require.NoError(t, err)
		}
		create("p0", []string{"P"}, map[string]interface{}{"id": int64(0)})
		create("p1", []string{"P"}, map[string]interface{}{"id": int64(1)})
		create("p2", []string{"P"}, map[string]interface{}{"id": int64(2)})
		create("q0", []string{"Q"}, nil)
		create("q1", []string{"Q"}, nil)
		for _, edge := range [][3]string{
			{"a0", "p0", "q0"}, {"b0", "q0", "p0"},
			{"a1", "p1", "q1"}, {"b1", "q1", "p1"},
			{"cross-in", "p0", "q1"}, {"cross-out", "q1", "p2"},
		} {
			require.NoError(t, store.CreateEdge(ctx, &storage.Edge{
				ID: storage.EdgeID(edge[0]), StartNode: storage.NodeID(edge[1]), EndNode: storage.NodeID(edge[2]), Type: "SELF",
			}))
		}
		exec := NewStorageExecutor(newRepeatedEndpointReadSpy(store), QueryCachePolicy{MaxEntries: 0, TTL: 0})

		result, err := exec.Execute(ctx, "MATCH (p:P)-[:SELF]->(q)-[:SELF]->(p) RETURN p.id", nil)
		require.NoError(t, err)
		ids := make([]int64, 0, len(result.Rows))
		for _, row := range result.Rows {
			id, ok := row[0].(int64)
			require.True(t, ok)
			ids = append(ids, id)
		}
		sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
		require.Equal(t, []int64{0, 1}, ids,
			"only a chain path back to its own start node is a row; p0 -> q1 -> p2 ends elsewhere")

		limited, err := exec.Execute(ctx, "MATCH (p:P)-[:SELF]->(q)-[:SELF]->(p) RETURN p.id LIMIT 1", nil)
		require.NoError(t, err)
		require.Len(t, limited.Rows, 1)
		limitedID, ok := limited.Rows[0][0].(int64)
		require.True(t, ok)
		require.Contains(t, []int64{0, 1}, limitedID)
	})
}

func BenchmarkRepeatedEndpointLimitedRead(b *testing.B) {
	store := repeatedEndpointStore(b, true)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(b.Context(), repeatedEndpointStatement, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("expected one self-loop row, got %d", len(result.Rows))
		}
	}
}
