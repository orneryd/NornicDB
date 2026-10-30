package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countStoreGraph: Users 1, 2 and 3 (also Admin), Others 4 and 5, an
// unlabelled 6; seven :Friend (one a self-loop on 1) and two :Likes.
func countStoreGraph(t *testing.T) (*StorageExecutor, storage.Engine) {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	_, err = exec.Execute(context.Background(), `CREATE (a:User {id: 1}), (b:User {id: 2}), (c:User:Admin {id: 3}),
		(x:Other {id: 4}), (y:Other {id: 5}), (z {id: 6}),
		(a)-[:Friend]->(b), (b)-[:Friend]->(c), (c)-[:Friend]->(a), (a)-[:Friend]->(x), (b)-[:Friend]->(y),
		(a)-[:Friend]->(a), (x)-[:Likes {w: 1}]->(a), (z)-[:Likes]->(b), (c)-[:Friend]->(z)`, nil)
	require.NoError(t, err)
	return exec, store
}

// Pattern counts, with Neo4j 5.26.30's results. Both endpoints labelled was
// answered from one endpoint's counter, ignoring the other label (#1076).
var countStoreCases = []struct {
	query string
	want  int64
}{
	{"MATCH (:User)-[r:Friend]->(:User) RETURN count(r)", 4},
	{"MATCH (:User)-[r:Friend]->(:Other) RETURN count(r)", 2},
	{"MATCH (:User)-[r:Friend|Likes]->(:User) RETURN count(r)", 4},
	{"MATCH (:User)<-[r:Friend]-(:Other) RETURN count(r)", 0},
	{"MATCH (:Other)<-[r:Friend]-(:User) RETURN count(r)", 2},
	{"MATCH (:User)-[r:Friend]->(:Missing) RETURN count(r)", 0},
	{"MATCH (a:User)-[r:Friend]->(b:User) RETURN count(*)", 4},
	{"MATCH (:User)-[r:Friend]->() RETURN count(r)", 7},
	{"MATCH ()-[r:Friend]->(:User) RETURN count(r)", 4},
	{"MATCH ()<-[r:Friend]-(:User) RETURN count(r)", 7},
	{"MATCH (:User:Admin)-[r:Friend]->() RETURN count(r)", 2},
	{"MATCH ()-[r]->() RETURN count(r)", 9},
	{"MATCH ()-[r:Friend]->() RETURN count(*)", 7},
	{"MATCH ()-[r:Friend]-() RETURN count(r)", 13},
	{"MATCH (n:User) RETURN count(n)", 3},
	{"MATCH (n:User:Admin) RETURN count(n)", 1},
}

func TestCountFromStoreMatchesNeo4j(t *testing.T) {
	ctx := context.Background()
	exec, _ := countStoreGraph(t)
	for _, tc := range countStoreCases {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, tc.query)
	}
}

// The owner declines what the counters can't answer, so the general path
// does: two labelled endpoints, two labels on one, a label without a type,
// properties, a repeated variable, variable length, a chain, and
// count of anything but the rows.
func TestRelationshipCountFromStoreDeclines(t *testing.T) {
	ctx := context.Background()
	exec, _ := countStoreGraph(t)
	for _, pattern := range []string{
		"(:User)-[r:Friend]->(:User)", "(:User:Admin)-[r:Friend]->()", "(:User)-[r]->()", "(:User)-[r:Friend]-()",
		"({id: 1})-[r:Friend]->()", "()-[r:Likes {w: 1}]->()", "(a)-[r:Friend]->(a)", "()-[r:Friend*1..2]->()",
		"(:User)-[r:Friend]->()-[s:Friend]->()",
	} {
		_, ok, err := exec.relationshipCountFromStore(ctx, exec.parseTraversalPattern(ctx, originalBraceView(pattern)), returnItem{expr: "count(r)"})
		require.NoError(t, err, pattern)
		require.False(t, ok, pattern)
	}
	matches := exec.parseTraversalPattern(ctx, originalBraceView("()-[r:Friend]->()"))
	for _, expr := range []string{"count(DISTINCT r)", "count(r) + 1", "sum(r)", "count(x)"} {
		_, ok, _ := exec.relationshipCountFromStore(ctx, matches, returnItem{expr: expr})
		require.False(t, ok, expr)
	}
	_, ok, _ := exec.relationshipCountFromStore(ctx, nil, returnItem{expr: "count(*)"})
	require.False(t, ok)
	for _, node := range []nodePatternInfo{{variable: "n"}, {variable: "n", labels: []string{"A", "B"}}, {variable: "n", labels: []string{"A"}, properties: map[string]interface{}{"id": 1}}} {
		_, ok, _ := exec.nodeCountFromStore(ctx, node, returnItem{expr: "count(n)"})
		require.False(t, ok)
	}
	count, ok, err := exec.nodeCountFromStore(ctx, nodePatternInfo{variable: "n", labels: []string{"User"}}, returnItem{expr: "count(*)"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(3), count)
}

type decayFilteringEngine struct{ storage.Engine }

func (decayFilteringEngine) IsDecayEnabled() bool { return true }

// Counters include what decay hides, and a temporal viewport reads another
// version: neither route answers from them then.
func TestCountFromStoreNotUnderDecayOrViewport(t *testing.T) {
	ctx := context.Background()
	_, store := countStoreGraph(t)
	decayed := NewStorageExecutor(decayFilteringEngine{Engine: store})
	matches := decayed.parseTraversalPattern(ctx, originalBraceView("()-[r:Friend]->()"))
	_, ok, _ := decayed.relationshipCountFromStore(ctx, matches, returnItem{expr: "count(r)"})
	require.False(t, ok)
	_, ok, _ = decayed.nodeCountFromStore(ctx, nodePatternInfo{variable: "n", labels: []string{"User"}}, returnItem{expr: "count(n)"})
	require.False(t, ok)

	plain := NewStorageExecutor(store)
	viewport := WithTemporalViewport(ctx, AsOfTemporalViewport(time.Now()))
	_, ok, _ = plain.relationshipCountFromStore(viewport, plain.parseTraversalPattern(ctx, originalBraceView("()-[r:Friend]->()")), returnItem{expr: "count(r)"})
	require.False(t, ok)
	_, ok, _ = plain.nodeCountFromStore(viewport, nodePatternInfo{variable: "n", labels: []string{"User"}}, returnItem{expr: "count(n)"})
	require.False(t, ok)
}

// Inside an explicit transaction that has written, every count sees its
// writes: the untyped total and the node total did not (#1076).
func TestCountFromStoreSeesTransactionWrites(t *testing.T) {
	ctx := context.Background()
	_, store := countStoreGraph(t)
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(WithTransactionControl(ctx), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(WithTransactionControl(ctx), "ROLLBACK", nil) })
	// Before the transaction writes, the committed totals.
	_, err = exec.Execute(ctx, "MATCH (n:User) RETURN count(n)", nil)
	require.NoError(t, err)
	committed := exec.txContext.storageWrapper
	require.NotNil(t, committed)
	nodesBefore, err := committed.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(6), nodesBefore)
	edgesBefore, err := committed.EdgeCount()
	require.NoError(t, err)
	require.Equal(t, int64(9), edgesBefore)
	_, err = exec.Execute(ctx, "MATCH (a:User {id: 1}), (x:Other {id: 4}) CREATE (a)-[:Friend]->(x), (x)-[:Friend]->(a), (:User {id: 7})-[:Friend]->(:Other {id: 8})", nil)
	require.NoError(t, err)
	for query, want := range map[string]int64{
		"MATCH ()-[r]->() RETURN count(r)":                    12,
		"MATCH ()-[r:Friend]->() RETURN count(r)":             10,
		"MATCH (:User)-[r:Friend]->(:Other) RETURN count(r)":  4,
		"MATCH (:User)-[r:Friend]->(:User) RETURN count(r)":   4,
		"MATCH (:Other)-[r:Friend]->(:Other) RETURN count(r)": 0,
		"MATCH (n:User) RETURN count(n)":                      4,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	wrapper := exec.txContext.storageWrapper
	nodes, err := wrapper.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(8), nodes)
	edges, err := wrapper.EdgeCount()
	require.NoError(t, err)
	require.Equal(t, int64(12), edges)
}

type failingCountEngine struct {
	storage.Engine
	nilEdges bool
}

var errCountStore = errors.New("counter unavailable")

func (f failingCountEngine) EdgeCountByStartLabel(string, string) (int64, error) {
	return 0, errCountStore
}

func (f failingCountEngine) EdgeCountByEndLabel(string, string) (int64, error) {
	return 0, errCountStore
}

func (f failingCountEngine) NodeCountByLabel(string) (int64, error) { return 0, errCountStore }

func (f failingCountEngine) AllEdges() ([]*storage.Edge, error) {
	if f.nilEdges {
		return []*storage.Edge{nil}, nil
	}
	return nil, errCountStore
}
func (f failingCountEngine) GetEdgesByType(string) ([]*storage.Edge, error) {
	if f.nilEdges {
		return []*storage.Edge{nil}, nil
	}
	return nil, errCountStore
}

// A counter or scan that fails is the statement's error; nil entries and a
// type listed twice are counted once (#1076).
func TestCountFromStoreErrorsAndDuplicates(t *testing.T) {
	ctx := context.Background()
	exec, store := countStoreGraph(t)
	result, err := exec.Execute(ctx, "MATCH ()-[r:Friend|Friend]-() RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(13)}}, result.Rows)

	failing := NewStorageExecutor(failingCountEngine{Engine: store})
	for _, pattern := range []string{"(:User)-[r:Friend]->()", "()-[r:Friend]->(:User)", "()-[r]-()", "()-[r:Friend]-()"} {
		_, ok, err := failing.relationshipCountFromStore(ctx, failing.parseTraversalPattern(ctx, originalBraceView(pattern)), returnItem{expr: "count(r)"})
		require.True(t, ok, pattern)
		require.ErrorIs(t, err, errCountStore, pattern)
	}
	_, err = failing.Execute(ctx, "MATCH (n:User) RETURN count(n)", nil)
	require.ErrorIs(t, err, errCountStore)
	nils := NewStorageExecutor(failingCountEngine{Engine: store, nilEdges: true})
	for _, pattern := range []string{"()-[r]-()", "()-[r:Friend]-()"} {
		count, ok, err := nils.relationshipCountFromStore(ctx, nils.parseTraversalPattern(ctx, originalBraceView(pattern)), returnItem{expr: "count(r)"})
		require.NoError(t, err)
		require.True(t, ok)
		require.Zero(t, count, pattern)
	}
}
