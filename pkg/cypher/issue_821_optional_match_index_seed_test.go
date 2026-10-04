package cypher

// Regression coverage for NornicDB #821: MATCH (p:Person {id: $id}) followed
// by OPTIONAL MATCH streamed the whole :Person label to collect the seed,
// although a property index covers id (the same plain MATCH used the index).
// The seed collector now probes the index for the pattern's inline
// properties, and the traversal-seeded route uses the same indexed MATCH seed.

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// labelStreamCountingEngine counts the nodes handed out by label and node
// scans.
type labelStreamCountingEngine struct {
	*storage.NamespacedEngine
	nodesVisited int64
	scanErr      error
}

func (s *labelStreamCountingEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	if s.scanErr != nil {
		return s.scanErr
	}
	return s.NamespacedEngine.StreamNodesByLabelProjected(label, properties, func(node *storage.Node) error {
		s.nodesVisited++
		return visit(node)
	})
}

func (s *labelStreamCountingEngine) StreamNodes(ctx context.Context, fn func(node *storage.Node) error) error {
	return s.NamespacedEngine.StreamNodes(ctx, func(node *storage.Node) error {
		s.nodesVisited++
		return fn(node)
	})
}

func (s *labelStreamCountingEngine) AllNodes() ([]*storage.Node, error) {
	nodes, err := s.NamespacedEngine.AllNodes()
	s.nodesVisited += int64(len(nodes))
	return nodes, err
}

func (s *labelStreamCountingEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	nodes, err := s.NamespacedEngine.GetNodesByLabel(label)
	s.nodesVisited += int64(len(nodes))
	return nodes, err
}

func TestIssue821OptionalMatchSeedUsesPropertyIndex(t *testing.T) {
	ctx := context.Background()
	spy := &labelStreamCountingEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutor(spy)
	run := func(query string, params map[string]interface{}) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result
	}
	run("CREATE INDEX person_id FOR (p:Person) ON (p.id)", nil)
	run("UNWIND range(0, 199) AS i CREATE (:Person {id: i, age: i % 50})", nil)
	// Person 7 knows 8 and 9 and is known by 10; Person 11 has no relationships.
	run("UNWIND [[7, 8], [7, 9], [10, 7]] AS pair MATCH (a:Person {id: pair[0]}), (b:Person {id: pair[1]}) CREATE (a)-[:KNOWS]->(b)", nil)

	for _, tc := range []struct {
		query string
		id    int64
		want  [][]interface{}
	}{
		{"MATCH (p:Person {id: $id}) OPTIONAL MATCH (p)<-[:KNOWS]-(f) RETURN p.id, count(f)", 7, [][]interface{}{{int64(7), int64(1)}}},
		{"MATCH (p:Person {id: $id}) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN count(f)", 7, [][]interface{}{{int64(2)}}},
		{"MATCH (p:Person {id: $id}) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN f.id ORDER BY f.id", 7, [][]interface{}{{int64(8)}, {int64(9)}}},
		{"MATCH (p:Person {id: $id}) OPTIONAL MATCH (p)<-[:KNOWS]-(f) RETURN p.id, f.id", 11, [][]interface{}{{int64(11), nil}}},
		{"MATCH (p:Person {id: $id}) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.id, f.id", 11, [][]interface{}{{int64(11), nil}}},
		{"MATCH (p:Person {id: $id}) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.id, f.id", 500, [][]interface{}{}},
		// A float equal to the stored integer matches, as in Neo4j.
		{"MATCH (p:Person {id: 7.0}) OPTIONAL MATCH (p)<-[:KNOWS]-(f) RETURN p.id, f.id", 0, [][]interface{}{{int64(7), int64(10)}}},
		// An indexed value no node holds is answered by the index too.
		{"MATCH (p:Person {id: $id}) RETURN p.id", 500, [][]interface{}{}},
		{"MATCH (p {id: $id}) RETURN p.id", 500, [][]interface{}{}},
		{"MERGE (p:Person {id: $id}) RETURN p.id", 600, [][]interface{}{{int64(600)}}},
		{"MATCH (p:Person {id: $id}) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.id, count(f)", 600, [][]interface{}{{int64(600), int64(0)}}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			spy.nodesVisited = 0
			result := run(tc.query, map[string]interface{}{"id": tc.id})
			require.Equal(t, tc.want, result.Rows)
			require.Zero(t, spy.nodesVisited, "an indexed lookup must not scan nodes")
		})
	}

	// Without an index on the property the label is streamed, and the
	// pattern's properties are compared with Cypher equality.
	result := run("MATCH (p:Person {age: 7.0}) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.id, f.id ORDER BY p.id, f.id", nil)
	require.Equal(t, [][]interface{}{{int64(7), int64(8)}, {int64(7), int64(9)}, {int64(57), nil}, {int64(107), nil}, {int64(157), nil}}, result.Rows)

	// An index files no list, so a list value is not proven absent by an
	// empty lookup: the label scan decides.
	run("CREATE INDEX person_tags FOR (p:Person) ON (p.tags)", nil)
	run("MATCH (p:Person {id: 7}) SET p.tags = [1, 2]", nil)
	result = run("MATCH (p:Person {tags: [1, 2]}) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.id, count(f)", nil)
	require.Equal(t, [][]interface{}{{int64(7), int64(2)}}, result.Rows)
}

func TestPropertyIndexMissIsAuthoritative(t *testing.T) {
	for _, value := range []interface{}{"a", true, 1, int64(1), uint8(1), 1.5, float32(1)} {
		require.True(t, propertyIndexMissIsAuthoritative(value), "%T", value)
	}
	for _, value := range []interface{}{nil, []interface{}{int64(1)}, map[string]interface{}{"a": int64(1)}, struct{}{}} {
		require.False(t, propertyIndexMissIsAuthoritative(value), "%T", value)
	}
}

func TestIssue821OptionalMatchSeedFiltersWhereIndexCandidatesByPatternProperties(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	for _, query := range []string{
		"CREATE INDEX person_id FOR (p:Person) ON (p.id)",
		"CREATE (:Person {id: 1, team: 'a'}), (:Person {id: 2, team: 'b'}), (:Person {id: 3, tags: [1, 2]})",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		pattern nodePatternInfo
		where   string
		want    []interface{}
	}{
		{nodePatternInfo{variable: "p", labels: []string{"Person"}, properties: map[string]interface{}{"team": "a"}}, "p.id IN [1, 2]", []interface{}{int64(1)}},
		{nodePatternInfo{variable: "p", labels: []string{"Person"}, properties: map[string]interface{}{"tags": []interface{}{int64(1), int64(2)}}}, "p.id IN [1, 3]", []interface{}{int64(3)}},
		{nodePatternInfo{variable: "p", labels: []string{"Person"}, properties: map[string]interface{}{"team": nil}}, "p.id IN [1, 2, 3]", []interface{}{}},
	} {
		nodes, err := exec.collectOptionalMatchInitialNodes(ctx, tc.pattern, tc.where, "", nil)
		require.NoError(t, err)
		got := make([]interface{}, 0, len(nodes))
		for _, node := range nodes {
			got = append(got, node.Properties["id"])
		}
		require.Equal(t, tc.want, got)
	}
}

func TestOptionalMatchSharedRoutePropagatesSeedLookupErrors(t *testing.T) {
	seedError := errors.New("seed lookup failed")
	executor := NewStorageExecutor(&labelStreamCountingEngine{
		NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"),
		scanErr:          seedError,
	})
	for _, query := range []string{
		"MATCH (p:ErrSeed) OPTIONAL MATCH (p)<-[:KNOWS]-(f) RETURN p.id, count(f)",
		"MATCH (p:ErrSeed) OPTIONAL MATCH (p)<-[:ORDERS]-(f) RETURN p.productName, count(f)",
		"MATCH (p:ErrSeed) OPTIONAL MATCH (p)<-[:ORDERS]-(f) WITH p RETURN p.productName",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), query, nil)
			require.ErrorIs(t, err, seedError)
			require.Nil(t, result)
		})
	}
}
