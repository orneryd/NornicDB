package cypher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A storage fault is injected below public Cypher dispatch. No private access
// kernel is called: these observations require the statement owner, predicates,
// endpoint reader and fallback route to agree on the visible result.
type relationshipPublicReadProbe struct {
	storage.Engine
	mode   string
	cancel context.CancelFunc
	calls  atomic.Int64
}

func (p *relationshipPublicReadProbe) RelationshipPropertySeeksAvailable() bool {
	return p.Engine.(storage.RelationshipPropertySeeker).RelationshipPropertySeeksAvailable()
}

func (p *relationshipPublicReadProbe) SeekRelationships(ctx context.Context, typ string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	p.calls.Add(1)
	if p.mode == "error" {
		return nil, true, errors.New("public relationship access failure")
	}
	if p.mode == "declined" {
		return nil, false, nil
	}
	if p.mode == "cancel_before_seek" {
		p.cancel()
	}
	edges, answered, err := p.Engine.(storage.RelationshipPropertySeeker).SeekRelationships(ctx, typ, properties, values)
	if p.mode == "cancelled" {
		p.cancel()
	}
	for index, edge := range edges {
		copy := *edge
		switch p.mode {
		case "residual":
			copy.Properties = map[string]interface{}{"key": "nonmatching candidate"}
		case "missing_start":
			copy.StartNode = "missing"
		case "missing_end":
			copy.EndNode = "missing"
		case "wrong_start_label":
			copy.StartNode = edge.EndNode
		case "wrong_end_label":
			copy.EndNode = edge.StartNode
		}
		edges[index] = &copy
	}
	return edges, answered, err
}

func TestRelationshipAccessProgramPublicReadFaults(t *testing.T) {
	for _, mode := range []string{"normal", "declined", "error", "cancelled", "cancel_before_seek", "residual", "missing_start", "missing_end", "wrong_start_label", "wrong_end_label"} {
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_read_faults")
			setup := NewStorageExecutor(store)
			for _, query := range []string{
				"CREATE (:BoundaryLeft)-[:BOUNDARY {key:'hit'}]->(:BoundaryRight)",
				"CREATE RANGE INDEX public_read_key FOR ()-[r:BOUNDARY]-() ON (r.key)",
			} {
				_, err := setup.Execute(context.Background(), query, nil)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			probe := &relationshipPublicReadProbe{Engine: store, mode: mode, cancel: cancel}
			exec := NewStorageExecutor(probe)
			result, err := exec.Execute(ctx, "PROFILE MATCH (a:BoundaryLeft)-[r:BOUNDARY]->(b:BoundaryRight) WHERE r.key=$key RETURN r.key", map[string]interface{}{"key": "hit"})
			require.Positive(t, probe.calls.Load(), "public query must reach the read capability")
			switch mode {
			case "error":
				require.ErrorContains(t, err, "public relationship access failure")
			case "cancelled", "cancel_before_seek":
				require.ErrorIs(t, err, context.Canceled)
			case "normal", "declined":
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"hit"}}, result.Rows)
			default:
				require.NoError(t, err)
				require.Empty(t, result.Rows)
			}
		})
	}
}

func TestRelationshipAccessProgramPublicIDEndpoints(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_id_endpoints"))
	_, err := exec.Execute(context.Background(), "CREATE (a:BoundaryLeft {name:'a'})-[:BOUNDARY {key:'hit'}]->(b:BoundaryRight {name:'b'}), (:BoundaryLeft {name:'c'})", nil)
	require.NoError(t, err)
	ids, err := exec.Execute(context.Background(), "MATCH ()-[r:BOUNDARY]->() RETURN id(r)", nil)
	require.NoError(t, err)
	require.Len(t, ids.Rows, 1)
	for _, test := range []struct {
		name, query string
		want        int
	}{
		{"type", "MATCH ()-[r:OTHER]->() WHERE id(r)=$id RETURN r.key", 0},
		{"inline_property", "MATCH ()-[r:BOUNDARY {key:'other'}]->() WHERE id(r)=$id RETURN r.key", 0},
		{"start_label", "MATCH (:BoundaryRight)-[r:BOUNDARY]->() WHERE id(r)=$id RETURN r.key", 0},
		{"end_label", "MATCH ()-[r:BOUNDARY]->(:BoundaryLeft) WHERE id(r)=$id RETURN r.key", 0},
		{"start_property", "MATCH ({name:'c'})-[r:BOUNDARY]->() WHERE id(r)=$id RETURN r.key", 0},
		{"end_property", "MATCH ()-[r:BOUNDARY]->({name:'c'}) WHERE id(r)=$id RETURN r.key", 0},
		{"bound_seed", "MATCH (a:BoundaryLeft {name:'c'}) WITH a MATCH (a)-[r:BOUNDARY]->() WHERE id(r)=$id RETURN r.key", 0},
		{"incoming", "MATCH ()<-[r:BOUNDARY]-() WHERE id(r)=$id RETURN r.key", 1},
		{"undirected", "MATCH ()-[r:BOUNDARY]-() WHERE id(r)=$id RETURN r.key", 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), "PROFILE "+test.query, map[string]interface{}{"id": ids.Rows[0][0]})
			require.NoError(t, err)
			require.Len(t, result.Rows, test.want)
			for _, row := range result.Rows {
				require.Equal(t, []interface{}{"hit"}, row)
			}
		})
	}
}

func TestRelationshipAccessProgramPublicPendingView(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "public_pending_view"))
	for _, query := range []string{
		"CREATE RANGE INDEX pending_key FOR ()-[r:PENDING]-() ON (r.key)",
		"BEGIN",
		"CREATE ()-[:PENDING {key:'hit'}]->(), ()-[:PENDING {key:'other'}]->(), ()-[:PENDING]->(), ()-[:OTHER {key:'hit'}]->()",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	result, err := exec.Execute(context.Background(), "PROFILE MATCH ()-[r:PENDING]->() WHERE r.key='hit' RETURN r.key", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"hit"}}, result.Rows)
	_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
	result, err = exec.Execute(context.Background(), "MATCH ()-[r:PENDING]->() RETURN r.key", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}

func TestRelationshipAccessProgramPublicDegrees(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_degrees"))
	_, err := exec.Execute(context.Background(), "CREATE (a:DegreeNode {name:'a'})-[:DEGREE]->(b:DegreeNode {name:'b'}), (a)-[:DEGREE]->(a)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(context.Background(), "MATCH (n:DegreeNode) RETURN n.name, COUNT { MATCH (n)--() }, COUNT { MATCH ()-->(n) }, COUNT { MATCH (n)-->() }", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{"a", int64(2), int64(1), int64(2)}, {"b", int64(1), int64(1), int64(0)}}, result.Rows)
}
