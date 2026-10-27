package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRemotePublicMutationBindingsAndAdmission(t *testing.T) {
	props := map[string]interface{}{"name": "updated", "score": int64(8)}
	transportErr := errors.New("remote mutation failure")
	for _, operation := range []struct {
		name, statement, id string
		update              bool
		run                 func(*RemoteEngine) error
	}{
		{"update node", "MATCH (n) WHERE n.id = $id SET n += $props RETURN count(n)", "node:one", true, func(e *RemoteEngine) error { return e.UpdateNode(&Node{ID: "node:one", Properties: props}) }},
		{"update edge", "MATCH ()-[r]->() WHERE r.id = $id SET r += $props RETURN count(r)", "edge:one", true, func(e *RemoteEngine) error { return e.UpdateEdge(&Edge{ID: "edge:one", Properties: props}) }},
		{"delete node", "MATCH (n) WHERE n.id = $id DETACH DELETE n", "node:one", false, func(e *RemoteEngine) error { return e.DeleteNode("node:one") }},
		{"delete edge", "MATCH ()-[r]->() WHERE r.id = $id DELETE r", "edge:one", false, func(e *RemoteEngine) error { return e.DeleteEdge("edge:one") }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			for _, response := range []struct {
				name string
				rows [][]interface{}
				err  error
			}{
				{"matched", [][]interface{}{{int64(1)}}, nil},
				{"zero matches", [][]interface{}{{int64(0)}}, nil},
				{"no rows", nil, nil},
				{"empty row", [][]interface{}{{}}, nil},
				{"transport failure", nil, transportErr},
			} {
				t.Run(response.name, func(t *testing.T) {
					calls := 0
					engine := &RemoteEngine{transport: &fakeRemoteTransport{queryFn: func(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
						calls++
						require.Equal(t, operation.statement, statement)
						want := map[string]interface{}{"id": operation.id}
						if operation.update {
							want["props"] = props
						}
						require.Equal(t, want, params)
						_, bounded := ctx.Deadline()
						require.True(t, bounded, "public Engine operations must retain their default timeout")
						return nil, response.rows, response.err
					}}}
					err := operation.run(engine)
					if response.err != nil {
						require.Same(t, transportErr, err)
					} else if operation.update && response.name != "matched" {
						require.ErrorIs(t, err, ErrNotFound)
					} else {
						require.NoError(t, err)
					}
					require.Equal(t, 1, calls)
					require.Equal(t, map[string]interface{}{"name": "updated", "score": int64(8)}, props)
				})
			}
		})
	}
}

func TestRemotePublicEdgeListDirectionsAndValues(t *testing.T) {
	const edgeType = "REL`TYPE"
	props := map[string]interface{}{"id": "edge:one", "weight": int64(9)}
	wire := map[string]interface{}{"id": "edge:one", "type": edgeType, "startNodeElementId": "node:start", "endNodeElementId": "node:end", "properties": props}
	transportErr := errors.New("remote edge list failure")
	for _, operation := range []struct {
		name, statement string
		params          map[string]interface{}
		run             func(*RemoteEngine) ([]*Edge, error)
	}{
		{"outgoing", "MATCH (n)-[r]->() WHERE n.id = $id RETURN r", map[string]interface{}{"id": "node:start"}, func(e *RemoteEngine) ([]*Edge, error) { return e.GetOutgoingEdges("node:start") }},
		{"incoming", "MATCH ()-[r]->(n) WHERE n.id = $id RETURN r", map[string]interface{}{"id": "node:end"}, func(e *RemoteEngine) ([]*Edge, error) { return e.GetIncomingEdges("node:end") }},
		{"between", "MATCH (a)-[r]->(b) WHERE a.id = $start AND b.id = $end RETURN r", map[string]interface{}{"start": "node:start", "end": "node:end"}, func(e *RemoteEngine) ([]*Edge, error) { return e.GetEdgesBetween("node:start", "node:end") }},
		{"type escaped", "MATCH ()-[r:`REL``TYPE`]->() RETURN r", nil, func(e *RemoteEngine) ([]*Edge, error) { return e.GetEdgesByType(edgeType) }},
		{"all", "MATCH ()-[r]->() RETURN r", nil, func(e *RemoteEngine) ([]*Edge, error) { return e.AllEdges() }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			for _, failure := range []bool{false, true} {
				engine := &RemoteEngine{transport: &fakeRemoteTransport{queryFn: func(_ context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
					require.Equal(t, operation.statement, statement)
					require.Equal(t, operation.params, params)
					if failure {
						return nil, nil, transportErr
					}
					return []string{"r"}, [][]interface{}{{}, {wire}, {}}, nil
				}}}
				edges, err := operation.run(engine)
				if failure {
					require.Same(t, transportErr, err)
					require.Nil(t, edges)
					continue
				}
				require.NoError(t, err)
				require.Len(t, edges, 1, "empty rows must not become entities")
				require.Equal(t, EdgeID("edge:one"), edges[0].ID)
				require.Equal(t, NodeID("node:start"), edges[0].StartNode)
				require.Equal(t, NodeID("node:end"), edges[0].EndNode)
				require.Equal(t, edgeType, edges[0].Type)
				require.Equal(t, props, edges[0].Properties)
			}
		})
	}
}

func TestRemotePublicCountAndDegreeBoundaries(t *testing.T) {
	transportErr := errors.New("remote scalar failure")
	for _, operation := range []struct {
		name, statement string
		params          map[string]interface{}
		suppressesError bool
		run             func(*RemoteEngine) (int64, error)
	}{
		{"nodes", "MATCH (n) RETURN count(n)", nil, false, (*RemoteEngine).NodeCount},
		{"edges", "MATCH ()-[r]->() RETURN count(r)", nil, false, (*RemoteEngine).EdgeCount},
		{"edge type", "MATCH ()-[r:`T``ype`]->() RETURN count(r)", nil, false, func(e *RemoteEngine) (int64, error) { return e.EdgeCountByType("T`ype") }},
		{"start label", "MATCH (s:`L``abel`)-[r:`T``ype`]->() RETURN count(r)", nil, false, func(e *RemoteEngine) (int64, error) { return e.EdgeCountByStartLabel("L`abel", "T`ype") }},
		{"end label", "MATCH ()-[r:`T``ype`]->(e:`L``abel`) RETURN count(r)", nil, false, func(e *RemoteEngine) (int64, error) { return e.EdgeCountByEndLabel("L`abel", "T`ype") }},
		{"in degree", "MATCH ()-[r]->(n) WHERE n.id = $id RETURN count(r)", map[string]interface{}{"id": "node:one"}, true, func(e *RemoteEngine) (int64, error) { return int64(e.GetInDegree("node:one")), nil }},
		{"out degree", "MATCH (n)-[r]->() WHERE n.id = $id RETURN count(r)", map[string]interface{}{"id": "node:one"}, true, func(e *RemoteEngine) (int64, error) { return int64(e.GetOutDegree("node:one")), nil }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			for _, response := range []struct {
				name string
				rows [][]interface{}
				err  error
				want int64
			}{
				{"no rows", nil, nil, 0},
				{"empty row", [][]interface{}{{}}, nil, 0},
				{"integer", [][]interface{}{{int64(7)}}, nil, 7},
				{"float", [][]interface{}{{float64(6.8)}}, nil, 6},
				{"transport error", nil, transportErr, 0},
			} {
				t.Run(response.name, func(t *testing.T) {
					calls := 0
					engine := &RemoteEngine{transport: &fakeRemoteTransport{queryFn: func(_ context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
						calls++
						require.Equal(t, operation.statement, statement)
						require.Equal(t, operation.params, params)
						return nil, response.rows, response.err
					}}}
					count, err := operation.run(engine)
					require.Equal(t, response.want, count)
					if response.err != nil && !operation.suppressesError {
						require.Same(t, transportErr, err)
					} else {
						require.NoError(t, err)
					}
					require.Equal(t, 1, calls)
				})
			}
		})
	}
}

func TestRemotePublicBatchGetNodesSharesDeadline(t *testing.T) {
	var shared context.Context
	var deadline time.Time
	var seen []string
	engine := &RemoteEngine{transport: &fakeRemoteTransport{queryFn: func(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
		require.Equal(t, "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1", statement)
		id := params["id"].(string)
		require.Equal(t, map[string]interface{}{"id": id}, params)
		seen = append(seen, id)
		currentDeadline, ok := ctx.Deadline()
		require.True(t, ok)
		if shared == nil {
			shared, deadline = ctx, currentDeadline
		} else {
			require.Same(t, shared, ctx)
			require.Equal(t, deadline, currentDeadline)
		}
		require.NoError(t, ctx.Err())
		if id == "missing" {
			return nil, [][]interface{}{{}}, nil
		}
		return []string{"n"}, [][]interface{}{{map[string]interface{}{"id": id, "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"name": id, "rank": int64(2)}}}}, nil
	}}}
	nodes, err := engine.BatchGetNodes([]NodeID{"node:first", "missing", "node:last"})
	require.NoError(t, err)
	require.Equal(t, []string{"node:first", "missing", "node:last"}, seen)
	require.Len(t, nodes, 2)
	for _, id := range []NodeID{"node:first", "node:last"} {
		require.Equal(t, id, nodes[id].ID)
		require.Equal(t, []string{"Person"}, nodes[id].Labels)
		require.Equal(t, map[string]interface{}{"name": string(id), "rank": int64(2)}, nodes[id].Properties)
	}
	require.ErrorIs(t, shared.Err(), context.Canceled, "the shared batch context is released when the public call returns")
}

func TestRemotePublicReadCardinality(t *testing.T) {
	node := map[string]interface{}{"id": "node:one", "properties": map[string]interface{}{"value": int64(3)}}
	edge := map[string]interface{}{"id": "edge:one", "type": "LINK", "startNodeElementId": "node:a", "endNodeElementId": "node:b"}
	firstNode := func(read func(*RemoteEngine) (*Node, error)) func(*RemoteEngine) ([]string, error) {
		return func(engine *RemoteEngine) ([]string, error) {
			value, err := read(engine)
			if err != nil {
				return nil, err
			}
			return []string{string(value.ID)}, nil
		}
	}
	for _, operation := range []struct {
		name string
		one  bool
		wire interface{}
		id   string
		run  func(*RemoteEngine) ([]string, error)
	}{
		{"node", true, node, "node:one", firstNode(func(engine *RemoteEngine) (*Node, error) { return engine.GetNode("node:one") })},
		{"first label", true, node, "node:one", firstNode(func(engine *RemoteEngine) (*Node, error) { return engine.GetFirstNodeByLabel("Entity") })},
		{"edge", true, edge, "edge:one", func(engine *RemoteEngine) ([]string, error) {
			value, err := engine.GetEdge("edge:one")
			if err != nil {
				return nil, err
			}
			return []string{string(value.ID)}, nil
		}},
		{"all nodes", false, node, "node:one", func(engine *RemoteEngine) ([]string, error) {
			values, err := engine.AllNodes()
			if err != nil {
				return nil, err
			}
			ids := make([]string, 0, len(values))
			for _, value := range values {
				ids = append(ids, string(value.ID))
			}
			return ids, nil
		}},
		{"all edges", false, edge, "edge:one", func(engine *RemoteEngine) ([]string, error) {
			values, err := engine.AllEdges()
			if err != nil {
				return nil, err
			}
			ids := make([]string, 0, len(values))
			for _, value := range values {
				ids = append(ids, string(value.ID))
			}
			return ids, nil
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			transportErr := errors.New("read failed")
			for _, response := range []struct {
				name string
				rows [][]interface{}
				err  error
			}{
				{"no rows", nil, nil},
				{"empty first row", [][]interface{}{{}, {operation.wire}}, nil},
				{"valid before malformed", [][]interface{}{{operation.wire}, {42}}, nil},
				{"malformed before valid", [][]interface{}{{42}, {operation.wire}}, nil},
				{"transport failure", [][]interface{}{{operation.wire}}, transportErr},
			} {
				t.Run(response.name, func(t *testing.T) {
					calls := 0
					engine := &RemoteEngine{transport: &fakeRemoteTransport{queryModeFn: func(ctx context.Context, _ string, _ map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error) {
						calls++
						require.Equal(t, RemoteAccessRead, mode)
						_, bounded := ctx.Deadline()
						require.True(t, bounded)
						return nil, response.rows, response.err
					}}}
					ids, err := operation.run(engine)
					require.Equal(t, 1, calls)
					switch {
					case response.err != nil:
						require.Same(t, transportErr, err)
						require.Nil(t, ids)
					case operation.one && (response.name == "no rows" || response.name == "empty first row"):
						require.ErrorIs(t, err, ErrNotFound)
						require.Nil(t, ids)
					case response.name == "malformed before valid" || (!operation.one && response.name == "valid before malformed"):
						require.Error(t, err)
						require.Nil(t, ids, "list admission must discard partial decoded output")
					case response.name == "no rows":
						require.NoError(t, err)
						require.Equal(t, []string{}, ids)
					default:
						require.NoError(t, err)
						require.Equal(t, []string{operation.id}, ids)
					}
				})
			}
		})
	}
}
