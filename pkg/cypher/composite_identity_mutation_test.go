package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompositeIdentityPublicMutations(t *testing.T) {
	for _, operation := range []struct{ name, query string }{
		{"update node", "USE comp MATCH (n:P) WHERE n.db='two' SET n.v=99 RETURN count(*)"},
		{"update edge", "USE comp MATCH (n:P)-[r:R]->() WHERE n.db='two' SET r.value=99 RETURN count(*)"},
		{"delete node", "USE comp MATCH (n:P) WHERE n.db='two' DETACH DELETE n RETURN count(*)"},
		{"delete edge", "USE comp MATCH (n:P)-[r:R]->() WHERE n.db='two' DELETE r RETURN count(*)"},
	} {
		t.Run(operation.name, func(t *testing.T) {
			exec, manager := newCompositeIdentityExecutor(t, false)
			result, err := exec.Execute(context.Background(), operation.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			first, err := manager.GetStorage("one")
			require.NoError(t, err)
			second, err := manager.GetStorage("two")
			require.NoError(t, err)
			node, err := first.GetNode("same")
			require.NoError(t, err)
			require.Equal(t, int64(7), node.Properties["v"])
			edge, err := first.GetEdge("same-rel")
			require.NoError(t, err)
			require.NotContains(t, edge.Properties, "value")
			node, err = second.GetNode("same")
			if operation.name == "delete node" {
				require.ErrorIs(t, err, storage.ErrNotFound)
			} else {
				require.NoError(t, err)
				if operation.name == "update node" {
					require.Equal(t, int64(99), node.Properties["v"])
				}
			}
			edge, err = second.GetEdge("same-rel")
			if operation.name == "delete node" || operation.name == "delete edge" {
				require.ErrorIs(t, err, storage.ErrNotFound)
			} else {
				require.NoError(t, err)
				if operation.name == "update edge" {
					require.Equal(t, int64(99), edge.Properties["value"])
				}
			}
		})
	}
}

func TestCompositeIdentityPublicMatchSources(t *testing.T) {
	exec, _ := newCompositeIdentityExecutor(t, false)
	for _, query := range []string{
		"USE comp MATCH (n:P)-[r:R]->(m) RETURN count(*)",
		"USE comp MATCH (n:P)<-[r:R]-(m) RETURN count(*)",
		"USE comp MATCH (n:P)-[r:R]-(m) RETURN count(*)",
		"USE comp MATCH (n:P)-[r:R]->(m) MATCH (n)-[r]->(m) RETURN count(*)",
		"USE comp MATCH (n:P)-[r:R*1]->(m:Q) MATCH (n)-[r*1]->(m) RETURN count(*)",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			expected := int64(2)
			if query == "USE comp MATCH (n:P)<-[r:R]-(m) RETURN count(*)" {
				expected = 0
			}
			require.Equal(t, [][]interface{}{{expected}}, result.Rows)
		})
	}
}

func TestCompositeIdentityPublicSelectedPaths(t *testing.T) {
	exec, _ := newCompositeIdentityExecutor(t, false)
	projection := " RETURN n.db, elementId(n), elementId(r), elementId(m) ORDER BY n.db"
	one := []interface{}{"one", "4:one:same", "5:one:same-rel", "4:one:target"}
	two := []interface{}{"two", "4:two:same", "5:two:same-rel", "4:two:target"}
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"unbound", "CYPHER 25 USE comp MATCH p=ANY SHORTEST (n:P)-[r:R]->(m:Q)", [][]interface{}{one, two}},
		{"native predicates", "CYPHER 25 USE comp MATCH p=ANY SHORTEST (n:P WHERE elementId(n)='4:two:same')-[r:R WHERE elementId(r)='5:two:same-rel']->(m:Q)", [][]interface{}{two}},
		{"bound endpoints", "CYPHER 25 USE comp MATCH (n:P {db:'two'}),(m:Q) WHERE elementId(m)='4:two:target' MATCH p=ANY SHORTEST (n)-[r:R]->(m)", [][]interface{}{two}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), tc.query+projection, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Rows)
		})
	}
}
