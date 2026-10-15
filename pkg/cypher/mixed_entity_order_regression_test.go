package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func mixedEntityOrderRemoteValues(t *testing.T, fixture string) []interface{} {
	t.Helper()
	wire, err := os.ReadFile("../storage/testdata/" + fixture)
	require.NoError(t, err)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(wire)
	}))
	t.Cleanup(peer.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: peer.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN graph_values", nil)
	require.NoError(t, err)
	return rows[0]
}

func mixedEntityOrderRemote(t *testing.T) storage.RemoteEntityValue {
	t.Helper()
	return mixedEntityOrderRemoteValues(t, "remote_entity_response.json")[0].(storage.RemoteEntityValue)
}

func TestMixedEntityOrderAllPublicPermutations(t *testing.T) {
	exec, store := newTestExecutor(t)
	a := &storage.Node{ID: "a", Properties: map[string]interface{}{"name": "a"}}
	c := &storage.Node{ID: "c", Properties: map[string]interface{}{"name": "c"}}
	_, err := store.CreateNode(a)
	require.NoError(t, err)
	_, err = store.CreateNode(c)
	require.NoError(t, err)
	a, err = store.GetNode("a")
	require.NoError(t, err)
	c, err = store.GetNode("c")
	require.NoError(t, err)
	remote := mixedEntityOrderRemote(t)
	for _, values := range [][]interface{}{{c, remote, a}, {c, a, remote}, {remote, c, a}, {remote, a, c}, {a, c, remote}, {a, remote, c}} {
		result, err := exec.Execute(context.Background(), "UNWIND $values AS v RETURN v.name AS name ORDER BY v", map[string]interface{}{"values": values})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"a"}, {"c"}, {"Remote"}}, result.Rows)
	}
}

func TestMixedEntityOrderLaws(t *testing.T) {
	remote := mixedEntityOrderRemote(t)
	for _, values := range [][]interface{}{
		{(*storage.Node)(nil), &storage.Node{ID: "a"}, &storage.Node{ID: "c"}, &storage.Node{ID: "a", Properties: map[string]interface{}{"version": 2}}, remote},
		{(*storage.Edge)(nil), &storage.Edge{ID: "a"}, &storage.Edge{ID: "c"}, storage.RemoteEntityValue{"elementId": "r", "type": "R"}},
	} {
		assertMixedEntityOrderLaws(t, values)
	}
}
func signEntityOrder(value int) int {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
}

// Decode numeric identities through the real HTTP metadata owner, rather than
// constructing its sealed internal transport metadata in a Cypher test.
func mixedEntityOrderKnownRemote(t *testing.T, id int, element string) storage.RemoteEntityValue {
	t.Helper()
	metadata := map[string]interface{}{"id": fmt.Sprint(id), "elementId": element, "labels": []string{}}
	properties := map[string]interface{}{"name": element}
	response := map[string]interface{}{"results": []interface{}{map[string]interface{}{"columns": []string{"n"}, "data": []interface{}{map[string]interface{}{"row": []interface{}{properties}, "rest": []interface{}{map[string]interface{}{"self": "/node/" + fmt.Sprint(id), "metadata": metadata, "data": properties}}, "meta": []interface{}{map[string]interface{}{"elementId": element, "type": "node"}}, "graph": map[string]interface{}{"nodes": []interface{}{map[string]interface{}{"id": fmt.Sprint(id), "elementId": element, "labels": []string{}, "properties": properties}}, "relationships": []interface{}{}}}}}}, "errors": []interface{}{}}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { require.NoError(t, json.NewEncoder(w).Encode(response)) }))
	t.Cleanup(peer.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: peer.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN n", nil)
	require.NoError(t, err)
	node := rows[0][0].(storage.RemoteEntityValue)
	legacy, _, _ := node.LegacyIDs()
	require.Equal(t, int64(id), legacy)
	return node
}

func TestRemoteIdentityAvailabilityOrderLaws(t *testing.T) {
	knownA := mixedEntityOrderKnownRemote(t, 9, "a")
	knownZ := mixedEntityOrderKnownRemote(t, 1, "z")
	contradictory := mixedEntityOrderKnownRemote(t, 10, "a")
	unknown := storage.RemoteEntityValue{"elementId": "m", "labels": []string{}}
	sameElementUnknown := storage.RemoteEntityValue{"elementId": "a", "labels": []string{}}
	values := []interface{}{knownA, knownZ, contradictory, unknown, sameElementUnknown}
	assertMixedEntityOrderLaws(t, values)
}

func TestMixedEntityOrderNestedAndAggregateConsumers(t *testing.T) {
	exec, _ := newTestExecutor(t)
	a, c := &storage.Node{ID: "a", Properties: map[string]interface{}{"name": "a"}}, &storage.Node{ID: "c", Properties: map[string]interface{}{"name": "c"}}
	remote := mixedEntityOrderRemote(t)
	for _, query := range []string{
		"UNWIND $values AS v RETURN v.name AS name ORDER BY [v]",
		"UNWIND $values AS v RETURN v.name AS name ORDER BY {entity:v}",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), query, map[string]interface{}{"values": []interface{}{c, remote, a}})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"a"}, {"c"}, {"Remote"}}, result.Rows)
		})
	}
	t.Run("aggregates", func(t *testing.T) {
		result, err := exec.Execute(context.Background(), "UNWIND $values AS v RETURN min(v).name AS lo, max(v).name AS hi", map[string]interface{}{"values": []interface{}{c, remote, a}})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"a", "Remote"}}, result.Rows)
	})
	t.Run("paths", func(t *testing.T) {
		values := []interface{}{&PathResult{Nodes: []*storage.Node{c}}, storage.RemotePathValue{remote}, &PathResult{Nodes: []*storage.Node{a}}}
		result, err := exec.Execute(context.Background(), "UNWIND $values AS v RETURN v AS path ORDER BY v", map[string]interface{}{"values": values})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{values[2]}, {values[0]}, {values[1]}}, result.Rows)
	})
}

func TestRemoteSnapshotEqualityIgnoresProperties(t *testing.T) {
	exec, _ := newTestExecutor(t)
	node := mixedEntityOrderRemote(t)
	changed := make(storage.RemoteEntityValue, len(node))
	for k, v := range node {
		changed[k] = v
	}
	changed["properties"] = map[string]interface{}{"name": "new snapshot"}
	for _, pair := range [][2]interface{}{{node, changed}, {storage.RemotePathValue{node}, storage.RemotePathValue{changed}}} {
		result, err := exec.Execute(context.Background(), "RETURN $a = $b AS equal", map[string]interface{}{"a": pair[0], "b": pair[1]})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{true}}, result.Rows)
	}
}

func assertMixedEntityOrderLaws(t *testing.T, values []interface{}) {
	t.Helper()
	for i, a := range values {
		for j, b := range values {
			ab, ba := compareValuesForSort(a, b), compareValuesForSort(b, a)
			require.Equal(t, signEntityOrder(ab), -signEntityOrder(ba), "antisymmetry %d/%d", i, j)
			for k, c := range values {
				bc, ac := compareValuesForSort(b, c), compareValuesForSort(a, c)
				if ab <= 0 && bc <= 0 {
					require.LessOrEqual(t, ac, 0, "transitivity %d/%d/%d", i, j, k)
				}
				if ab == 0 {
					require.Equal(t, signEntityOrder(ac), signEntityOrder(bc), "equivalence congruence %d/%d/%d", i, j, k)
				}
			}
		}
	}
}

func TestMixedPathOrderNilCarrierLaws(t *testing.T) {
	a, c := &storage.Node{ID: "a"}, &storage.Node{ID: "c"}
	remote := mixedEntityOrderRemote(t)
	assertMixedEntityOrderLaws(t, []interface{}{(*PathResult)(nil), &PathResult{Nodes: []*storage.Node{a}}, &PathResult{Nodes: []*storage.Node{c}}, storage.RemotePathValue{}, storage.RemotePathValue{remote}})
}

func TestMixedRelationshipOrderAllPublicPermutations(t *testing.T) {
	exec, _ := newTestExecutor(t)
	a, c := &storage.Edge{ID: "a"}, &storage.Edge{ID: "c"}
	remote := mixedEntityOrderRemoteValues(t, "remote_entity_response.json")[1].(storage.RemoteEntityValue)
	for _, values := range [][]interface{}{{c, remote, a}, {c, a, remote}, {remote, c, a}, {remote, a, c}, {a, c, remote}, {a, remote, c}} {
		result, err := exec.Execute(context.Background(), "UNWIND $values AS v RETURN elementId(v) AS id ORDER BY v", map[string]interface{}{"values": values})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"5:test:a"}, {"5:test:c"}, {remote["elementId"]}}, result.Rows)
	}
}

func TestRemoteSnapshotGroupingUsesIdentity(t *testing.T) {
	exec, _ := newTestExecutor(t)
	actualPath := mixedEntityOrderRemoteValues(t, "remote_http_empty_entities.json")[5].(storage.RemotePathValue)
	actualEntities := mixedEntityOrderRemoteValues(t, "remote_entity_response.json")
	for _, value := range []interface{}{actualEntities[0], actualEntities[1], actualPath} {
		var changed interface{}
		switch source := value.(type) {
		case storage.RemoteEntityValue:
			snapshot := make(storage.RemoteEntityValue, len(source))
			for k, v := range source {
				snapshot[k] = v
			}
			snapshot["properties"] = map[string]interface{}{"snapshot": 2}
			changed = snapshot
		case storage.RemotePathValue:
			snapshot := make(storage.RemotePathValue, len(source))
			for i, entity := range source {
				snapshot[i] = make(storage.RemoteEntityValue, len(entity))
				for k, v := range entity {
					snapshot[i][k] = v
				}
				snapshot[i]["properties"] = map[string]interface{}{"snapshot": 2}
			}
			changed = snapshot
		}
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			params := map[string]interface{}{"values": []interface{}{value, changed}, "a": value, "b": changed}
			result, err := exec.Execute(context.Background(), "RETURN $a = $b AS equal", params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{true}}, result.Rows)
			result, err = exec.Execute(context.Background(), "UNWIND $values AS v RETURN count(DISTINCT v) AS n", params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			result, err = exec.Execute(context.Background(), "UNWIND $values AS v RETURN v AS entity, count(*) AS n", params)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, int64(2), result.Rows[0][1])
		})
	}
}

func TestPathIdentityEqualityAndEquivalenceAcrossCarriers(t *testing.T) {
	a := &storage.Node{ID: "a", Properties: map[string]interface{}{"version": 1}}
	snapshot := &storage.Node{ID: "a", Properties: map[string]interface{}{"version": 2}}
	native := &PathResult{Nodes: []*storage.Node{a}}
	changed := PathResult{Nodes: []*storage.Node{snapshot}}
	wrapper := map[string]interface{}{"_pathResult": changed}
	for _, left := range []interface{}{native, changed, wrapper} {
		for _, right := range []interface{}{native, changed, wrapper} {
			require.Equal(t, true, cypherEquality(left, right))
			require.Equal(t, cypherEquivalenceKey(left), cypherEquivalenceKey(right))
		}
	}
	forged := map[string]interface{}{"_pathResult": "ordinary user property"}
	require.Equal(t, true, cypherEquality(forged, forged))
}

func TestOrdinaryMapPathKeyDoesNotConferGraphIdentity(t *testing.T) {
	exec, _ := newTestExecutor(t)
	native := &storage.Node{ID: "a", Properties: map[string]interface{}{"name": "node"}}
	genuine := &PathResult{Nodes: []*storage.Node{native}}
	for _, tc := range []struct {
		name  string
		value interface{}
		want  string
		same  interface{}
	}{
		{"ordinary map", map[string]interface{}{"name": "map"}, "MAP NOT NULL", true},
		{"forged boolean path key", map[string]interface{}{"_pathResult": true, "name": "map"}, "MAP NOT NULL", true},
		{"forged scalar path key", map[string]interface{}{"_pathResult": int64(1), "name": "map"}, "MAP NOT NULL", true},
		{"forged nil path key", map[string]interface{}{"_pathResult": nil}, "MAP NOT NULL", nil},
		{"typed path wrapper", map[string]interface{}{"_pathResult": genuine}, "PATH NOT NULL", true},
		{"typed path", genuine, "PATH NOT NULL", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), "RETURN valueType($v) AS kind, $v = $v AS same", map[string]interface{}{"v": tc.value})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want, tc.same}}, result.Rows)
		})
	}
	forged := map[string]interface{}{"_pathResult": int64(1), "name": "map"}
	result, err := exec.Execute(context.Background(), "UNWIND $values AS v RETURN v.name AS name ORDER BY v", map[string]interface{}{"values": []interface{}{native, forged}})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"map"}, {"node"}}, result.Rows)
}

func TestRemoteNilGraphCarriersKeepNullSortAndIdentity(t *testing.T) {
	for _, value := range []interface{}{storage.RemoteEntityValue(nil), storage.RemotePathValue(nil)} {
		require.Equal(t, valueKindNull, cypherValueKindOf(value))
		require.Nil(t, cypherEquality(value, value))
		require.Zero(t, compareValuesForSort(value, nil))
		require.Zero(t, compareValuesForSort(nil, value))
		require.Equal(t, cypherEquivalenceKey(nil), cypherEquivalenceKey(value))
	}
}

func TestRemoteNullGraphPredicatesStayUnknown(t *testing.T) {
	exec, _ := newTestExecutor(t)
	for _, value := range []interface{}{storage.RemoteEntityValue(nil), storage.RemotePathValue(nil)} {
		for _, operator := range []string{"=", "<>", "<", "<=", ">", ">="} {
			t.Run(fmt.Sprintf("%T/%s", value, operator), func(t *testing.T) {
				result, err := exec.Execute(context.Background(), "RETURN $v "+operator+" $v AS predicate", map[string]interface{}{"v": value})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{nil}}, result.Rows)
			})
		}
	}
}

func TestRemoteIdentityPredicatesIgnoreOptionalNumericMetadata(t *testing.T) {
	exec, _ := newTestExecutor(t)
	unknown := mixedEntityOrderRemote(t)
	known := mixedEntityOrderKnownRemote(t, 9, unknown["elementId"].(string))
	conflicting := mixedEntityOrderKnownRemote(t, 10, unknown["elementId"].(string))
	for _, pair := range [][2]interface{}{{known, unknown}, {unknown, known}, {known, conflicting}, {conflicting, known}} {
		result, err := exec.Execute(context.Background(), "RETURN $a = $b AS eq, $a < $b AS lt, $a <= $b AS le, $a > $b AS gt, $a >= $b AS ge", map[string]interface{}{"a": pair[0], "b": pair[1]})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{true, false, true, false, true}}, result.Rows)
	}
}

func TestEntityPredicateIdentityDomainsRemainPartial(t *testing.T) {
	exec, _ := newTestExecutor(t)
	unknown := mixedEntityOrderRemote(t)
	knownOther := mixedEntityOrderKnownRemote(t, 9, "other")
	native := &storage.Node{ID: "a"}
	for _, pair := range [][2]interface{}{{native, unknown}, {unknown, native}, {unknown, knownOther}, {knownOther, unknown}, {(*storage.Node)(nil), native}, {native, (*storage.Node)(nil)}, {(*storage.Edge)(nil), &storage.Edge{ID: "a"}}, {&storage.Edge{ID: "a"}, (*storage.Edge)(nil)}} {
		result, err := exec.Execute(context.Background(), "RETURN $a < $b AS lt, $a <= $b AS le, $a > $b AS gt, $a >= $b AS ge", map[string]interface{}{"a": pair[0], "b": pair[1]})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{nil, nil, nil, nil}}, result.Rows)
	}
}
