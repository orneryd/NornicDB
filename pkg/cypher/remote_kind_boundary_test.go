package cypher

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRemoteGraphValuesUseSharedKindAndListBoundary(t *testing.T) {
	node := storage.RemoteEntityValue{"elementId": "n", "labels": []interface{}{}, "properties": map[string]interface{}{}}
	rel := storage.RemoteEntityValue{"elementId": "r", "type": "R", "startNodeElementId": "n", "endNodeElementId": "n", "properties": map[string]interface{}{}}
	path := storage.RemotePathValue{node, rel, node}
	for _, tc := range []struct {
		name     string
		value    interface{}
		kind     cypherValueKind
		rank     int
		typeName string
	}{
		{"node", node, valueKindNode, 1, "NODE"}, {"relationship", rel, valueKindRelationship, 2, "RELATIONSHIP"}, {"path", path, valueKindPath, 4, "PATH"},
		{"ordinary entity-shaped map", map[string]interface{}(node), valueKindMap, 0, "MAP"}, {"ordinary list", []interface{}{node}, valueKindList, 3, "LIST<NODE NOT NULL>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.kind, cypherValueKindOf(tc.value))
			require.Equal(t, tc.rank, cypherSortRank(tc.value))
			require.Equal(t, tc.typeName, valueTypeOf(tc.value).render(false))
		})
	}
	for _, name := range []string{"head", "last", "reverse"} {
		t.Run(name, func(t *testing.T) {
			_, err := evaluateListAccessFunction(name, path)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Path")
		})
	}
	_, ok := cypherSortList(path)
	require.False(t, ok)
	_, ok = cypherListValue(path)
	require.False(t, ok)
}

func TestRemoteEntityOrderingUsesIdentity(t *testing.T) {
	first := storage.RemoteEntityValue{"elementId": "a", "labels": []interface{}{}, "properties": map[string]interface{}{"score": int64(99)}}
	same := storage.RemoteEntityValue{"elementId": "a", "labels": []interface{}{}, "properties": map[string]interface{}{"score": int64(1)}}
	require.Zero(t, compareValuesForSort(first, same), "entity properties must not change identity ordering")
}

func TestRemoteEntityOrderingUsesSourceNumericIDs(t *testing.T) {
	ids := []string{"9", "2", "2"}
	elements := []string{"a", "z", "y"}
	row, rest, meta, nodes := []interface{}{}, []interface{}{}, []interface{}{}, []interface{}{}
	for i, id := range ids {
		properties := map[string]interface{}{}
		row = append(row, properties)
		metadata := map[string]interface{}{"id": id, "elementId": elements[i], "labels": []string{}}
		rest = append(rest, map[string]interface{}{"self": "/node/" + id, "metadata": metadata, "data": properties})
		meta = append(meta, map[string]interface{}{"elementId": elements[i], "type": "node"})
		nodes = append(nodes, map[string]interface{}{"id": id, "elementId": elements[i], "labels": []string{}, "properties": properties})
	}
	response := map[string]interface{}{"results": []interface{}{map[string]interface{}{"columns": []string{"a", "b", "c"}, "data": []interface{}{map[string]interface{}{"row": row, "rest": rest, "meta": meta, "graph": map[string]interface{}{"nodes": nodes, "relationships": []interface{}{}}}}}}, "errors": []interface{}{}}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { require.NoError(t, json.NewEncoder(w).Encode(response)) }))
	defer peer.Close()
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: peer.URL, Database: "neo4j"})
	require.NoError(t, err)
	defer remote.Close()
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN a,b,c", nil)
	require.NoError(t, err)
	require.Positive(t, compareValuesForSort(rows[0][0], rows[0][1]), "source numeric IDs take precedence over element-ID spelling")
	require.Negative(t, compareValuesForSort(rows[0][1], rows[0][0]))
	require.Positive(t, compareValuesForSort(rows[0][1], rows[0][2]), "different source elements with equal legacy ID retain stable identity ordering")
	unknown := storage.RemoteEntityValue{"elementId": "m", "labels": []interface{}{}}
	require.Negative(t, compareValuesForSort(unknown, rows[0][1]))
	for _, pair := range [][2]interface{}{{unknown, map[string]interface{}{}}, {map[string]interface{}{}, unknown}, {unknown, storage.RemoteEntityValue{"elementId": "r", "type": "R"}}} {
		_, comparable, handled := compareCypherStructuredValues(pair[0], pair[1], compareSortableValues)
		require.False(t, comparable)
		require.True(t, handled)
	}
}
