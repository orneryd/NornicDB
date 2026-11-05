package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMarkedRemoteGraphValuesProduceHTTPGraphMetadata(t *testing.T) {
	s := &Server{}
	g := &GraphResult{}
	state := &transactionHTTPValueState{graph: g}
	n := storage.RemoteEntityValue{"elementId": "4:source:n", "labels": []interface{}{"Doc"}, "properties": map[string]interface{}{"name": "Remote"}}
	r := storage.RemoteEntityValue{"elementId": "5:source:e", "type": "LINKS", "startNodeElementId": "4:source:n", "endNodeElementId": "4:source:m", "properties": map[string]interface{}{}}
	for _, v := range []storage.RemoteEntityValue{n, r} {
		row, meta, projectionErr1 := s.transactionHTTPValue(t.Context(), v, "local", state)
		require.NoError(t, projectionErr1)
		require.Equal(t, v["properties"], row)
		require.Len(t, meta, 1)
		require.Equal(t, v["elementId"], meta[0].(map[string]interface{})["elementId"])
	}
	require.Len(t, g.Nodes, 1)
	require.Equal(t, "4:source:n", g.Nodes[0].ElementID)
	require.Equal(t, []string{"Doc"}, g.Nodes[0].Labels)
	require.Len(t, g.Relationships, 1)
	require.Equal(t, "4:source:n", g.Relationships[0].StartNode)
	require.Equal(t, "4:source:m", g.Relationships[0].EndNode)
}

func TestHTTPRemoteSourcePublicMapSerialization(t *testing.T) {
	fixture, err := os.ReadFile("../storage/testdata/remote_http_empty_entities.json")
	require.NoError(t, err)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(fixture) }))
	defer source.Close()
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: source.URL, Database: "neo4j"})
	require.NoError(t, err)
	defer remote.Close()
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN recorded", nil, storage.RemoteAccessRead)
	require.NoError(t, err)
	node := rows[0][1].(storage.RemoteEntityValue)
	s := &Server{}
	graph := &GraphResult{}
	_, meta, projectionErr2 := s.transactionHTTPValue(t.Context(), node, "local", &transactionHTTPValueState{graph: graph})
	require.NoError(t, projectionErr2)
	require.Equal(t, int64(95), meta[0].(map[string]interface{})["id"])
	require.Len(t, graph.Nodes, 1)
	for _, value := range []interface{}{map[string]interface{}(node), map[string]interface{}{"nested": map[string]interface{}(node)}} {
		ordinaryGraph := &GraphResult{}
		row, _, projectionErr3 := s.transactionHTTPValue(t.Context(), value, "local", &transactionHTTPValueState{graph: ordinaryGraph})
		require.NoError(t, projectionErr3)
		encoded, err := json.Marshal(row)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "remoteIdentity")
		require.Empty(t, ordinaryGraph.Nodes)
		require.Empty(t, ordinaryGraph.Relationships)
	}
}
