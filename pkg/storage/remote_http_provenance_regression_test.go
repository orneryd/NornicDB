package storage

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteHTTPEmptyMapDoesNotStealEntityProvenance(t *testing.T) {
	// Neo4j's flattened row metadata has no entry for an empty map. REST values
	// preserve column/container positions even when both property maps are empty.
	for _, row := range []struct {
		name, values, rest string
		entityColumn       int
	}{
		{"ordinary before node", `[{},{}]`, `[{}, {"self":"http://remote/db/data/node/1","metadata":{"id":1,"elementId":"4:db:n","labels":["Doc"]},"data":{}}]`, 1},
		{"node before ordinary", `[{},{}]`, `[{"self":"http://remote/db/data/node/1","metadata":{"id":1,"elementId":"4:db:n","labels":["Doc"]},"data":{}},{}]`, 0},
		{"nested ordinary before node", `[[{}],{}]`, `[[{}],{"self":"http://remote/db/data/node/1","metadata":{"id":1,"elementId":"4:db:n","labels":["Doc"]},"data":{}}]`, 1},
	} {
		t.Run(row.name, func(t *testing.T) {
			engine := makeEngineWithTransport(t, "http://remote.example", func(*http.Request) (*http.Response, error) {
				response := `{"results":[{"columns":["a","b"],"data":[{"row":` + row.values + `,"rest":` + row.rest + `,"meta":[{"elementId":"4:db:n","type":"node"}],"graph":{"nodes":[{"id":"1","elementId":"4:db:n","labels":["Doc"],"properties":{}}],"relationships":[]}}]}],"errors":[]}`
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
			})
			_, rows, err := engine.QueryCypher(context.Background(), "RETURN ordinary, entity", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.IsType(t, RemoteEntityValue{}, rows[0][row.entityColumn])
			_, ordinaryMarked := rows[0][1-row.entityColumn].(RemoteEntityValue)
			require.False(t, ordinaryMarked)
		})
	}
}

// Recorded from Neo4j5.26.30 in an isolated transaction, then rolled back and
// verified absent. This pins the actual row/meta/REST shape, including paths.
func TestRemoteHTTPRecordedEmptyEntityPositions(t *testing.T) {
	fixture, err := os.ReadFile("testdata/remote_http_empty_entities.json")
	require.NoError(t, err)
	engine := makeEngineWithTransport(t, "http://remote.example", func(request *http.Request) (*http.Response, error) {
		var submitted remoteTxRequest
		require.NoError(t, json.NewDecoder(request.Body).Decode(&submitted))
		require.Contains(t, submitted.Statements[0].ResultDataContents, "rest")
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(fixture)))}, nil
	})
	_, rows, err := engine.QueryCypher(context.Background(), "RETURN recorded_fixture", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	row := rows[0]
	require.Equal(t, map[string]interface{}{}, row[0])
	require.IsType(t, RemoteEntityValue{}, row[1])
	require.Equal(t, []interface{}{map[string]interface{}{}}, row[2])
	require.IsType(t, RemoteEntityValue{}, row[3])
	wrapped := row[4].(map[string]interface{})["items"].([]interface{})
	require.Equal(t, map[string]interface{}{}, wrapped[0])
	require.IsType(t, RemoteEntityValue{}, wrapped[1])
	require.IsType(t, RemoteEntityValue{}, wrapped[2])
	path := row[5].(RemotePathValue)
	require.Len(t, path, 3)
	for _, entity := range path {
		require.IsType(t, RemoteEntityValue{}, entity)
	}
}

func TestRemoteHTTPRESTEnvelopeRequiresDistinctSerializerMetadata(t *testing.T) {
	metadata := map[string]interface{}{"elementId": "node"}
	envelope := map[string]interface{}{"metadata": metadata, "self": "http://remote/node/1", "data": map[string]interface{}{}}
	require.True(t, remoteHTTPRESTEntity(asRemoteRESTRaw(t, envelope), map[string]interface{}{}, metadata))
	require.False(t, remoteHTTPRESTEntity(asRemoteRESTRaw(t, envelope), envelope, metadata), "ordinary map with these keys has identical REST data")
	require.False(t, remoteHTTPRESTEntity(asRemoteRESTRaw(t, "ordinary"), nil, metadata))
	require.False(t, remoteHTTPRESTEntity(asRemoteRESTRaw(t, map[string]interface{}{}), nil, metadata))
	require.False(t, remoteHTTPRESTEntity(asRemoteRESTRaw(t, envelope), nil, map[string]interface{}{"elementId": "different"}))
	require.False(t, remoteHTTPRESTEntity(asRemoteRESTRaw(t, envelope), nil, metadata))
	require.False(t, remoteHTTPRESTPath(asRemoteRESTRaw(t, "ordinary"), nil))
	require.False(t, remoteHTTPRESTPath(asRemoteRESTRaw(t, map[string]interface{}{}), nil))
	path := map[string]interface{}{"nodes": []interface{}{"node"}, "relationships": []interface{}{}, "length": float64(0)}
	require.True(t, remoteHTTPRESTPath(asRemoteRESTRaw(t, path), []interface{}{nil}))
	require.False(t, remoteHTTPRESTPath(asRemoteRESTRaw(t, path), path))
}

func asRemoteRESTRaw(t *testing.T, value interface{}) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return raw
}

func TestRemoteHTTPRejectsInconsistentRESTSidecars(t *testing.T) {
	entities := map[string]interface{}{"node": map[string]interface{}{"properties": map[string]interface{}{}}}
	for _, tc := range []struct{ name, row, rest string }{
		{"invalid REST JSON", `1`, `{`},
		{"map represented as list", `{"key":1}`, `[]`},
		{"list represented as map", `[1]`, `{}`},
		{"list length mismatch", `[1]`, `[]`},
		{"missing map key", `{"key":1}`, `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := 0
			_, err := decodeRemoteHTTPValueWithREST(json.RawMessage(tc.row), json.RawMessage(tc.rest), []interface{}{nil}, &index, entities)
			require.Error(t, err)
		})
	}
	require.False(t, remoteHTTPRESTEntity(json.RawMessage(`{"metadata":{"elementId":"node"},"self":"url","data":{}}`), map[string]interface{}{}, map[string]interface{}{"elementId": float64(1)}))
}

func TestRemoteHTTPRejectsInconsistentTopLevelREST(t *testing.T) {
	for _, rest := range []string{`[]`, `[{}]`, `[{},{},{}]`} {
		t.Run(rest, func(t *testing.T) {
			engine := makeEngineWithTransport(t, "http://remote.example", func(*http.Request) (*http.Response, error) {
				response := `{"results":[{"columns":["a","b"],"data":[{"row":[{},{}],"rest":` + rest + `,"meta":[{"elementId":"n","type":"node"}],"graph":{"nodes":[{"id":"1","elementId":"n","labels":[],"properties":{}}],"relationships":[]}}]}],"errors":[]}`
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
			})
			_, _, err := engine.QueryCypher(context.Background(), "RETURN ordinary,entity", nil)
			require.Error(t, err)
			var localized *localization.LocalizedError
			require.True(t, errors.As(err, &localized))
			require.Equal(t, localization.MessageStorageRemoteRESTRowLengthMismatch, localized.Message.ID)
		})
	}
}
