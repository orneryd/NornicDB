package bolt

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestMarkedRemoteGraphValuesUseEntityStructures(t *testing.T) {
	node := storage.RemoteEntityValue{"elementId": "4:source:n", "labels": []interface{}{"Doc"}, "properties": map[string]interface{}{"name": "Remote"}}
	edge := storage.RemoteEntityValue{"elementId": "5:source:e", "type": "LINKS", "startNodeElementId": "4:source:n", "endNodeElementId": "4:source:m", "properties": map[string]interface{}{}}
	for _, tc := range []struct {
		name string
		v    storage.RemoteEntityValue
		tag  byte
	}{{"node", node, 0x4e}, {"relationship", edge, 0x52}} {
		t.Run(tc.name, func(t *testing.T) {
			for _, utc := range []bool{false, true} {
				for _, v5 := range []bool{false, true} {
					wire := encodeRecordValueInto(nil, tc.v, utc, v5, "local")
					require.GreaterOrEqual(t, len(wire), 2)
					require.Equal(t, tc.tag, wire[1])
					_, used, err := decodePackStreamValue(wire, 0)
					require.NoError(t, err)
					require.Equal(t, len(wire), used)
					if v5 {
						require.Contains(t, string(wire), tc.v["elementId"].(string))
					}
				}
			}
			wire := encodePackStreamValue(tc.v)
			require.Equal(t, tc.tag, wire[1])
		})
	}
}

func TestRemotePathWireDirectionsAndNulls(t *testing.T) {
	a := storage.RemoteEntityValue{"elementId": "a", "labels": []interface{}{}, "properties": map[string]interface{}{}}
	b := storage.RemoteEntityValue{"elementId": "b", "labels": []interface{}{}, "properties": map[string]interface{}{}}
	r := storage.RemoteEntityValue{"elementId": "r", "type": "R", "startNodeElementId": "a", "endNodeElementId": "b"}
	for _, utc := range []bool{false, true} {
		for _, v5 := range []bool{false, true} {
			for _, p := range []storage.RemotePathValue{nil, {}, {a, r}} {
				require.Equal(t, []byte{0xc0}, encodeRecordValueInto(nil, p, utc, v5, "local"))
			}
			for _, tc := range []struct {
				p        storage.RemotePathValue
				sequence []interface{}
			}{
				{storage.RemotePathValue{a}, []interface{}{}},
				{storage.RemotePathValue{a, r, b}, []interface{}{int64(1), int64(1)}},
				{storage.RemotePathValue{b, r, a}, []interface{}{int64(-1), int64(1)}},
				{storage.RemotePathValue{a, r, b, r, a}, []interface{}{int64(1), int64(1), int64(-1), int64(0)}},
			} {
				wire := encodeRecordValueInto(nil, tc.p, utc, v5, "local")
				require.Equal(t, byte(0x50), wire[1])
				value, used, err := decodePackStreamValue(wire, 0)
				require.NoError(t, err)
				require.Equal(t, len(wire), used)
				require.Equal(t, tc.sequence, value.(map[string]interface{})["sequence"])
			}
			var nilEntity storage.RemoteEntityValue
			require.Equal(t, []byte{0xc0}, encodeRecordValueInto(nil, nilEntity, utc, v5, "local"))
			bare := storage.RemoteEntityValue{"elementId": "empty", "labels": []interface{}{}}
			wire := encodeRecordValueInto(nil, bare, utc, v5, "local")
			require.Equal(t, byte(0x4e), wire[1])
		}
	}
}

func TestHTTPRemoteSourceIdentitySurvivesBoltEncoding(t *testing.T) {
	fixture, err := os.ReadFile("../storage/testdata/remote_http_empty_entities.json")
	require.NoError(t, err)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture)
	}))
	defer source.Close()
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: source.URL, Database: "neo4j"})
	require.NoError(t, err)
	defer remote.Close()
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN recorded", nil, storage.RemoteAccessRead)
	require.NoError(t, err)
	node := rows[0][1].(storage.RemoteEntityValue)
	edge := rows[0][3].(storage.RemoteEntityValue)
	path := rows[0][5].(storage.RemotePathValue)
	id, _, _ := node.LegacyIDs()
	require.Equal(t, int64(95), id)
	id, start, end := edge.LegacyIDs()
	require.Equal(t, int64(2), id)
	require.Equal(t, int64(95), start)
	require.Equal(t, int64(5), end)
	ordinary := map[string]interface{}(node)
	for _, utc := range []bool{false, true} {
		for _, v5 := range []bool{false, true} {
			for _, value := range []interface{}{ordinary, map[string]interface{}{"box": ordinary}} {
				wire := encodeRecordValueInto(nil, value, utc, v5, "local")
				decoded, _, err := decodePackStreamValue(wire, 0)
				require.NoError(t, err)
				require.IsType(t, map[string]interface{}{}, decoded)
				require.NotContains(t, string(wire), "remoteIdentity")
			}
		}
	}

	for _, utc := range []bool{false, true} {
		for _, v5 := range []bool{false, true} {
			for _, entity := range []storage.RemoteEntityValue{node, edge} {
				wire := encodeRecordValueInto(nil, entity, utc, v5, "local")
				value, _, err := decodePackStreamValue(wire, 0)
				require.NoError(t, err)
				decoded := value.(map[string]interface{})
				id, start, end := entity.LegacyIDs()
				require.Equal(t, id, decoded["id"])
				if _, relationship := entity["type"]; relationship {
					require.Equal(t, start, decoded["startNodeId"])
					require.Equal(t, end, decoded["endNodeId"])
				}
				if v5 {
					require.Contains(t, string(wire), entity["elementId"].(string))
					if _, relationship := entity["type"]; relationship {
						require.Contains(t, string(wire), entity["startNodeElementId"].(string))
						require.Contains(t, string(wire), entity["endNodeElementId"].(string))
					}
				}
			}
			wire := encodeRecordValueInto(nil, path, utc, v5, "local")
			decoded, _, err := decodePackStreamValue(wire, 0)
			require.NoError(t, err)
			require.Equal(t, "Path", decoded.(map[string]interface{})["_type"])
			wire = encodeRecordValueInto(nil, rows[0][0], utc, v5, "local")
			require.Equal(t, byte(0xa0), wire[0])
		}
	}
}
