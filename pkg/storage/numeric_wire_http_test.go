package storage

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/stretchr/testify/require"
)

func TestNumericWireRemoteParameters(t *testing.T) {
	requests := make(chan json.Number, 1)
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Statements []struct {
				Parameters map[string]interface{} `json:"parameters"`
			} `json:"statements"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.UseNumber()
		if err := decoder.Decode(&body); err != nil {
			t.Error(err)
			return
		}
		requests <- body.Statements[0].Parameters["v"].(json.Number)
		_, _ = w.Write([]byte(`{"results":[{"columns":["v","u"],"data":[{"row":[2.0,18446744073709551615]}]}],"errors":[]}`))
	}))
	defer host.Close()
	remote, err := NewRemoteEngine(RemoteEngineConfig{URI: host.URL, Database: "test"})
	require.NoError(t, err)
	defer remote.Close()
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN $v", map[string]interface{}{"v": float64(2)}, RemoteAccessRead)
	require.NoError(t, err)
	require.Equal(t, float64(2), rows[0][0])
	require.Equal(t, uint64(math.MaxUint64), rows[0][1])
	require.Equal(t, json.Number("2.0"), <-requests)
}

func TestNumericWireNodeJSON(t *testing.T) {
	data, err := (&Node{ID: "a", Properties: map[string]interface{}{"v": float64(2), "nested": []interface{}{float64(3)}}}).MarshalNeo4jJSON()
	require.NoError(t, err)
	var node Neo4jNode
	require.NoError(t, valueconvert.PropertyJSON.Unmarshal(data, &node))
	require.Equal(t, float64(2), node.Properties["v"])
	require.Equal(t, []interface{}{float64(3)}, node.Properties["nested"])
}
