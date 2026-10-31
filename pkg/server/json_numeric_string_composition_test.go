// Author: #1665 implementation lane (Codex). Public combined numeric/string contract.
package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPNumericAndLosslessStringPropertyComposition(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	database := server.dbManager.DefaultDatabaseName()
	host := httptest.NewServer(server.buildRouter())
	t.Cleanup(host.Close)
	// Author: #1665 implementation lane (Codex). Homogeneous property lists
	// retain numeric kinds. Mixed INTEGER/FLOAT property lists deliberately
	// coerce to FLOAT in the retained Cypher property owner (#643).
	payload := `{"statements":[{"statement":"CREATE (n:NumericStringComposition) SET n += $props RETURN n.big, n.whole, n.s, n[$key], n.ints, n.floats, n.mixed","parameters":{"props":{"big":9007199254740993,"whole":2.0,"s":"\ud800","\ud800":"kept","ints":[9007199254740993,-9007199254740993],"floats":[2.0,3.0],"mixed":[1,2.0]},"key":"\ud800"}}]}`
	request, err := http.NewRequest(http.MethodPost, host.URL+"/db/"+database+"/tx/commit", strings.NewReader(payload))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := host.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var result struct {
		Results []struct {
			Data []struct {
				Row []json.RawMessage `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []any `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(raw, &result))
	require.Empty(t, result.Errors)
	require.Len(t, result.Results, 1)
	require.Len(t, result.Results[0].Data, 1)
	row := result.Results[0].Data[0].Row
	require.Len(t, row, 7)
	require.Equal(t, "9007199254740993", string(row[0]))
	require.Equal(t, "2.0", string(row[1]))
	require.Equal(t, `"\ud800"`, string(row[2]), "the property string retains its admitted UTF16 identity")
	require.Equal(t, `"kept"`, string(row[3]))
	require.Equal(t, `[9007199254740993,-9007199254740993]`, string(row[4]))
	require.Equal(t, `[2.0,3.0]`, string(row[5]))
	require.Equal(t, `[1.0,2.0]`, string(row[6]), "mixed numeric property lists follow the retained coercion contract")
	store, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	unit := string([]byte{0xed, 0xa0, 0x80})
	found := false
	for _, node := range nodes {
		for _, label := range node.Labels {
			if label != "NumericStringComposition" {
				continue
			}
			found = true
			require.Equal(t, int64(9007199254740993), node.Properties["big"])
			require.Equal(t, float64(2), node.Properties["whole"])
			require.Equal(t, unit, node.Properties["s"])
			require.Equal(t, "kept", node.Properties[unit])
			// Inspect leaves independently of the storage owner's homogeneous
			// slice representation: exact integer versus FLOAT is the contract.
			for key, want := range map[string][]any{
				"ints":   {int64(9007199254740993), int64(-9007199254740993)},
				"floats": {float64(2), float64(3)},
				"mixed":  {float64(1), float64(2)},
			} {
				stored := reflect.ValueOf(node.Properties[key])
				require.Equal(t, reflect.Slice, stored.Kind(), key)
				require.Equal(t, len(want), stored.Len(), key)
				for index, expected := range want {
					require.Equal(t, expected, stored.Index(index).Interface(), key)
				}
			}
		}
	}
	require.True(t, found, "the HTTP mutation reached stored properties")
}
