package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// The reference has no APOC plugin. Its HTTP JSON parameter reader supplies the
// INTEGER/FLOAT contract independently of NornicDB's decoder; Bolt checks exact
// integer equality without decoding numeric results through JSON.
func TestJSONIngestionNeo4jReference(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires the TrueNAS Neo4j reference")
	}
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	ctx := context.Background()
	t.Cleanup(func() { _ = driver.Close(ctx) })
	result, err := neo4j.ExecuteQuery(ctx, driver, "CALL dbms.components() YIELD versions RETURN versions[0]", nil, neo4j.EagerResultTransformer)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(result.Records[0].Values[0].(string), "5.26."))
	result, err = neo4j.ExecuteQuery(ctx, driver, "RETURN $equal = $floating, $adjacent = $floating", map[string]interface{}{"equal": int64(9007199254740992), "adjacent": int64(9007199254740993), "floating": float64(9007199254740992)}, neo4j.EagerResultTransformer)
	require.NoError(t, err)
	require.Equal(t, []interface{}{true, false}, result.Records[0].Values)
	const body = `{"statements":[{"statement":"RETURN $v = 9007199254740993, valueType($v), valueType($d), valueType($e), $nested[0] = -9007199254740993, valueType($nested[1])","parameters":{"v":9007199254740993,"d":2.0,"e":2e0,"nested":[-9007199254740993,1.25]}}]}`
	request, err := http.NewRequestWithContext(ctx, "POST", os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")+"/db/neo4j/tx/commit", bytes.NewBufferString(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	var decoded struct {
		Errors  []interface{} `json:"errors"`
		Results []struct {
			Data []struct {
				Row []interface{} `json:"row"`
			} `json:"data"`
		} `json:"results"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&decoded))
	require.Empty(t, decoded.Errors)
	require.Len(t, decoded.Results, 1)
	require.Len(t, decoded.Results[0].Data, 1)
	require.Equal(t, []interface{}{true, "INTEGER NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", true, "FLOAT NOT NULL"}, decoded.Results[0].Data[0].Row)
}
