package server

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// A partially pulled result keeps its admitted image while a write invalidates
// cache membership. New HTTP reads see the write; the open Bolt result does not.
func TestCachedTransportDeliverySurvivesWriteDuringPull(t *testing.T) {
	for _, parser := range []struct {
		name      string
		configure func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.configure())

			s, users := setupTestServer(t)
			authorization := "Bearer " + getAuthToken(t, users, "admin")
			post := func(query string) TransactionResponse {
				t.Helper()
				response := makeRequest(t, s, "POST", "/db/nornic/tx/commit", map[string]any{
					"statements": []map[string]any{{"statement": query}},
				}, authorization)
				var decoded TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
				require.Equal(t, 200, response.Code)
				require.Empty(t, decoded.Errors)
				return decoded
			}
			post("UNWIND range(0,2) AS i CREATE (:TransportWire {i: i, name: 'original'})")
			const query = "MATCH (n:TransportWire) RETURN n.i AS `item number`, {b:n.i,a:n.name} AS value, n ORDER BY n.i"
			for range 2 {
				response := post(query)
				require.Len(t, response.Results, 1)
				require.Equal(t, []string{"item number", "value", "n"}, response.Results[0].Columns)
				require.Len(t, response.Results[0].Data, 3)
				require.Equal(t, "original", response.Results[0].Data[0].Row[1].(map[string]any)["a"])
			}

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, s.GetDatabaseManager())
			finished := make(chan error, 1)
			go func() { finished <- server.Serve(listener) }()
			t.Cleanup(func() { require.NoError(t, server.Close()); <-finished })
			driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, driver.Close(context.Background())) })
			ctx := t.Context()
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic", FetchSize: 1})
			t.Cleanup(func() { require.NoError(t, session.Close(context.Background())) })
			for range 2 {
				result, err := session.Run(ctx, query, nil)
				require.NoError(t, err)
				records, err := result.Collect(ctx)
				require.NoError(t, err)
				require.Len(t, records, 3)
			}
			result, err := session.Run(ctx, query, nil)
			require.NoError(t, err)
			require.True(t, result.Next(ctx))
			require.Equal(t, int64(0), result.Record().Values[0])
			post("MATCH (n:TransportWire {i:0}) SET n.name = 'changed'")
			remaining := 0
			for result.Next(ctx) {
				record := result.Record()
				require.Equal(t, "original", record.Values[1].(map[string]any)["a"])
				require.Equal(t, "original", record.Values[2].(neo4j.Node).Props["name"])
				remaining++
			}
			require.NoError(t, result.Err())
			require.Equal(t, 2, remaining)
			fresh := post(query)
			require.Equal(t, "changed", fresh.Results[0].Data[0].Row[1].(map[string]any)["a"])
			require.Equal(t, "changed", fresh.Results[0].Data[0].Row[2].(map[string]any)["name"])

		})
	}
}
