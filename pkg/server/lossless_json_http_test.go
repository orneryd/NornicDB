package server

import (
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLosslessJSONPublicRemoteHTTP(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			t.Setenv("NORNIC_CYPHER_PARSER", parser)
			server, authenticator := setupTestServer(t)
			endpoint := httptest.NewServer(server.buildRouter())
			defer endpoint.Close()
			remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: endpoint.URL, Database: "nornic", AuthToken: "Bearer " + getAuthToken(t, authenticator, "admin")})
			require.NoError(t, err)
			defer remote.Close()
			high := string(cyphertext.AppendCodepoint(nil, 0xd800))
			low := string(cyphertext.AppendCodepoint(nil, 0xdc00))
			value := map[string]any{high: low, low: high, "large": int64(9007199254740993), `literal\ud800`: `literal\ud800`}
			columns, rows, err := remote.QueryCypher(t.Context(), "RETURN $value AS value", map[string]any{"value": value}, storage.RemoteAccessRead)
			require.NoError(t, err)
			require.Equal(t, []string{"value"}, columns)
			require.Equal(t, [][]any{{value}}, rows)
			query := "RETURN {" + cyphertext.PrintIdentifier(high, nil, cyphertext.RawSource) + ": 1, " + cyphertext.PrintIdentifier(low, nil, cyphertext.RawSource) + ": 2} AS value"
			_, rows, err = remote.QueryCypher(t.Context(), query, nil, storage.RemoteAccessRead)
			require.NoError(t, err)
			require.Equal(t, [][]any{{map[string]any{high: int64(1), low: int64(2)}}}, rows)
			// A graph value forces the ordered metadata/REST traversal, whose map
			// keys must use the same lossless token owner as ordinary JSON trees.
			_, rows, err = remote.QueryCypher(t.Context(), "CREATE (n:LosslessJSONWire {marker: 'x'}) RETURN [n, $value] AS value", map[string]any{"value": value}, storage.RemoteAccessWrite)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			list, ok := rows[0][0].([]any)
			require.True(t, ok)
			require.Len(t, list, 2)
			_, ok = list[0].(storage.RemoteEntityValue)
			require.True(t, ok)
			require.Equal(t, value, list[1])

		})
	}
}
