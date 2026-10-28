package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestDefaultNamespaceConfigAgreesAcrossOpenAndHTTP(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		name := "memory"
		if persistent {
			name = "persistent"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("NORNICDB_DEFAULT_DATABASE", "")
			t.Setenv("NEO4J_dbms_default__database", "")
			config := nornicdb.DefaultConfig()
			config.Database.DefaultDatabase = "configured_namespace"
			config.Database.AsyncWritesEnabled = false
			config.Memory.DecayEnabled = false
			config.Memory.AutoLinksEnabled = false
			config.Memory.EmbeddingEnabled = false
			config.Auth.Enabled = false
			directory := ""
			if persistent {
				directory = t.TempDir()
			}
			db, err := nornicdb.Open(directory, config)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			_, err = db.ExecuteCypher(t.Context(), "CREATE (:NamespaceFixture {value: 1})", nil)
			require.NoError(t, err)
			serverConfig := DefaultConfig()
			serverConfig.ProcessConfig = config
			serverConfig.EmbeddingEnabled = false
			serverConfig.MCPEnabled = false
			server, err := New(db, nil, serverConfig)
			require.NoError(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				require.NoError(t, server.Stop(ctx))
			})
			require.Equal(t, config.Database.DefaultDatabase, server.dbManager.DefaultDatabaseName())
			response := makeRequest(t, server, http.MethodPost, "/db/configured_namespace/tx/commit", map[string]interface{}{
				"statements": []map[string]interface{}{{"statement": "MATCH (n:NamespaceFixture) RETURN count(n) AS total"}},
			}, "")
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			var body struct {
				Errors  []any `json:"errors"`
				Results []struct {
					Columns []string `json:"columns"`
					Data    []struct {
						Row []int `json:"row"`
					} `json:"data"`
				} `json:"results"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Empty(t, body.Errors)
			require.Len(t, body.Results, 1)
			require.Equal(t, []string{"total"}, body.Results[0].Columns)
			require.Len(t, body.Results[0].Data, 1)
			require.Equal(t, []int{1}, body.Results[0].Data[0].Row, "embedded and HTTP operations must use the same resolved namespace")
		})
	}
}
