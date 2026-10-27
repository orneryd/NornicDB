package server

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/url"
	"testing"
)

// Both coordinator and peer execute through their public transaction handlers.
// The peer returns stored data plus independently evaluated source literals;
// a synthetic marker/echo response cannot satisfy the assertion.
func TestFabricRemoteRawSourceEmission(t *testing.T) {
	catalog := newDecodedIdentityCatalog(t)
	for _, tc := range []struct {
		name, query string
		columns     []string
		row         []any
	}{
		{"direct", `USE identity_catalog.` + "` r `" + ` MATCH (n:AliasIdentityProbe) RETURN n.marker AS marker, '\u005cu0061' AS escaped, '\u0061' AS control`, []string{"marker", "escaped", "control"}, []any{decodedIdentityAllowedName, `\u0061`, "a"}},
		{"import", `WITH 7 AS input CALL { WITH input USE identity_catalog.` + "` r `" + ` MATCH (n:AliasIdentityProbe) RETURN n.marker AS marker, '\u005cu0061' AS escaped, '\u0061' AS control, input AS imported } RETURN marker, escaped, control, imported`, []string{"marker", "escaped", "control", "imported"}, []any{decodedIdentityAllowedName, `\u0061`, "a", float64(7)}},
		{"scoped", `WITH 7 AS input CALL (input) { USE identity_catalog.` + "` r `" + ` MATCH (n:AliasIdentityProbe) RETURN n.marker AS marker, '\u005cu0061' AS escaped, '\u0061' AS control, input AS imported } RETURN marker, escaped, control, imported`, []string{"marker", "escaped", "control", "imported"}, []any{decodedIdentityAllowedName, `\u0061`, "a", float64(7)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog.resetProbes()
			wire := makeRequest(t, catalog.server, http.MethodPost, "/db/"+url.PathEscape(decodedIdentityCatalogName)+"/tx/commit", map[string]any{"statements": []any{map[string]any{"statement": tc.query}}}, catalog.token)
			require.Equal(t, http.StatusOK, wire.Code, wire.Body.String())
			var response TransactionResponse
			require.NoError(t, json.Unmarshal(wire.Body.Bytes(), &response))
			require.Empty(t, response.Errors, "%+v", response.Errors)
			require.Len(t, response.Results, 1)
			require.Equal(t, tc.columns, response.Results[0].Columns)
			require.Len(t, response.Results[0].Data, 1)
			require.Equal(t, tc.row, response.Results[0].Data[0].Row)
			require.Positive(t, catalog.allowed.observed())
			require.Zero(t, catalog.denied.observed())
		})
	}
	t.Run("direct-raw-executor", func(t *testing.T) {
		executor := fabric.NewRemoteFragmentExecutor()
		defer executor.Close()
		location := &fabric.LocationRemote{GraphName: decodedIdentityCatalogName + "." + decodedIdentitySpacedAlias, DBName: decodedIdentityAllowedName, ResolveEngine: func(string) (storage.Engine, error) {
			return catalog.server.dbManager.GetStorage(decodedIdentityCatalogName + "." + decodedIdentitySpacedAlias)
		}}
		result, err := executor.Execute(t.Context(), location, `MATCH (n:AliasIdentityProbe) RETURN n.marker AS marker, '\u005cu0061' AS escaped, '\u0061' AS control`, nil, "", storage.RemoteAccessRead)
		require.NoError(t, err)
		require.Equal(t, []string{"marker", "escaped", "control"}, result.Columns)
		require.Equal(t, [][]any{{decodedIdentityAllowedName, `\u0061`, "a"}}, result.Rows)
	})

}
