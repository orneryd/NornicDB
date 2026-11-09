package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestCatalogVisibilityBeforeProjectionAndDatabaseLookup(t *testing.T) {
	s, users := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, s.dbManager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "hidden_catalog"}))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, "admin", []string{"nornic"}))
	token := getAuthToken(t, users, "admin")
	for _, query := range []string{
		"SHOW DATABASES YIELD name RETURN name",
		"SHOW DATABASES YIELD name AS database RETURN database",
		"SHOW DATABASES YIELD name RETURN count(name) AS total",
	} {
		rows, code := statementRows(t, s, "nornic", token, query)
		require.Empty(t, code, query)
		require.Len(t, rows, 1, query)
		if query == "SHOW DATABASES YIELD name RETURN count(name) AS total" {
			require.EqualValues(t, 1, rows[0][0])
		} else {
			require.Equal(t, "nornic", rows[0][0])
		}
	}
}

func TestCatalogDatabaseSelectionAcrossTransactionEntryPoints(t *testing.T) {
	s, users := setupTestServer(t)
	require.NoError(t, s.dbManager.CreateDatabase(context.Background(), multidb.DatabaseCreateRequest{Name: "hidden_catalog"}))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(context.Background(), "admin", []string{"nornic"}))
	token := getAuthToken(t, users, "admin")
	for _, route := range []struct {
		name, suffix string
		statements   []map[string]any
		status       int
	}{
		{"implicit", "/tx/commit", []map[string]any{{"statement": "RETURN 1"}}, http.StatusOK},
		{"multi", "/tx/commit", []map[string]any{{"statement": "RETURN 1"}, {"statement": "RETURN 2"}}, http.StatusOK},
		{"explicit", "/tx", []map[string]any{{"statement": "RETURN 1"}}, http.StatusForbidden},
	} {
		for _, db := range []string{"hidden_catalog", "absent_catalog"} {
			t.Run(route.name+"/"+db, func(t *testing.T) {
				r := makeRequest(t, s, http.MethodPost, "/db/"+db+route.suffix, map[string]any{"statements": route.statements}, "Bearer "+token)
				require.Equal(t, route.status, r.Code, r.Body.String())
				require.Contains(t, r.Body.String(), "Neo.ClientError.Security.Forbidden")
				require.NotContains(t, r.Body.String(), "DatabaseNotFound")
			})
		}
	}
}
