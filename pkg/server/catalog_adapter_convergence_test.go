package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPAdapterConvergedCatalogContract(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")

	t.Run("adapter pointer implements complete required contract", func(t *testing.T) {
		var adapter any = server.databaseManager()
		_, ok := adapter.(cypher.DatabaseManagerInterface)
		require.True(t, ok, "*databaseManagerAdapter must implement cypher.DatabaseManagerInterface")
	})

	t.Run("bad limits type rejected before any manager effect", func(t *testing.T) {
		adapter := server.databaseManager()
		require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "limits_bad"}))

		err := adapter.SetDatabaseLimits(t.Context(), "limits_bad", map[string]interface{}{})
		require.EqualError(t, err, "invalid limits type")

		// Canceled context + missing database + bad type: conversion wins.
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		err = adapter.SetDatabaseLimits(canceled, "absent_database", nil)
		require.EqualError(t, err, "invalid limits type")

		boxed, getErr := adapter.GetDatabaseLimits("limits_bad")
		require.NoError(t, getErr)
		assert.Nil(t, boxed, "rejected input must not publish limits")
	})

	t.Run("nil private outer adapter receiver panics on method promotion", func(t *testing.T) {
		// Decided boundary: nil private outers are unsupported. Promoting the
		// embedded Catalog selects the field on a nil pointer, so the call
		// panics. A zero-value NONNIL adapter keeps bad-type rejection.
		var nilOuter *databaseManagerAdapter
		require.Panics(t, func() {
			_ = nilOuter.SetDatabaseLimits(t.Context(), "limits_bad", map[string]interface{}{})
		})
		zero := &databaseManagerAdapter{}
		require.EqualError(t, zero.SetDatabaseLimits(t.Context(), "limits_bad", uint8(0)), "invalid limits type")

		// The embedded value itself still rejects bad input directly.
		var nilCatalog *catalogadapter.Catalog
		require.EqualError(t, nilCatalog.SetDatabaseLimits(t.Context(), "limits_bad", uint8(0)), "invalid limits type")
	})

	t.Run("ALTER DATABASE SET LIMIT reaches the native manager through HTTP", func(t *testing.T) {
		require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "limits_http"}))
		require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "admin", []string{"nornic", "limits_http"}))
		require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "admin", "limits_http", true, true))

		result := postStatement(t, server, "nornic", "ALTER DATABASE limits_http SET LIMIT max_nodes = 42", token)
		require.Empty(t, result.Errors, "ALTER DATABASE SET LIMIT must succeed through HTTP")
		require.Len(t, result.Results, 1)
		require.Len(t, result.Results[0].Data, 1)
		require.Equal(t, "limits_http", result.Results[0].Data[0].Row[0])

		limits, err := server.dbManager.GetDatabaseLimits("limits_http")
		require.NoError(t, err)
		require.NotNil(t, limits)
		require.Equal(t, int64(42), limits.Storage.MaxNodes, "native manager must hold the HTTP-transported limits")

		show := postStatement(t, server, "nornic", "SHOW LIMITS FOR DATABASE limits_http", token)
		require.Empty(t, show.Errors)
		require.Len(t, show.Results, 1)
		found := false
		for _, row := range show.Results[0].Data {
			if len(row.Row) >= 3 && row.Row[1] == "max_nodes" {
				require.EqualValues(t, 42, row.Row[2], "HTTP JSON carries the limit as a number")
				found = true
			}
		}
		require.True(t, found, "SHOW LIMITS must read back max_nodes=42")
	})
}

func postStatement(t *testing.T, server *Server, database, statement, token string) TransactionResponse {
	t.Helper()
	recorder := makeRequest(t, server, http.MethodPost, "/db/"+database+"/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": statement}},
	}, token)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var response TransactionResponse
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
	return response
}

func TestHTTPDropAdapterKeepsCacheOnErrors(t *testing.T) {
	for _, test := range []struct {
		name, statement string
		canceled        bool
	}{
		{name: "regular rejection", statement: "DROP DATABASE nornic"},
		{name: "composite rejection", statement: "DROP COMPOSITE DATABASE absent_composite"},
		{name: "canceled regular drop", canceled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			name := "nornic"
			if test.canceled {
				name = "cancel_drop"
				require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
			}
			cached, err := server.executorCache.Get(name)
			require.NoError(t, err)
			_, err = cached.Execute(t.Context(), "RETURN 1 AS value", nil)
			require.NoError(t, err)
			owner := cached.StatementCaches()
			require.NotNil(t, owner)
			if test.canceled {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				require.ErrorIs(t, server.databaseManager().DropDatabase(ctx, name), context.Canceled)
			} else {
				response := postStatement(t, server, "system", test.statement, token)
				require.NotEmpty(t, response.Errors)
			}
			next, err := server.executorCache.Get(name)
			require.NoError(t, err)
			require.Same(t, cached, next, "a rejected drop must not invalidate the surviving execution view")
			require.False(t, owner.Disposed())
			require.True(t, server.dbManager.Exists(name))
		})
	}
}
