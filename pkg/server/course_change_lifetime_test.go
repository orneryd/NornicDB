package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestCourseChangeServerLifetime(t *testing.T) {
	server, auth := setupTestServer(t)
	lifetime := server.shutdown.workContext
	server.config.Port = 0
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "lifetimeleaf"}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "lifetimecomp", []multidb.ConstituentRef{{Alias: "leaf", DatabaseName: "lifetimeleaf", Type: "local", AccessMode: "read_write"}}))
	require.NoError(t, server.Start())
	req, err := http.NewRequest(http.MethodPost, "http://"+server.listener.Addr().String()+"/db/lifetimecomp/tx", strings.NewReader(`{"statements":[{"statement":"USE lifetimecomp.leaf CREATE (:Lifetime) RETURN 1"}]}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+getAuthToken(t, auth, "admin"))
	req.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	var opened TransactionResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&opened))
	response.Body.Close()
	require.Empty(t, opened.Errors)
	require.NotEmpty(t, opened.Commit)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, server.Stop(ctx))
	require.ErrorIs(t, lifetime.Err(), context.Canceled, "New's transaction lifetime must be canceled by Stop after Start")
}
