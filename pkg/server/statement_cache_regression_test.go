package server

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestStatementHTTPCompositeSelection(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "r2blockedcmp", []multidb.ConstituentRef{
		{Alias: "source", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
	}))
	_, err := users.CreateUser("r2limited", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "editor", []string{"nornic"}))
	token := getAuthToken(t, users, "r2limited")
	for _, query := range []string{
		"USE r2blockedcmp.source RETURN 1 AS one",
		"CALL { USE r2blockedcmp.source RETURN 1 AS one } RETURN one",
		"CALL { USE r2blockedcmp.source MATCH (n) RETURN count(n) AS c } RETURN c",
	} {
		t.Run(query, func(t *testing.T) {
			executor, err := server.executorCache.Get("nornic")
			require.NoError(t, err)
			_, err = executor.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			for _, explicit := range []bool{false, true} {
				body, raw := systemProcedureResponse(t, server, token, "nornic", query, explicit)
				t.Logf("response=%s", raw)
				require.NotEmpty(t, body.Errors)
				require.Equal(t, "Neo.ClientError.Security.Forbidden", body.Errors[0].Code)
				for _, result := range body.Results {
					require.Empty(t, result.Data)
				}
			}
		})
	}
}

type passwordChangeWriter struct {
	*httptest.ResponseRecorder
	beforeCacheInsert func()
}

func (w *passwordChangeWriter) Header() http.Header {
	if w.beforeCacheInsert != nil {
		callback := w.beforeCacheInsert
		w.beforeCacheInsert = nil
		callback()
	}
	return w.ResponseRecorder.Header()
}

func TestStatementPasswordCacheRepopulation(t *testing.T) {
	server, users := setupTestServer(t)
	_, err := users.CreateUser("r2race", "oldpassword123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("r2race:oldpassword123"))
	handler := server.withAuth(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }, auth.PermRead)
	req := httptest.NewRequest(http.MethodGet, "/r2", nil)
	req.Header.Set("Authorization", basic)
	// Complete an account update while the response cookie is being written.
	first := &passwordChangeWriter{ResponseRecorder: httptest.NewRecorder(), beforeCacheInsert: func() {
		require.NoError(t, users.ChangePassword("r2race", "oldpassword123", "newpassword123"))
	}}
	handler(first, req)
	require.Equal(t, http.StatusOK, first.Code)
	_, _, err = users.Authenticate("r2race", "oldpassword123", "review", "review")
	require.Error(t, err, "the authenticator already refuses the old password")
	next := httptest.NewRecorder()
	handler(next, req)
	t.Logf("new Basic request with changed password: status=%d body=%s", next.Code, next.Body.String())
	require.Equal(t, http.StatusUnauthorized, next.Code, "a late cache insertion must not resurrect the old password")
}
