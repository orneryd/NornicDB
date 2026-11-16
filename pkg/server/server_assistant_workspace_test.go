package server

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/multidb"
	watcher "github.com/orneryd/nornicdb/plugins/heimdall"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestAssistantToolsUseCallerPrincipalAndLiveReadOnlyScope(t *testing.T) {
	server, _ := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "other"}))
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	readable := true
	original := auth.WithRequestIdentity(t.Context(), auth.RequestIdentity{ID: "caller-principal", Username: "caller"})
	original = auth.WithRequestPrincipalRoles(original, []string{"admin"})
	original = auth.WithRequestPrincipal(original, func(database string, permission auth.Permission) bool { return readable }, auth.NewRequestDatabaseScope("nornic", map[string]string{"nornic": "nornic", "other": "other", "selected-alias": "nornic"}))
	original = auth.WithRequestResolvedAccessResolver(original, func(string) auth.ResolvedAccess { return auth.ResolvedAccess{Read: true, Write: true} })
	ctx := auth.WithReadOnlyDatabaseScope(original, "selected-alias")
	require.Equal(t, auth.RequestIdentityFromContext(original), auth.RequestIdentityFromContext(ctx))
	_, err := router.Query(ctx, "nornic", "RETURN 1 AS ready", nil)
	require.NoError(t, err)
	for _, query := range []struct{ database, statement string }{
		{"nornic", "CREATE (:NotAllowed)"}, {"other", "RETURN 1 AS ready"}, {"nornic", "USE other RETURN 1 AS ready"}, {"nornic", "CREATE USER forbidden SET PASSWORD 'synthetic-password'"},
	} {
		_, err := router.Query(ctx, query.database, query.statement, nil)
		require.Error(t, err, "%s must remain inside the caller's read-only selection", query.statement)
	}
	require.False(t, auth.RequestResolvedAccessResolverFromContext(ctx)("nornic").Write, "a legacy administrator adapter must not restore write access")
	readable = false
	_, err = router.Query(ctx, "nornic", "RETURN 1 AS ready", nil)
	require.Error(t, err, "a retained turn must observe revoked access")
}

func TestAssistantConversationPersistsTurnsAndRetryWithoutDuplicates(t *testing.T) {
	server, authenticator := setupTestServer(t)
	reader := getAuthToken(t, authenticator, "reader")
	response := makeRequest(t, server, http.MethodPost, "/api/bifrost/workspace", map[string]string{"database": "reader-chats"}, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	ctx := auth.WithRequestIdentity(t.Context(), auth.RequestIdentity{ID: server.dbManager.PrivateDatabaseOwner("reader-chats"), Username: "reader"})
	ctx = auth.WithRequestPrincipal(ctx, func(database string, permission auth.Permission) bool {
		return database == "nornic" && (permission == auth.PermRead || permission == auth.Permission("access"))
	}, auth.NewRequestDatabaseScope("nornic", map[string]string{"nornic": "nornic"}))
	plugin := &watcher.WatcherPlugin{}
	require.NoError(t, plugin.Initialize(heimdall.SubsystemContext{SubsystemHost: heimdall.SubsystemHost{Database: router, Config: heimdall.DefaultConfig()}}))
	database := "nornic"
	created, err := plugin.Conversation(ctx, heimdall.ConversationRequest{Operation: "create", Database: &database})
	require.NoError(t, err)
	id := created.(map[string]interface{})["id"].(string)
	turn, err := plugin.OpenTurn(ctx, id, "message-1", "Explain this database")
	require.NoError(t, err)
	require.Equal(t, []heimdall.ChatMessage{{Role: "user", Content: "Explain this database"}}, turn.Messages)
	_, err = plugin.OpenTurn(ctx, id, "message-2", "Another question")
	require.ErrorIs(t, err, heimdall.ErrConversationBusy)
	require.NoError(t, turn.Complete(ctx, heimdall.ConversationReply{Content: "This database contains the test records.", Status: "completed"}))
	turn.Release()
	for attempt := 0; attempt < 2; attempt++ {
		turn, err = plugin.OpenTurn(ctx, id, "message-1", "Explain this database")
		require.NoError(t, err)
		require.Len(t, turn.Messages, 1, "a retried response must not feed itself back into the model")
		require.NoError(t, turn.Complete(ctx, heimdall.ConversationReply{Content: "Saved response", Reasoning: "Model-provided reasoning", Status: "completed"}))
		turn.Release()
	}
	saved, err := plugin.Conversation(ctx, heimdall.ConversationRequest{Operation: "get", ID: id})
	require.NoError(t, err)
	messages := saved.(map[string]interface{})["messages"].([]map[string]interface{})
	require.Len(t, messages, 2)
	require.Equal(t, "Saved response", messages[1]["content"])
	require.Equal(t, "Model-provided reasoning", messages[1]["reasoning_content"])
	turn, err = plugin.OpenTurn(ctx, id, "message-2", "What did I ask?")
	require.NoError(t, err)
	require.Len(t, turn.Messages, 3)
	require.Equal(t, "Saved response", turn.Messages[1].Content)
	require.NoError(t, turn.Complete(ctx, heimdall.ConversationReply{Content: "Stopped by the user", Status: "cancelled"}))
	turn.Release()
	plugin.PostExecute(&heimdall.PostExecuteContext{Context: ctx, Action: "heimdall_watcher_query", Result: &heimdall.ActionResult{Success: true}, PluginData: map[string]interface{}{"private_conversation": true}})
	for _, label := range []string{"AssistantMessage", "HeimdallSessionMemory", "HeimdallSessionFact"} {
		rows, err := router.Query(ctx, "nornic", "MATCH (m:"+label+") RETURN count(m) AS count", nil)
		require.NoError(t, err)
		require.Equal(t, int64(0), rows[0]["count"], "private conversation data must never go to the default database")
	}
}

func TestAssistantWorkspaceIsPrivateAcrossHTTPAndRoleOnlyTransports(t *testing.T) {
	server, authenticator := setupTestServer(t)
	_, err := authenticator.CreateUser(t.Context(), "sam", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	reader := getAuthToken(t, authenticator, "reader")
	sam := getAuthToken(t, authenticator, "sam")
	admin := getAuthToken(t, authenticator, "admin")
	response := makeRequest(t, server, http.MethodGet, "/api/bifrost/workspace", nil, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	var workspace heimdall.ChatWorkspace
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &workspace))
	require.False(t, workspace.Configured)
	require.Equal(t, "reader-chats", workspace.SuggestedDatabase)
	response = makeRequest(t, server, http.MethodPost, "/api/bifrost/workspace", map[string]string{"database": "reader-notes"}, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	for _, token := range []string{reader, admin} {
		response = makeRequest(t, server, http.MethodGet, "/db/reader-notes", nil, "Bearer "+token)
		require.Equal(t, 200, response.Code, response.Body.String())
	}
	response = makeRequest(t, server, http.MethodGet, "/db/reader-notes", nil, "Bearer "+sam)
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	response = makeRequest(t, server, http.MethodGet, "/db", nil, "Bearer "+sam)
	require.NotContains(t, response.Body.String(), "reader-notes")
	require.False(t, server.GetDatabaseAccessModeForRoles([]string{"viewer"}).CanAccessDatabase("reader-notes"))
	require.True(t, server.GetDatabaseAccessModeForRoles([]string{"admin"}).CanAccessDatabase("reader-notes"))
	// The private persistence grant cannot select any other database or create users.
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	identity := auth.RequestIdentity{ID: server.dbManager.PrivateDatabaseOwner("reader-notes"), Username: "reader"}
	ctx := auth.WithRequestIdentity(context.Background(), identity)
	rows, err := router.ChatQuery(ctx, "MATCH (t:AssistantThread) RETURN t.id AS id", nil)
	require.NoError(t, err)
	require.NotNil(t, rows, "empty conversation collections must serialize as [] rather than null")
	require.Empty(t, rows)
	_, err = router.ChatQuery(ctx, "CREATE (:AssistantThread {id: $id})", map[string]interface{}{"id": "first"})
	require.NoError(t, err)
	_, err = router.ChatQuery(ctx, "USE nornic MATCH (n) RETURN n", nil)
	require.Error(t, err)
	_, err = router.ChatQuery(ctx, "CREATE USER intruder SET PASSWORD 'not-a-secret'", nil)
	require.Error(t, err)
}
