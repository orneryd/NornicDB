package server

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	watcher "github.com/orneryd/nornicdb/plugins/heimdall"
	"github.com/stretchr/testify/require"
)

func TestAssistantAttachmentsStayPrivateAndUseBoundedFileRetrieval(t *testing.T) {
	server, authenticator := setupTestServer(t)
	reader := getAuthToken(t, authenticator, "reader")
	admin := getAuthToken(t, authenticator, "admin")
	response := makeRequest(t, server, "POST", "/api/bifrost/workspace", map[string]string{"database": "reader-files"}, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	heimdall.ResetSubsystemManagerForTests()
	manager := heimdall.GetSubsystemManager()
	manager.SetContext(heimdall.SubsystemHost{Database: router, Config: heimdall.DefaultConfig()})
	plugin := &watcher.WatcherPlugin{}
	require.NoError(t, manager.RegisterPlugin(plugin, "", true))
	t.Cleanup(func() { require.NoError(t, manager.StopAll()); heimdall.ResetSubsystemManagerForTests() })
	response = makeRequest(t, server, "POST", "/api/bifrost/conversations", map[string]string{"title": "File review"}, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	var thread map[string]interface{}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &thread))
	id := thread["id"].(string)
	upload := func(name string, data []byte) *httptest.ResponseRecorder {
		t.Helper()
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		part, err := writer.CreateFormFile("file", name)
		require.NoError(t, err)
		_, err = part.Write(data)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		request := httptest.NewRequest("POST", "/api/bifrost/conversations/"+id+"/attachments", &body)
		request.Header.Set("Content-Type", writer.FormDataContentType())
		request.Header.Set("Authorization", "Bearer "+reader)
		result := httptest.NewRecorder()
		server.buildRouter().ServeHTTP(result, request)
		return result
	}
	data := []byte(strings.Repeat("needle file data\n", 1200))
	response = upload("notes.txt", data)
	require.Equal(t, 200, response.Code, response.Body.String())
	var attachment heimdall.ConversationAttachment
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &attachment))
	require.True(t, attachment.Readable)
	require.Greater(t, attachment.Chunks, 2)
	repeated := upload("notes.txt", data)
	require.Equal(t, 200, repeated.Code, repeated.Body.String())
	var duplicate heimdall.ConversationAttachment
	require.NoError(t, json.Unmarshal(repeated.Body.Bytes(), &duplicate))
	require.Equal(t, attachment.ID, duplicate.ID)
	response = makeRequest(t, server, "GET", "/api/bifrost/conversations/"+id+"/attachments/"+attachment.ID, nil, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Equal(t, data, response.Body.Bytes())
	response = makeRequest(t, server, "GET", "/api/bifrost/conversations/"+id+"/attachments/"+attachment.ID, nil, "Bearer "+admin)
	require.NotEqual(t, 200, response.Code, "another account's assistant API must not resolve this private file")
	ctx := auth.WithRequestIdentity(t.Context(), auth.RequestIdentity{ID: server.dbManager.PrivateDatabaseOwner("reader-files"), Username: "reader"})
	ctx = heimdall.WithConversationScope(ctx, id)
	ctx = heimdall.WithConversationAttachments(ctx, []string{attachment.ID})
	turn, err := plugin.OpenTurn(ctx, id, "file-question", "Summarize the file")
	require.NoError(t, err)
	require.Contains(t, turn.Messages[0].Content, "Kept outside model context")
	require.NotContains(t, turn.Messages[0].Content, "needle file data")
	result, err := plugin.Actions()["chat_memory"].Handler(heimdall.ActionContext{Context: ctx, Params: map[string]interface{}{"attachment_id": attachment.ID, "query": "needle"}})
	require.NoError(t, err)
	rows := result.Data["file_chunks"].([]map[string]interface{})
	require.Len(t, rows, 2)
	require.Contains(t, rows[0]["content"], "needle")
	require.NoError(t, turn.Complete(ctx, heimdall.ConversationReply{Content: "Retrieved relevant file chunks", Status: "completed"}))
	turn.Release()
	response = makeRequest(t, server, "GET", "/api/bifrost/conversations/"+id, nil, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "notes.txt")
	response = makeRequest(t, server, "DELETE", "/api/bifrost/conversations/"+id, nil, "Bearer "+reader)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	for _, label := range []string{"AssistantAttachment", "AssistantAttachmentData", "AssistantAttachmentChunk", "AssistantMessage"} {
		rows, err := router.ChatQuery(ctx, "MATCH (n:"+label+") RETURN count(n) AS count", nil)
		require.NoError(t, err)
		require.Equal(t, int64(0), rows[0]["count"])
	}
}
