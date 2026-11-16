package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	watcher "github.com/orneryd/nornicdb/plugins/heimdall"
	"github.com/stretchr/testify/require"
)

type assistantSummaryInvoker struct {
	heimdall.NoOpHeimdallInvoker
	started chan auth.RequestIdentity
	release <-chan struct{}
}

func (m *assistantSummaryInvoker) SendRawPrompt(ctx context.Context, prompt string) (*heimdall.ActionResult, error) {
	select {
	case m.started <- auth.RequestIdentityFromContext(ctx):
	default:
	}
	select {
	case <-m.release:
		return &heimdall.ActionResult{Success: true, Message: "The caller is reviewing their database schema and attached notes. Preserve the latest questions."}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestAssistantCompactionRunsInBackgroundWithCallerAndRetainsTranscript(t *testing.T) {
	server, authenticator := setupTestServer(t)
	reader := getAuthToken(t, authenticator, "reader")
	response := makeRequest(t, server, "POST", "/api/bifrost/workspace", map[string]string{"database": "reader-context"}, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	heimdall.ResetSubsystemManagerForTests()
	manager := heimdall.GetSubsystemManager()
	work := lifecycle.NewWorkLifetime(t.Context())
	release := make(chan struct{})
	var releaseOnce sync.Once
	invoker := &assistantSummaryInvoker{started: make(chan auth.RequestIdentity, 16), release: release}
	config := heimdall.DefaultConfig()
	config.ContextSize = 2048
	manager.SetContext(heimdall.SubsystemHost{Database: router, Config: config, Lifetime: work, Heimdall: invoker})
	plugin := &watcher.WatcherPlugin{}
	require.NoError(t, manager.RegisterPlugin(plugin, "", true))
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		require.NoError(t, work.Close())
		require.NoError(t, manager.StopAll())
		heimdall.ResetSubsystemManagerForTests()
	})
	response = makeRequest(t, server, "POST", "/api/bifrost/conversations", map[string]string{"title": "Context review"}, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	var thread map[string]interface{}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &thread))
	id := thread["id"].(string)
	ctx := auth.WithRequestIdentity(t.Context(), auth.RequestIdentity{ID: server.dbManager.PrivateDatabaseOwner("reader-context"), Username: "reader"})
	for index := 0; index < 5; index++ {
		turn, err := plugin.OpenTurn(ctx, id, fmt.Sprintf("message-%d", index), fmt.Sprintf("Schema question %d", index))
		require.NoError(t, err)
		require.NoError(t, turn.Complete(ctx, heimdall.ConversationReply{Content: fmt.Sprintf("Schema answer %d", index), Status: "completed", ContextTokens: 1700, ContextLimit: 2048}))
		turn.Release()
	}
	response = makeRequest(t, server, "POST", "/api/bifrost/conversations/"+id+"/compact", nil, "Bearer "+reader)
	require.Equal(t, 202, response.Code, response.Body.String())
	select {
	case caller := <-invoker.started:
		require.Equal(t, "reader", caller.Username)
		require.Equal(t, auth.RequestIdentityFromContext(ctx).ID, caller.ID)
	case <-time.After(3 * time.Second):
		t.Fatal("background summarizer did not start")
	}
	response = makeRequest(t, server, "GET", "/api/bifrost/conversations/"+id+"/context", nil, "Bearer "+reader)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"compaction_status":"running"`)
	_, err := plugin.OpenTurn(ctx, id, "while-compacting", "Another question")
	require.ErrorIs(t, err, heimdall.ErrConversationBusy)
	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		response = makeRequest(t, server, "GET", "/api/bifrost/conversations/"+id+"/context", nil, "Bearer "+reader)
		return response.Code == 200 && strings.Contains(response.Body.String(), `"compaction_status":"complete"`)
	}, 3*time.Second, 10*time.Millisecond)
	saved, err := plugin.Conversation(ctx, heimdall.ConversationRequest{Operation: "get", ID: id})
	require.NoError(t, err)
	require.Len(t, saved.(map[string]interface{})["messages"], 10, "the stored transcript must remain intact")
	require.Eventually(t, func() bool {
		turn, err := plugin.OpenTurn(ctx, id, "after-compaction", "What is next?")
		if err != nil {
			return false
		}
		defer turn.Release()
		require.Equal(t, "system", turn.Messages[0].Role)
		require.Contains(t, turn.Messages[0].Content, "reviewing their database schema")
		require.Len(t, turn.Messages, 4)
		return true
	}, 3*time.Second, 10*time.Millisecond)
}
