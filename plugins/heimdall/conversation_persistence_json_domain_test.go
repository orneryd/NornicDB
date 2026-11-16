package heimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/require"
)

// persistedConversationHost records the parameters the private-conversation
// plugin sends to storage so a test can compare the stored documents with the
// bytes encoding/json produced before the property-domain migration.
type persistedConversationHost struct {
	*memoryDBRouter
	messageParams   map[string]interface{}
	assistantParams map[string]interface{}
}

func (h *persistedConversationHost) ChatWorkspace(ctx context.Context, createName *string) (heimdall.ChatWorkspace, error) {
	return heimdall.ChatWorkspace{Configured: true, Database: "test-db", Username: "tester"}, nil
}

func (h *persistedConversationHost) ChatDiscover(ctx context.Context, query string, limit int) (*heimdall.DiscoverResult, error) {
	return &heimdall.DiscoverResult{}, nil
}

func (h *persistedConversationHost) ChatQuery(ctx context.Context, query string, params map[string]interface{}) ([]map[string]interface{}, error) {
	switch {
	case strings.Contains(query, "AssistantAttachment {id:$attachment}"):
		return []map[string]interface{}{{
			"id": "attach-1", "name": "notes.txt", "content_type": "text/plain",
			"size": int64(4096), "estimated_tokens": int64(1024), "readable": true, "chunks": int64(3),
		}}, nil
	case strings.Contains(query, "t.database AS database"):
		return []map[string]interface{}{{"database": "", "archived": false}}, nil
	case strings.Contains(query, "RETURN m.content AS content,m.attachment_ids AS attachment_ids"):
		h.messageParams = params
		return []map[string]interface{}{{"content": params["content"], "attachment_ids": params["attachments"]}}, nil
	case strings.Contains(query, "t.summary AS summary,t.summary_count AS covered"):
		return []map[string]interface{}{{"summary": "", "covered": int64(0)}}, nil
	case strings.Contains(query, "WHERE m.id <> $exclude"):
		return nil, nil
	case strings.Contains(query, "RETURN m.id AS id"):
		h.assistantParams = params
		return []map[string]interface{}{{"id": params["assistant"]}}, nil
	}
	return nil, fmt.Errorf("unexpected conversation query: %s", query)
}

// The private-conversation plugin persists attachment_ids on the user message
// and tool_calls on the assistant message. Both documents must keep the exact
// bytes encoding/json wrote and must read back through the property owner.
func TestOpenTurnPersistsStandardJSONDocuments(t *testing.T) {
	host := &persistedConversationHost{memoryDBRouter: &memoryDBRouter{}}
	plugin := &WatcherPlugin{}
	require.NoError(t, plugin.Initialize(heimdall.SubsystemContext{SubsystemHost: heimdall.SubsystemHost{Database: host, Config: heimdall.DefaultConfig()}}))

	ctx := heimdall.WithConversationAttachments(context.Background(), []string{"attach-1"})
	turn, err := plugin.OpenTurn(ctx, "thread-1", "message-1", "summarise the attachment")
	require.NoError(t, err)
	defer turn.Release()

	const attachmentGolden = `["attach-1"]`
	standardIDs, err := json.Marshal([]string{"attach-1"})
	require.NoError(t, err)
	require.Equal(t, attachmentGolden, string(standardIDs))
	require.Equal(t, attachmentGolden, host.messageParams["attachments"])

	tools := []heimdall.ConversationTool{{
		ID:         "call-1",
		Name:       "query <n>",
		Status:     "completed",
		Input:      json.RawMessage(`{"cypher":"MATCH (n) RETURN n","limit":1}`),
		Result:     json.RawMessage(`{"rows":[{"n":1}]}`),
		DurationMS: math.MaxInt64,
	}}
	require.NoError(t, turn.Complete(ctx, heimdall.ConversationReply{Content: "saved", Status: "completed", Tools: tools}))

	const toolsGolden = `[{"id":"call-1","name":"query \u003cn\u003e","status":"completed","input":{"cypher":"MATCH (n) RETURN n","limit":1},"result":{"rows":[{"n":1}]},"duration_ms":9223372036854775807}]`
	standardTools, err := json.Marshal(tools)
	require.NoError(t, err)
	require.Equal(t, toolsGolden, string(standardTools))
	stored, ok := host.assistantParams["tools"].(string)
	require.True(t, ok)
	require.Equal(t, toolsGolden, stored)

	var decoded []heimdall.ConversationTool
	require.NoError(t, convert.PropertyJSON.Unmarshal([]byte(stored), &decoded))
	require.Equal(t, tools, decoded)
}
