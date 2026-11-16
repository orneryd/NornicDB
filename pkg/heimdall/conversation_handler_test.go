package heimdall

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type turnTestPlugin struct {
	*MockHeimdallPlugin
	saved    ConversationReply
	saveErr  error
	released bool
}

func (p *turnTestPlugin) OpenTurn(ctx context.Context, thread, message, content string) (*ConversationTurn, error) {
	return &ConversationTurn{Messages: []ChatMessage{{Role: "user", Content: content}}, Complete: func(ctx context.Context, reply ConversationReply) error { p.saved = reply; return p.saveErr }, Release: func() { p.released = true }}, nil
}

func TestConversationCompletionPersistsBeforeSuccessAndReportsSaveErrors(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "saved", true: "save failed"}[failed], func(t *testing.T) {
			prior := globalManager
			globalManager = nil
			manager := GetSubsystemManager()
			defer func() { _ = manager.StopAll(); globalManager = prior }()
			manager.SetContext(SubsystemHost{Config: DefaultConfig()})
			plugin := &turnTestPlugin{MockHeimdallPlugin: NewMockPlugin("turn-test")}
			if failed {
				plugin.saveErr = errors.New("database write failed")
			}
			require.NoError(t, manager.RegisterPlugin(plugin, "", true))
			generator := NewMockGenerator("test")
			generator.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
				return callback("A streamed reply ")
			}
			handler := testHandler(newTestManager(generator), DefaultConfig())
			request := ChatRequest{ConversationID: "thread", MessageID: "message", Stream: true, Messages: []ChatMessage{{Role: "user", Content: "Explain this database"}}}
			body, err := json.Marshal(request)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			handler.handleChatCompletions(recorder, httptest.NewRequest("POST", "/api/bifrost/chat/completions", strings.NewReader(string(body))))
			require.True(t, plugin.released)
			require.Equal(t, "A streamed reply", plugin.saved.Content)
			if failed {
				require.Contains(t, recorder.Body.String(), "nornic.error")
				require.NotContains(t, recorder.Body.String(), "[DONE]")
			} else {
				require.Contains(t, recorder.Body.String(), "[DONE]")
			}
		})
	}
}
