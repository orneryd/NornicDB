package heimdall

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

func TestConversationToolEventsUseCanonicalRedaction(t *testing.T) {
	recorder := httptest.NewRecorder()
	lifecycle := &requestLifecycle{promptCtx: &PromptContext{ConversationID: "thread"}, StreamWriter: recorder, StreamFlusher: recorder}
	const endpoint = "https://viewer:synthetic-pass@example.invalid/path?api_key=synthetic-key"
	input := map[string]interface{}{"endpoint": endpoint, "count": 7}
	encoded, err := json.Marshal(input)
	require.NoError(t, err)
	index := lifecycle.beginTool("inspect", input)
	require.Zero(t, index)
	require.Len(t, lifecycle.tools, 1)
	require.Equal(t, json.RawMessage(observability.RedactText(string(encoded))), lifecycle.tools[0].Input)
	require.True(t, json.Valid(lifecycle.tools[0].Input))
	require.Equal(t, "running", lifecycle.tools[0].Status)
	failure := errors.New("provider failed at " + endpoint)
	lifecycle.endTool(index, input, failure, 12*time.Millisecond)
	require.Equal(t, "error", lifecycle.tools[0].Status)
	require.Equal(t, int64(12), lifecycle.tools[0].DurationMS)
	require.Equal(t, observability.RedactError(failure).Error(), lifecycle.tools[0].Error)
	require.Equal(t, lifecycle.tools[0].Input, lifecycle.tools[0].Result)
	require.Equal(t, 2, strings.Count(recorder.Body.String(), "event: nornic.tool"))
	require.NotContains(t, recorder.Body.String(), "synthetic-pass")
	require.NotContains(t, recorder.Body.String(), "synthetic-key")
	require.Equal(t, json.RawMessage(`null`), safeToolValue(make(chan int)), "unencodable tool values remain valid JSON")
	success := lifecycle.beginTool("inspect", nil)
	lifecycle.endTool(success, map[string]string{"result": "ok"}, nil, time.Millisecond)
	require.Equal(t, "completed", lifecycle.tools[success].Status)
	require.JSONEq(t, `{"result":"ok"}`, string(lifecycle.tools[success].Result))
	idle := &requestLifecycle{promptCtx: &PromptContext{}}
	require.Equal(t, -1, idle.beginTool("inspect", input))
	idle.endTool(-1, nil, nil, 0)
	require.Empty(t, idle.tools, "stateless requests do not accumulate conversation events")
}

func TestConversationContextUsageReportsAndRejectsOverBudget(t *testing.T) {
	recorder := httptest.NewRecorder()
	lifecycle := &requestLifecycle{contextLimit: 100, StreamWriter: recorder, StreamFlusher: recorder}
	require.NoError(t, lifecycle.updateContextUsage(70, 30), "the exact limit is accepted")
	require.Equal(t, 70, lifecycle.contextTokens)
	require.Contains(t, recorder.Body.String(), "event: nornic.usage")
	require.Contains(t, recorder.Body.String(), `"estimated":true`)
	require.ErrorContains(t, lifecycle.updateContextUsage(71, 30), "exceeding the 100-token context")
	require.Equal(t, 71, lifecycle.contextTokens, "usage reports the rejected request's estimate too")
	require.Equal(t, 2, strings.Count(recorder.Body.String(), "event: nornic.usage"))
	require.NoError(t, (&requestLifecycle{}).updateContextUsage(1000, 1000), "an unspecified limit does not introduce an artificial cap")
}
