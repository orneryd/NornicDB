package heimdall

import (
	"encoding/json"
	"fmt"
	"time"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// ConversationTool is the inspected operation and result, scoped to one turn.
type ConversationTool struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Status     string          `json:"status"`
	Input      json.RawMessage `json:"input"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
	DurationMS int64           `json:"duration_ms,omitempty"`
}

func safeToolValue(value interface{}) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return json.RawMessage(observability.RedactText(string(data)))
}
func (l *requestLifecycle) publishTool(index int) {
	if index < 0 || l.StreamWriter == nil {
		return
	}
	data, _ := valueconvert.PropertyJSON.Marshal(l.tools[index])
	fmt.Fprintf(l.StreamWriter, "event: nornic.tool\ndata: %s\n\n", data)
	l.StreamFlusher.Flush()
}
func (l *requestLifecycle) beginTool(name string, input interface{}) int {
	if l.promptCtx.ConversationID == "" {
		return -1
	}
	index := len(l.tools)
	l.tools = append(l.tools, ConversationTool{ID: generateID(), Name: name, Status: "running", Input: safeToolValue(input)})
	l.publishTool(index)
	return index
}
func (l *requestLifecycle) endTool(index int, result interface{}, err error, duration time.Duration) {
	if index < 0 {
		return
	}
	tool := &l.tools[index]
	tool.Status = "completed"
	tool.Result = safeToolValue(result)
	tool.DurationMS = duration.Milliseconds()
	if err != nil {
		tool.Status = "error"
		tool.Error = observability.RedactError(err).Error()
	}
	l.publishTool(index)
}
