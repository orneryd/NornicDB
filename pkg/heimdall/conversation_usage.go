package heimdall

import (
	"encoding/json"
	"fmt"
)

func (l *requestLifecycle) updateContextUsage(tokens, reservedOutput int) error {
	l.contextTokens = tokens
	if l.StreamWriter != nil {
		data, _ := json.Marshal(map[string]interface{}{"input_tokens_estimated": tokens, "context_window": l.contextLimit, "reserved_output_tokens": reservedOutput, "estimated": true})
		fmt.Fprintf(l.StreamWriter, "event: nornic.usage\ndata: %s\n\n", data)
		l.StreamFlusher.Flush()
	}
	if l.contextLimit > 0 && tokens+reservedOutput > l.contextLimit {
		return fmt.Errorf("this turn needs approximately %d input tokens plus %d reserved output tokens, exceeding the %d-token context. Compact older turns or attach large inputs as files", tokens, reservedOutput, l.contextLimit)
	}
	return nil
}
