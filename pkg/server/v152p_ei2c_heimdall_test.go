package server

// Batch 2c item 7 (research only, untracked; same text on both builds):
// report entry 23 end to end. The assistant's chat endpoint
// (heimdall.Handler, /api/bifrost/chat/completions) with the real in-process
// MCP tools (mcpToolRunnerAdapter over mcp.NewServer on a real database, as
// server.go wires it) and a scripted OpenAI-compatible model on a loopback HTTP
// server (provider "openai"): the model calls the real `store` tool with the
// two integers in metadata, then answers "done". The stored node is then read
// back with Cypher.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestZZEI2cHeimdallStoreIntegers(t *testing.T) {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var calls atomic.Int32
	var requests []string
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, r.URL.Path)
		n := calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			args := `{"content":"x","metadata":{"external_id":9007199254740993,"ledger":1234567890123456789}}`
			resp := map[string]any{"id": "c1", "object": "chat.completion", "model": "test",
				"choices": []any{map[string]any{"index": 0, "finish_reason": "tool_calls", "message": map[string]any{"role": "assistant", "content": "",
					"tool_calls": []any{map[string]any{"id": "t1", "type": "function", "function": map[string]any{"name": "store", "arguments": args}}}}}}}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		_ = body
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "c2", "object": "chat.completion", "model": "test",
			"choices": []any{map[string]any{"index": 0, "finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": "done"}}}})
	}))
	t.Cleanup(model.Close)

	hcfg := heimdall.DefaultConfig()
	hcfg.Enabled = true
	hcfg.BifrostEnabled = true
	hcfg.Provider = "openai"
	hcfg.APIURL = model.URL
	hcfg.APIKey = "probe-key"
	hcfg.Model = "test"
	manager, err := heimdall.NewManager(hcfg)
	require.NoError(t, err)
	t.Logf("ZZEI2c H manager supports tools=%v", manager.SupportsTools())
	handler := heimdall.NewHandler(manager, hcfg, newHeimdallDBRouter(db, nil, &nornicConfig.FeatureFlagsConfig{}), &heimdallMetricsReader{})
	handler.SetInMemoryToolRunner(&mcpToolRunnerAdapter{s: mcp.NewServer(db, nil), allowlist: []string{"store"}})

	reqBody := `{"model":"test","messages":[{"role":"user","content":"store this"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	out := rec.Body.String()
	if len(out) > 400 {
		out = out[:400]
	}
	t.Logf("ZZEI2c H chat status=%d model calls=%d paths=%v body=%s", rec.Code, calls.Load(), requests, out)

	res, err := db.ExecuteCypher(context.Background(), "MATCH (n) WHERE n.content = 'x' RETURN n.external_id AS e, n.ledger AS l, toString(n.external_id) AS es, toString(n.ledger) AS ls, labels(n) AS labels", nil)
	require.NoError(t, err)
	t.Logf("ZZEI2c H stored rows=%d", len(res.Rows))
	for _, row := range res.Rows {
		var parts []string
		for _, v := range row {
			parts = append(parts, fmt.Sprintf("%T %v", v, v))
		}
		t.Logf("ZZEI2c H stored external_id/ledger: %s (expected 9007199254740993 and 1234567890123456789)", strings.Join(parts, " | "))
	}
}
