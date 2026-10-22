package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/mcp"
	watcher "github.com/orneryd/nornicdb/plugins/heimdall"
	"github.com/stretchr/testify/require"
)

type mcpInferencePlugin struct {
	watcher.WatcherPlugin
	invoker heimdall.HeimdallInvoker
	seen    *heimdall.ActionContext
}

func (p *mcpInferencePlugin) Name() string { return "external_test" }
func (p *mcpInferencePlugin) Initialize(ctx heimdall.SubsystemContext) error {
	p.invoker = ctx.Heimdall
	return nil
}
func (p *mcpInferencePlugin) Actions() map[string]heimdall.ActionFunc {
	return map[string]heimdall.ActionFunc{
		"infer": {External: true, Description: "test inference", Handler: func(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
			p.seen = &ctx
			return p.invoker.SendRawPrompt(ctx, ctx.Params["prompt"].(string))
		}},
		"internal": {Handler: func(heimdall.ActionContext) (*heimdall.ActionResult, error) {
			return nil, errors.New("internal action executed")
		}},
		"nil": {External: true},
	}
}

type mcpInferenceGenerator struct{ calls []string }

func (g *mcpInferenceGenerator) Generate(ctx context.Context, prompt string, _ heimdall.GenerateParams) (string, error) {
	g.calls = append(g.calls, prompt)
	return "inference answer", nil
}
func (*mcpInferenceGenerator) GenerateStream(context.Context, string, heimdall.GenerateParams, func(string) error) error {
	return errors.New("unexpected streaming call")
}
func (*mcpInferenceGenerator) Close() error      { return nil }
func (*mcpInferenceGenerator) ModelPath() string { return "test-model" }

func TestHeimdallMCPExposureAndInference(t *testing.T) {
	heimdall.ResetSubsystemManagerForTests()
	t.Cleanup(heimdall.ResetSubsystemManagerForTests)
	manager := heimdall.GetSubsystemManager()
	generator := &mcpInferenceGenerator{}
	invoker := heimdall.NewLiveHeimdallInvoker(manager, generator, nil, nil, nil)
	manager.SetContext(heimdall.SubsystemContext{Heimdall: invoker})
	plugin := &mcpInferencePlugin{}
	require.NoError(t, manager.RegisterPlugin(plugin, "", true))
	server := mcp.NewServer(nil, nil)
	registerHeimdallMCPTools(nil, manager, nil, nil, nil)
	registerHeimdallMCPTools(server, manager, nil, nil, nil)
	definitions := server.ToolDefinitions()
	require.Len(t, definitions, len(mcp.GetToolDefinitions())+2)
	require.Equal(t, "heimdall_external_test_infer", definitions[len(definitions)-2].Name)
	require.JSONEq(t, string(heimdall.DefaultActionInputSchema), string(definitions[len(definitions)-2].InputSchema))
	// The internal model still sees both public and private actions.
	names := map[string]bool{}
	for _, tool := range heimdall.ActionsAsMCPTools() {
		names[tool.Name] = true
	}
	require.True(t, names["heimdall_external_test_internal"])
	require.True(t, names["heimdall_external_test_infer"])
	roles := []string{"plugin-caller"}
	ctx := auth.WithRequestPrincipalRoles(context.Background(), roles)
	request := httptest.NewRequest(http.MethodPost, "/mcp/tenant", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"heimdall_external_test_infer","arguments":{"prompt":"summarize this"}}}`)).WithContext(ctx)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	var envelope struct {
		Result mcp.CallToolResponse `json:"result"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
	require.False(t, envelope.Result.IsError)
	require.Len(t, envelope.Result.Content, 1)
	var result heimdall.ActionResult
	require.NoError(t, json.Unmarshal([]byte(envelope.Result.Content[0].Text), &result))
	require.Equal(t, heimdall.ActionResult{Success: true, Message: "inference answer"}, result)
	require.Equal(t, []string{"summarize this"}, generator.calls)
	require.NotNil(t, plugin.seen)
	require.Equal(t, roles, plugin.seen.PrincipalRoles)
	require.Equal(t, "tenant", plugin.seen.Params["database"])
	require.Equal(t, "tenant", mcp.DatabaseFromContext(plugin.seen.Context))
	_, err := server.CallTool(context.Background(), "heimdall_external_test_internal", nil)
	require.Error(t, err)
	_, err = server.CallTool(context.Background(), "heimdall_external_test_nil", nil)
	require.EqualError(t, err, "action heimdall_external_test_nil has no handler")
	// Default plugin actions never opt into exposure.
	for name, action := range (&watcher.WatcherPlugin{}).Actions() {
		require.False(t, action.External, name)
	}
}

func TestHeimdallMCPShutdownRemovesTools(t *testing.T) {
	heimdall.ResetSubsystemManagerForTests()
	t.Cleanup(heimdall.ResetSubsystemManagerForTests)
	manager := heimdall.GetSubsystemManager()
	plugin := &mcpInferencePlugin{}
	require.NoError(t, manager.RegisterPlugin(plugin, "", true))
	server := mcp.NewServer(nil, nil)
	registerHeimdallMCPTools(server, manager, nil, nil, nil)
	require.NoError(t, manager.ShutdownAll())
	require.Len(t, server.ToolDefinitions(), len(mcp.GetToolDefinitions()))
	_, err := server.CallTool(context.Background(), "heimdall_external_test_infer", nil)
	require.Error(t, err)
	require.Empty(t, manager.ExternalActions())
	require.NoError(t, manager.RegisterPlugin(&mcpInferencePlugin{}, "", true))
	registerHeimdallMCPTools(server, manager, nil, nil, nil)
	require.Len(t, server.ToolDefinitions(), len(mcp.GetToolDefinitions())+2)
	require.Len(t, manager.ExternalActions(), 2, "reload must not duplicate FIFO entries")
}

func TestHeimdallMCPShutdownKeepsCollisionWinner(t *testing.T) {
	heimdall.ResetSubsystemManagerForTests()
	t.Cleanup(heimdall.ResetSubsystemManagerForTests)
	manager := heimdall.GetSubsystemManager()
	require.NoError(t, manager.RegisterPlugin(&mcpInferencePlugin{}, "", true))
	server := mcp.NewServer(nil, nil)
	const name = "heimdall_external_test_infer"
	require.NoError(t, server.RegisterTool(mcp.Tool{Name: name}, func(context.Context, map[string]interface{}) (interface{}, error) { return "original", nil }))
	registerHeimdallMCPTools(server, manager, nil, nil, nil)
	require.NoError(t, manager.ShutdownAll())
	result, err := server.CallTool(context.Background(), name, nil)
	require.NoError(t, err)
	require.Equal(t, "original", result)
	require.Equal(t, len(mcp.GetToolDefinitions())+1, len(server.ToolDefinitions()))
}
