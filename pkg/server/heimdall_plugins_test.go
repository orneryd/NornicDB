package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	goplugin "plugin"
	"runtime"
	"strings"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/mcp"
	watcher "github.com/orneryd/nornicdb/plugins/heimdall"
	"github.com/stretchr/testify/require"
)

// This integration test compiles a real plugin without nolocalllm. Run the host
// tests with -tags nolocalllm to assert loading across the host backend setting.
func TestHeimdallPluginOnlyStartup(t *testing.T) { testHeimdallPluginStartup(t, false) }

func TestHeimdallPluginFailedStart(t *testing.T) { testHeimdallPluginStartup(t, true) }

func testHeimdallPluginStartup(t *testing.T, failStart bool) {
	t.Helper()
	if !pluginTestCGOBuild {
		t.Skip("Go plugins require CGO")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Go plugins require Linux or macOS")
	}
	goBinary, err := exec.LookPath("go")
	require.NoError(t, err)
	dir := t.TempDir()
	// Unique source identity isolates Go's process-global plugin cache across
	// repeated test runs. Its value is controlled and asserted, not time based.
	source := fmt.Sprintf(`package main
import (
 "fmt"
 "strings"
 "github.com/orneryd/nornicdb/pkg/heimdall"
 watcher "github.com/orneryd/nornicdb/plugins/heimdall"
)
const identity = %q
const failStart = %t
type testPlugin struct { watcher.WatcherPlugin; ctx heimdall.SubsystemContext; starts int; stops int; shutdowns int }
func (*testPlugin) Name() string { return "plugin_only_test" }
func (p *testPlugin) Initialize(ctx heimdall.SubsystemContext) error { p.ctx=ctx; return p.WatcherPlugin.Initialize(ctx) }
func (p *testPlugin) Start() error { p.starts++; if failStart { return fmt.Errorf("requested start failure") }; return nil }
func (p *testPlugin) Stop() error { p.stops++; return nil }
func (p *testPlugin) Shutdown() error { p.shutdowns++; return nil }
func (p *testPlugin) Metrics() map[string]interface{} { return map[string]interface{}{"starts":p.starts,"stops":p.stops,"shutdowns":p.shutdowns} }
func (p *testPlugin) Actions() map[string]heimdall.ActionFunc {
 return map[string]heimdall.ActionFunc{"probe":{External:true, Handler:func(ctx heimdall.ActionContext)(*heimdall.ActionResult,error){
 inference,err:=p.ctx.Heimdall.SendRawPrompt(ctx,"no model should be loaded")
 if err!=nil { return nil,err }
 return &heimdall.ActionResult{Success:true,Data:map[string]interface{}{"identity":identity,"enabled":p.ctx.Config.Enabled,"bifrost":p.ctx.Config.BifrostEnabled,"starts":p.starts,"inference":inference.Success,"message":inference.Message,"database":ctx.Params["database"],"api_key_empty":p.ctx.Config.APIKey=="","api_url_credentials_hidden":!strings.Contains(p.ctx.Config.APIURL,"synthetic-user") && !strings.Contains(p.ctx.Config.APIURL,"synthetic-pass") && !strings.Contains(p.ctx.Config.APIURL,"synthetic-query-token")}},nil
 }}}
}
var Plugin heimdall.HeimdallPlugin = &testPlugin{}
`, filepath.Base(dir), failStart)
	sourcePath := filepath.Join(dir, "plugin.go")
	require.NoError(t, os.WriteFile(sourcePath, []byte(source), 0600))
	buildCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	args := []string{"build", "-buildmode=plugin", "-o", filepath.Join(dir, "probe.so")}
	if pluginTestRaceBuild {
		args = append(args, "-race")
	}
	args = append(args, sourcePath)
	cmd := exec.CommandContext(buildCtx, goBinary, args...)
	cmd.Env = append(os.Environ(), "GOFLAGS=", "GOPROXY=off")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "plugin build: %s", output)
	heimdall.ResetSubsystemManagerForTests()
	t.Cleanup(heimdall.ResetSubsystemManagerForTests)
	loaderCalls := 0
	previous := heimdall.SetGeneratorLoader(func(string, int, int, int, int) (heimdall.Generator, error) {
		loaderCalls++
		return nil, fmt.Errorf("unexpected model load")
	})
	defer heimdall.SetGeneratorLoader(previous)
	s, _ := setupTestServerWithConfig(t, func(cfg *Config) {
		cfg.PluginsDir = ""
		cfg.HeimdallPluginsDir = dir
		cfg.Features = &nornicConfig.FeatureFlagsConfig{
			HeimdallEnabled: false,
			HeimdallAPIURL:  "https://synthetic-user:synthetic-pass@provider.invalid/v1?access_token=synthetic-query-token",
			HeimdallAPIKey:  "synthetic-api-key",
		}
	})
	require.Zero(t, loaderCalls)
	require.Nil(t, s.getHeimdallHandler())
	require.NotNil(t, s.mcpServer)
	if failStart {
		_, registered := heimdall.GetSubsystemManager().GetPlugin("plugin_only_test")
		require.False(t, registered)
		loaded, err := goplugin.Open(filepath.Join(dir, "probe.so"))
		require.NoError(t, err)
		symbol, err := loaded.Lookup("Plugin")
		require.NoError(t, err)
		failedPlugin, ok := symbol.(*heimdall.HeimdallPlugin)
		require.True(t, ok)
		require.Equal(t, map[string]interface{}{"starts": 1, "stops": 0, "shutdowns": 1}, (*failedPlugin).Metrics())
		require.Empty(t, heimdall.GetSubsystemManager().ExternalActions())
		require.Equal(t, len(mcp.GetToolDefinitions()), len(s.mcpServer.ToolDefinitions()))
		_, err = s.mcpServer.CallTool(context.Background(), "heimdall_plugin_only_test_probe", nil)
		require.Error(t, err)
		return
	}
	require.True(t, heimdall.HeimdallPluginsInitialized())
	// Native server route provides authentication and the normal MCP public path.
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"heimdall_plugin_only_test_probe","arguments":{}}}`))
	request.Header.Set("Authorization", "Bearer "+getAuthToken(t, s.auth, "admin"))
	response := httptest.NewRecorder()
	s.buildRouter().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var envelope struct {
		Result mcp.CallToolResponse `json:"result"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
	require.False(t, envelope.Result.IsError, response.Body.String())
	require.Len(t, envelope.Result.Content, 1)
	var result heimdall.ActionResult
	require.NoError(t, json.Unmarshal([]byte(envelope.Result.Content[0].Text), &result))
	require.True(t, result.Success)
	require.Equal(t, map[string]interface{}{"identity": filepath.Base(dir), "enabled": false, "bifrost": false, "starts": float64(1), "inference": false, "message": "SLM not available", "database": s.mcpServer.DefaultDatabaseName(), "api_key_empty": true, "api_url_credentials_hidden": true}, result.Data)
	plugin, ok := heimdall.GetSubsystemManager().GetPlugin("plugin_only_test")
	require.True(t, ok)
	require.NoError(t, s.Stop(context.Background()))
	require.NoError(t, s.Stop(context.Background()))
	require.Equal(t, map[string]interface{}{"starts": 1, "stops": 1, "shutdowns": 1}, plugin.Metrics())
	require.Empty(t, heimdall.GetSubsystemManager().ExternalActions())
	require.Len(t, s.mcpServer.ToolDefinitions(), len(mcp.GetToolDefinitions()))
	_, err = s.mcpServer.CallTool(context.Background(), "heimdall_plugin_only_test_probe", nil)
	require.Error(t, err)
	require.Zero(t, loaderCalls)
}

type failingCleanupPlugin struct {
	watcher.WatcherPlugin
	stops, shutdowns     int
	stopErr, shutdownErr error
}

func (*failingCleanupPlugin) Name() string      { return "cleanup_failure_test" }
func (p *failingCleanupPlugin) Stop() error     { p.stops++; return p.stopErr }
func (p *failingCleanupPlugin) Shutdown() error { p.shutdowns++; return p.shutdownErr }

func TestPluginCleanupContinuesAfterStopFailure(t *testing.T) {
	heimdall.ResetSubsystemManagerForTests()
	t.Cleanup(heimdall.ResetSubsystemManagerForTests)
	manager := heimdall.GetSubsystemManager()
	plugin := &failingCleanupPlugin{stopErr: errors.New("stop failed"), shutdownErr: errors.New("shutdown failed")}
	require.NoError(t, manager.RegisterPlugin(plugin, "", true))
	s := &Server{heimdallSubsystem: manager}
	err := s.stopHeimdallPlugins()
	require.ErrorIs(t, err, plugin.stopErr)
	require.ErrorIs(t, err, plugin.shutdownErr)
	require.Equal(t, 1, plugin.stops)
	require.Equal(t, 1, plugin.shutdowns)
	require.Empty(t, manager.ExternalActions())
	require.NoError(t, s.stopHeimdallPlugins())
	require.Equal(t, 1, plugin.stops)
	require.Equal(t, 1, plugin.shutdowns)
}
