package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/stretchr/testify/require"
)

// This integration test compiles a real plugin without nolocalllm. Run the host
// tests with -tags nolocalllm to assert loading across the host backend setting.
func TestHeimdallPluginOnlyStartup(t *testing.T) {
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
 "github.com/orneryd/nornicdb/pkg/heimdall"
 watcher "github.com/orneryd/nornicdb/plugins/heimdall"
)
const identity = %q
type testPlugin struct { watcher.WatcherPlugin; ctx heimdall.SubsystemContext; starts int }
func (*testPlugin) Name() string { return "plugin_only_test" }
func (p *testPlugin) Initialize(ctx heimdall.SubsystemContext) error { p.ctx=ctx; return p.WatcherPlugin.Initialize(ctx) }
func (p *testPlugin) Start() error { p.starts++; return nil }
func (p *testPlugin) Actions() map[string]heimdall.ActionFunc {
 return map[string]heimdall.ActionFunc{"probe":{External:true, Handler:func(ctx heimdall.ActionContext)(*heimdall.ActionResult,error){
 inference,err:=p.ctx.Heimdall.SendRawPrompt(ctx,"no model should be loaded")
 if err!=nil { return nil,err }
 return &heimdall.ActionResult{Success:true,Data:map[string]interface{}{"identity":identity,"enabled":p.ctx.Config.Enabled,"bifrost":p.ctx.Config.BifrostEnabled,"starts":p.starts,"inference":inference.Success,"message":inference.Message,"database":ctx.Params["database"]}},nil
 }}}
}
var Plugin heimdall.HeimdallPlugin = &testPlugin{}
`, filepath.Base(dir))
	sourcePath := filepath.Join(dir, "plugin.go")
	require.NoError(t, os.WriteFile(sourcePath, []byte(source), 0600))
	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
		cfg.Features = &nornicConfig.FeatureFlagsConfig{HeimdallEnabled: false}
	})
	require.Zero(t, loaderCalls)
	require.Nil(t, s.getHeimdallHandler())
	require.NotNil(t, s.mcpServer)
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
	require.Equal(t, map[string]interface{}{"identity": filepath.Base(dir), "enabled": false, "bifrost": false, "starts": float64(1), "inference": false, "message": "SLM not available", "database": s.mcpServer.DefaultDatabaseName()}, result.Data)
	require.Zero(t, loaderCalls)
}
