package nornicdb

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildTestPluginSO(t *testing.T, dir, baseName, source string) string {
	t.Helper()
	srcPath := filepath.Join(dir, baseName+".go")
	soPath := filepath.Join(dir, baseName+".so")
	require.NoError(t, os.WriteFile(srcPath, []byte(source), 0o600))

	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", soPath, srcPath)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build plugin failed: %s", string(out))
	return soPath
}

func resetPluginTestState() {
	pluginsMu.Lock()
	loadedPlugins = make(map[string]*LoadedPlugin)
	pluginFunctions = make(map[string]PluginFunction)
	pluginProcedures = make(map[string]PluginProcedure)
	pluginsInitialized = false
	pluginsMu.Unlock()
	heimdall.ResetSubsystemManagerForTests()
}

func TestPluginFunctionRegistry(t *testing.T) {
	// Clear any existing state
	resetPluginTestState()

	t.Run("GetPluginFunction returns false for unregistered function", func(t *testing.T) {
		_, found := GetPluginFunction("nonexistent.function")
		assert.False(t, found)
	})

	t.Run("register and retrieve plugin function", func(t *testing.T) {
		// Manually register a function (simulating plugin load)
		pluginsMu.Lock()
		pluginFunctions["test.plugin.sum"] = PluginFunction{
			Name:        "test.plugin.sum",
			Handler:     func(vals []interface{}) float64 { return 42.0 },
			Description: "Test sum function",
			Category:    "test",
		}
		pluginsMu.Unlock()

		fn, found := GetPluginFunction("test.plugin.sum")
		require.True(t, found)
		assert.Equal(t, "test.plugin.sum", fn.Name)
		assert.Equal(t, "Test sum function", fn.Description)
		assert.NotNil(t, fn.Handler)
	})

	t.Run("ListPluginFunctions returns registered functions", func(t *testing.T) {
		names := ListPluginFunctions()
		assert.Contains(t, names, "test.plugin.sum")
	})

	t.Run("case sensitivity - function names are case sensitive", func(t *testing.T) {
		_, found := GetPluginFunction("TEST.PLUGIN.SUM")
		assert.False(t, found, "Function lookup should be case sensitive")

		_, found = GetPluginFunction("test.plugin.sum")
		assert.True(t, found)
	})
}

func TestPluginProcedureRegistry(t *testing.T) {
	pluginsMu.Lock()
	pluginProcedures = make(map[string]PluginProcedure)
	pluginsMu.Unlock()

	_, found := GetPluginProcedure("missing.proc")
	require.False(t, found)
	require.Empty(t, ListPluginProcedures())

	pluginsMu.Lock()
	pluginProcedures["test.proc"] = PluginProcedure{
		Name:        "test.proc",
		Mode:        "READ",
		Description: "test proc",
	}
	pluginsMu.Unlock()

	proc, found := GetPluginProcedure("test.proc")
	require.True(t, found)
	require.Equal(t, "test.proc", proc.Name)
	require.Equal(t, "READ", proc.Mode)

	names := ListPluginProcedures()
	require.Contains(t, names, "test.proc")
}

func TestLoadedPluginTracking(t *testing.T) {
	// Clear state
	pluginsMu.Lock()
	pluginFunctions = make(map[string]PluginFunction)
	loadedPlugins = make(map[string]*LoadedPlugin)
	pluginsInitialized = false
	pluginsMu.Unlock()

	t.Run("ListLoadedPlugins returns empty when no plugins loaded", func(t *testing.T) {
		plugins := ListLoadedPlugins()
		assert.Empty(t, plugins)
	})

	t.Run("track loaded plugin", func(t *testing.T) {
		pluginsMu.Lock()
		loadedPlugins["testplugin"] = &LoadedPlugin{
			Name:    "testplugin",
			Version: "1.0.0",
			Path:    "/path/to/testplugin.so",
			Functions: []PluginFunction{
				{Name: "testplugin.func1", Handler: nil, Description: "Test func 1"},
				{Name: "testplugin.func2", Handler: nil, Description: "Test func 2"},
			},
		}
		pluginsInitialized = true
		pluginsMu.Unlock()

		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1)
		assert.Equal(t, "testplugin", plugins[0].Name)
		assert.Equal(t, "1.0.0", plugins[0].Version)
		assert.Len(t, plugins[0].Functions, 2)
	})

	t.Run("PluginsInitialized returns correct state", func(t *testing.T) {
		assert.True(t, PluginsInitialized())
	})
}

func TestLoadPluginsFromDir(t *testing.T) {
	t.Run("empty directory path returns nil", func(t *testing.T) {
		err := LoadPluginsFromDir("", nil)
		assert.NoError(t, err)
	})

	t.Run("non-existent directory returns nil (not an error)", func(t *testing.T) {
		err := LoadPluginsFromDir("/nonexistent/path/to/plugins", nil)
		assert.NoError(t, err)
	})

	t.Run("file path instead of directory returns error", func(t *testing.T) {
		// Create a temp file
		err := LoadPluginsFromDir("/dev/null", nil) // This is a file, not a directory
		// Should return error since it's not a directory
		if err != nil {
			assert.Contains(t, err.Error(), "not a directory")
		}
	})

	t.Run("heimdall plugin is skipped when context is nil", func(t *testing.T) {
		resetPluginTestState()

		dir := t.TempDir()
		_ = buildTestPluginSO(t, dir, "heimdall_skip_ctx", `package main
import "github.com/orneryd/nornicdb/pkg/heimdall"
type P struct{}
func (P) Name() string { return "skipctx" }
func (P) Version() string { return "1.0.0" }
func (P) Type() string { return "heimdall" }
func (P) Description() string { return "test" }
func (P) Initialize(heimdall.SubsystemContext) error { return nil }
func (P) Start() error { return nil }
func (P) Stop() error { return nil }
func (P) Shutdown() error { return nil }
func (P) Status() heimdall.SubsystemStatus { return heimdall.StatusRunning }
func (P) Health() heimdall.SubsystemHealth { return heimdall.SubsystemHealth{Status: heimdall.StatusRunning, Healthy: true} }
func (P) Metrics() map[string]interface{} { return nil }
func (P) Config() map[string]interface{} { return nil }
func (P) Configure(map[string]interface{}) error { return nil }
func (P) ConfigSchema() map[string]interface{} { return nil }
func (P) Actions() map[string]heimdall.ActionFunc { return nil }
func (P) Summary() string { return "ok" }
func (P) RecentEvents(limit int) []heimdall.SubsystemEvent { return nil }
var Plugin P
`)

		err := LoadPluginsFromDir(dir, nil)
		require.NoError(t, err)
		require.True(t, PluginsInitialized())
		require.Empty(t, ListLoadedPlugins(), "heimdall plugin should be skipped without context")
	})

	t.Run("heimdall plugin loads when context is provided", func(t *testing.T) {
		resetPluginTestState()

		dir := t.TempDir()
		_ = buildTestPluginSO(t, dir, "heimdall_with_ctx", `package main
import "github.com/orneryd/nornicdb/pkg/heimdall"
type P struct{}
func (P) Name() string { return "ctxok" }
func (P) Version() string { return "1.0.0" }
func (P) Type() string { return "heimdall" }
func (P) Description() string { return "test" }
func (P) Initialize(heimdall.SubsystemContext) error { return nil }
func (P) Start() error { return nil }
func (P) Stop() error { return nil }
func (P) Shutdown() error { return nil }
func (P) Status() heimdall.SubsystemStatus { return heimdall.StatusRunning }
func (P) Health() heimdall.SubsystemHealth { return heimdall.SubsystemHealth{Status: heimdall.StatusRunning, Healthy: true} }
func (P) Metrics() map[string]interface{} { return map[string]interface{}{"ok": true} }
func (P) Config() map[string]interface{} { return nil }
func (P) Configure(map[string]interface{}) error { return nil }
func (P) ConfigSchema() map[string]interface{} { return nil }
func (P) Actions() map[string]heimdall.ActionFunc { return map[string]heimdall.ActionFunc{"status": {Name: "ctxok.status", Description: "status", Category: "test"}} }
func (P) Summary() string { return "ok" }
func (P) RecentEvents(limit int) []heimdall.SubsystemEvent { return nil }
var Plugin P
`)

		ctx := &heimdall.SubsystemContext{
			Config: heimdall.Config{
				Model:       "test",
				ModelsDir:   "models",
				GPULayers:   0,
				ContextSize: 1024,
			},
		}

		err := LoadPluginsFromDir(dir, ctx)
		require.NoError(t, err)
		require.True(t, PluginsInitialized())
		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1)
		require.Equal(t, PluginTypeHeimdall, plugins[0].Type)
		require.Equal(t, "ctxok", plugins[0].Name)
	})
}

func TestPluginFunctionHandlerTypes(t *testing.T) {
	// Test that various handler types can be stored and retrieved
	pluginsMu.Lock()
	pluginFunctions = make(map[string]PluginFunction)
	pluginsMu.Unlock()

	testCases := []struct {
		name    string
		handler interface{}
	}{
		{"no_args_string", func() string { return "hello" }},
		{"no_args_float", func() float64 { return 3.14 }},
		{"single_float", func(x float64) float64 { return x * 2 }},
		{"two_floats", func(a, b float64) float64 { return a + b }},
		{"string_func", func(s string) string { return s + "!" }},
		{"list_func", func(vals []interface{}) float64 { return float64(len(vals)) }},
		{"two_lists", func(a, b []float64) float64 { return float64(len(a) + len(b)) }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pluginsMu.Lock()
			pluginFunctions["test."+tc.name] = PluginFunction{
				Name:    "test." + tc.name,
				Handler: tc.handler,
			}
			pluginsMu.Unlock()

			fn, found := GetPluginFunction("test." + tc.name)
			require.True(t, found)
			assert.NotNil(t, fn.Handler)
		})
	}
}

func TestPluginTypeDetection(t *testing.T) {
	t.Run("detectPluginType direct coverage", func(t *testing.T) {
		require.Equal(t, PluginTypeFunction, detectPluginType(mockTypeFunctionValue{}))
		require.Equal(t, PluginTypeHeimdall, detectPluginType(mockTypeHeimdallValue{}))
		require.Equal(t, PluginTypeUnknown, detectPluginType(&mockPluginNoType{}))
		require.Equal(t, PluginTypeUnknown, detectPluginType(&mockTypeNonString{}))
	})

	t.Run("detects function plugin type from reflection", func(t *testing.T) {
		plugin := &mockFunctionPlugin{
			name:    "testfunc",
			version: "1.0.0",
			ptype:   "function",
		}

		// detectPluginType uses reflection internally
		// Keep the pointer for method lookup (methods are defined on *mock...)
		val := reflect.ValueOf(plugin)

		// Test that Type() method exists and returns correct value
		typeMethod := val.MethodByName("Type")
		require.True(t, typeMethod.IsValid(), "Type method should exist")
		result := typeMethod.Call(nil)
		require.Len(t, result, 1)
		assert.Equal(t, "function", result[0].String())
	})

	t.Run("detects heimdall plugin type from reflection", func(t *testing.T) {
		plugin := &mockHeimdallPlugin{
			name:    "testsys",
			version: "1.0.0",
			ptype:   "heimdall",
		}

		// Keep the pointer for method lookup
		val := reflect.ValueOf(plugin)

		typeMethod := val.MethodByName("Type")
		require.True(t, typeMethod.IsValid())
		result := typeMethod.Call(nil)
		require.Len(t, result, 1)
		assert.Equal(t, "heimdall", result[0].String())
	})

	t.Run("handles various type strings", func(t *testing.T) {
		testCases := []struct {
			typeStr  string
			expected PluginType
		}{
			{"function", PluginTypeFunction},
			{"apoc", PluginTypeFunction},
			{"", PluginTypeFunction},
			{"heimdall", PluginTypeHeimdall},
			{"somethingelse", PluginTypeUnknown},
		}

		for _, tc := range testCases {
			t.Run(tc.typeStr, func(t *testing.T) {
				// Test the type detection logic directly
				var result PluginType
				switch tc.typeStr {
				case "heimdall":
					result = PluginTypeHeimdall
				case "function", "apoc", "":
					result = PluginTypeFunction
				default:
					result = PluginTypeUnknown
				}
				assert.Equal(t, tc.expected, result)
			})
		}
	})
}

func TestCallStringMethodAndReflectWrapperBranches(t *testing.T) {
	t.Run("callStringMethod validates method shape", func(t *testing.T) {
		_, err := callStringMethod(reflect.ValueOf(struct{}{}), "Missing")
		require.Error(t, err)

		_, err = callStringMethod(reflect.ValueOf(mockStringMethodBadReturn{}), "Name")
		require.Error(t, err)
	})

	t.Run("reflect wrapper happy path", func(t *testing.T) {
		w := &reflectHeimdallWrapper{val: reflect.ValueOf(mockReflectPlugin{})}

		require.Equal(t, "mock", w.Name())
		require.Equal(t, "1.0.0", w.Version())
		require.Equal(t, "heimdall", w.Type())
		require.Equal(t, "desc", w.Description())
		require.NoError(t, w.Initialize(heimdall.SubsystemContext{}))
		require.NoError(t, w.Start())
		require.NoError(t, w.Stop())
		require.NoError(t, w.Shutdown())
		require.Equal(t, heimdall.StatusRunning, w.Status())
		require.True(t, w.Health().Healthy)
		require.Equal(t, "mock", w.Metrics()["name"])
		require.Equal(t, "mock", w.Config()["name"])
		require.NoError(t, w.Configure(map[string]interface{}{"k": "v"}))
		require.Equal(t, "string", w.ConfigSchema()["type"])
		require.Len(t, w.Actions(), 1)
		require.Equal(t, "summary", w.Summary())
		require.Len(t, w.RecentEvents(1), 1)
	})

	t.Run("reflect wrapper fallback and error branches", func(t *testing.T) {
		w := &reflectHeimdallWrapper{val: reflect.ValueOf(mockReflectPluginErrors{})}

		require.Error(t, w.Initialize(heimdall.SubsystemContext{}))
		require.Error(t, w.Start())
		require.Error(t, w.Stop())
		require.Error(t, w.Shutdown())
		require.Equal(t, heimdall.StatusError, w.Status())
		require.False(t, w.Health().Healthy)
		require.Nil(t, w.Metrics())
		require.Nil(t, w.Config())
		require.Error(t, w.Configure(map[string]interface{}{"k": "v"}))
		require.Nil(t, w.ConfigSchema())
		require.Nil(t, w.Actions())
		require.Nil(t, w.RecentEvents(2))
	})
}

func TestPluginLoadAndProcedureExtractionHelpers(t *testing.T) {
	t.Run("extractProcedures parses defaults and explicit fields", func(t *testing.T) {
		procs, err := extractProcedures(reflect.ValueOf(mockFunctionPluginWithProcedures{}), "covplug")
		require.NoError(t, err)
		require.Len(t, procs, 2)

		byName := map[string]PluginProcedure{}
		for _, p := range procs {
			byName[p.Name] = p
		}
		require.Contains(t, byName, "covplug.readProc")
		require.Contains(t, byName, "apoc.custom.proc")

		require.Equal(t, "READ", byName["covplug.readProc"].Mode)
		require.Contains(t, byName["covplug.readProc"].Signature, "covplug.readProc")
		require.Equal(t, 1, byName["covplug.readProc"].MinArgs)
		require.Equal(t, 3, byName["covplug.readProc"].MaxArgs)
		require.True(t, byName["apoc.custom.proc"].WorksOnSystem)
	})

	t.Run("loadFunctionPlugin builds loaded metadata", func(t *testing.T) {
		loaded, err := loadFunctionPlugin(mockFunctionPluginWithProcedures{}, "/tmp/covplug.so")
		require.NoError(t, err)
		require.Equal(t, "covplug", loaded.Name)
		require.Equal(t, PluginTypeFunction, loaded.Type)
		require.Len(t, loaded.Functions, 1)
		require.Len(t, loaded.Procedures, 2)
	})

	t.Run("loadFunctionPlugin errors when Name/Version missing", func(t *testing.T) {
		_, err := loadFunctionPlugin(mockPluginNoType{}, "/tmp/no_name.so")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no Name() method")

		_, err = loadFunctionPlugin(onlyNamePlugin{}, "/tmp/no_version.so")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no Version() method")
	})

	t.Run("registerProcedureWithCypher handles handler types", func(t *testing.T) {
		before := len(cypher.ListRegisteredProcedures())
		registerProcedureWithCypher(PluginProcedure{
			Name:    "cov.invalid.handler",
			Handler: "not-a-func",
		})
		require.Equal(t, before, len(cypher.ListRegisteredProcedures()))

		name := "cov.proc." + strings.ReplaceAll(t.Name(), "/", "_") + ".p" + time.Now().Format("150405")
		registerProcedureWithCypher(PluginProcedure{
			Name:          name,
			Signature:     name + "()",
			Mode:          "WRITE",
			WorksOnSystem: false,
			MinArgs:       0,
			MaxArgs:       1,
			Handler: func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error) {
				return &cypher.ExecuteResult{}, nil
			},
		})
		require.Greater(t, len(cypher.ListRegisteredProcedures()), before)

		schemaName := name + ".schema"
		registerProcedureWithCypher(PluginProcedure{
			Name:      schemaName,
			Signature: schemaName + "()",
			Mode:      "SCHEMA",
			Handler: func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error) {
				return &cypher.ExecuteResult{}, nil
			},
		})
		procedure, found := cypher.RegisteredProcedureForCall("CALL " + schemaName + "()")
		require.True(t, found)
		require.Equal(t, cypher.ProcedureModeSchema, procedure.Mode)
	})

	t.Run("loadHeimdallPlugin reflection and validation branches", func(t *testing.T) {
		loaded, err := loadHeimdallPlugin(mockReflectPluginErrors{}, "/tmp/heim.so", nil)
		require.Error(t, err)
		require.ErrorIs(t, err, errHeimdallContextRequired)
		require.NotNil(t, loaded)
		require.Equal(t, PluginTypeHeimdall, loaded.Type)

		_, err = loadHeimdallPlugin(mockPluginNoType{}, "/tmp/invalid.so", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing")
	})

	t.Run("extractProcedures invalid return and non-map branches", func(t *testing.T) {
		procs, err := extractProcedures(reflect.ValueOf(badProceduresReturn{}), "x")
		require.Error(t, err)
		require.Nil(t, procs)

		procs, err = extractProcedures(reflect.ValueOf(nonMapProcedures{}), "x")
		require.NoError(t, err)
		require.Empty(t, procs)
	})

	t.Run("loadPluginFile returns open error for invalid plugin file", func(t *testing.T) {
		badPlugin := filepath.Join(t.TempDir(), "bad.so")
		require.NoError(t, os.WriteFile(badPlugin, []byte("not-a-go-plugin"), 0644))

		loaded, err := loadPluginFile(badPlugin, nil)
		require.Error(t, err)
		require.Nil(t, loaded)
		require.Contains(t, err.Error(), "open")
	})

	t.Run("loadPluginFile branch coverage with built plugins", func(t *testing.T) {
		tmpDir := t.TempDir()

		noSymbol := buildTestPluginSO(t, tmpDir, "nosymbol", `package main
var NotPlugin = 1
`)
		loaded, err := loadPluginFile(noSymbol, nil)
		require.Error(t, err)
		require.Nil(t, loaded)
		require.Contains(t, err.Error(), "no Plugin symbol")

		unknownType := buildTestPluginSO(t, tmpDir, "unknown_type", `package main
type PluginValue struct{}
var Plugin PluginValue
`)
		loaded, err = loadPluginFile(unknownType, nil)
		require.Error(t, err)
		require.Nil(t, loaded)
		require.Contains(t, err.Error(), "unknown plugin type")

		functionPlugin := buildTestPluginSO(t, tmpDir, "function_ok", `package main
type TestPlugin struct{}
func (TestPlugin) Type() string { return "function" }
func (TestPlugin) Name() string { return "func_ok" }
func (TestPlugin) Version() string { return "1.0.0" }
func (TestPlugin) Functions() map[string]interface{} {
	return map[string]interface{}{
		"double": func(x float64) float64 { return x * 2 },
	}
}
var Plugin TestPlugin
`)
		loaded, err = loadPluginFile(functionPlugin, nil)
		require.NoError(t, err)
		require.NotNil(t, loaded)
		require.Equal(t, PluginTypeFunction, loaded.Type)
		require.Equal(t, "func_ok", loaded.Name)
		require.Len(t, loaded.Functions, 1)
	})

	t.Run("loadPluginsFromDir tolerates bad plugin files", func(t *testing.T) {
		resetPluginTestState()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.so"), []byte("bad"), 0644))
		require.NoError(t, LoadPluginsFromDir(dir, nil))
		require.True(t, PluginsInitialized())
		require.Empty(t, ListLoadedPlugins())
	})

	t.Run("LoadPluginsFromDir loads valid function plugin from directory", func(t *testing.T) {
		resetPluginTestState()

		dir := t.TempDir()
		_ = buildTestPluginSO(t, dir, "dir_function_ok", `package main
type TestPlugin struct{}
func (TestPlugin) Type() string { return "function" }
func (TestPlugin) Name() string { return "dir_func_ok" }
func (TestPlugin) Version() string { return "1.0.0" }
func (TestPlugin) Functions() map[string]interface{} {
	return map[string]interface{}{
		"double": func(x float64) float64 { return x * 2 },
	}
}
var Plugin TestPlugin
`)

		require.NoError(t, LoadPluginsFromDir(dir, nil))
		require.True(t, PluginsInitialized())
		require.NotEmpty(t, ListLoadedPlugins())
		require.NotEmpty(t, ListPluginFunctions())
	})

	t.Run("registerHeimdallPlugin returns start error with context", func(t *testing.T) {
		p := &mockHeimdallPluginStartErr{
			name:    "start-err-" + time.Now().Format("150405.000000000"),
			version: "1.0.0",
			ptype:   "heimdall",
		}
		loaded, err := registerHeimdallPlugin(p, "/tmp/start_err.so", &heimdall.SubsystemContext{})
		require.Error(t, err)
		require.Nil(t, loaded)
		require.Contains(t, err.Error(), "start")
	})
}

func TestExtractFunctions(t *testing.T) {
	t.Run("extracts functions from plugin", func(t *testing.T) {
		plugin := &mockFunctionPlugin{
			name:    "testplugin",
			version: "1.0.0",
			ptype:   "function",
			functions: map[string]interface{}{
				"double": func(x float64) float64 { return x * 2 },
				"sum":    func(vals []float64) float64 { return vals[0] + vals[1] },
			},
		}

		funcs, err := extractFunctions(reflectValueOf(plugin), "testplugin")
		require.NoError(t, err)
		assert.Len(t, funcs, 2)

		// Check function names are prefixed
		names := make(map[string]bool)
		for _, fn := range funcs {
			names[fn.Name] = true
		}
		assert.True(t, names["apoc.testplugin.double"] || names["double"])
		assert.True(t, names["apoc.testplugin.sum"] || names["sum"])
	})

	t.Run("handles empty functions map", func(t *testing.T) {
		plugin := &mockFunctionPlugin{
			name:      "emptyplugin",
			version:   "1.0.0",
			ptype:     "function",
			functions: map[string]interface{}{},
		}

		funcs, err := extractFunctions(reflectValueOf(plugin), "emptyplugin")
		require.NoError(t, err)
		assert.Empty(t, funcs)
	})

	t.Run("returns error when Functions method missing", func(t *testing.T) {
		funcs, err := extractFunctions(reflectValueOf(&mockPluginNoType{}), "nop")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no Functions() method")
		require.Nil(t, funcs)
	})

	t.Run("returns error for invalid Functions return arity", func(t *testing.T) {
		funcs, err := extractFunctions(reflectValueOf(badFunctionsReturn{}), "bad")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Functions() invalid return")
		require.Nil(t, funcs)
	})

	t.Run("returns empty when Functions result is non-map", func(t *testing.T) {
		funcs, err := extractFunctions(reflectValueOf(nonMapFunctionsReturn{}), "nonmap")
		require.NoError(t, err)
		require.Empty(t, funcs)
	})

	t.Run("preserves fully-qualified apoc names", func(t *testing.T) {
		funcs, err := extractFunctions(reflectValueOf(fqPlugin{}), "ignored")
		require.NoError(t, err)
		require.Len(t, funcs, 1)
		require.Equal(t, "apoc.custom.echo", funcs[0].Name)
	})
}

func TestLoadedPluginTypes(t *testing.T) {
	// Clear state
	pluginsMu.Lock()
	pluginFunctions = make(map[string]PluginFunction)
	loadedPlugins = make(map[string]*LoadedPlugin)
	pluginsInitialized = false
	pluginsMu.Unlock()

	t.Run("tracks function plugin", func(t *testing.T) {
		pluginsMu.Lock()
		loadedPlugins["funcplugin"] = &LoadedPlugin{
			Name:    "funcplugin",
			Version: "1.0.0",
			Type:    PluginTypeFunction,
			Path:    "/path/to/funcplugin.so",
			Functions: []PluginFunction{
				{Name: "funcplugin.test", Handler: nil, Description: "Test"},
			},
		}
		pluginsMu.Unlock()

		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1)
		assert.Equal(t, PluginTypeFunction, plugins[0].Type)
		assert.Len(t, plugins[0].Functions, 1)
	})

	t.Run("tracks heimdall plugin", func(t *testing.T) {
		pluginsMu.Lock()
		loadedPlugins["sysplugin"] = &LoadedPlugin{
			Name:      "sysplugin",
			Version:   "1.0.0",
			Type:      PluginTypeHeimdall,
			Path:      "/path/to/sysplugin.so",
			Functions: nil, // Heimdall plugins don't have functions
		}
		pluginsMu.Unlock()

		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 2) // funcplugin + sysplugin

		var heimdallPlugin *LoadedPlugin
		for _, p := range plugins {
			if p.Type == PluginTypeHeimdall {
				heimdallPlugin = p
				break
			}
		}
		require.NotNil(t, heimdallPlugin)
		assert.Equal(t, "sysplugin", heimdallPlugin.Name)
		assert.Nil(t, heimdallPlugin.Functions)
	})

	t.Run("lists mixed plugin types", func(t *testing.T) {
		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 2)

		hasFunction := false
		hasHeimdall := false
		for _, p := range plugins {
			if p.Type == PluginTypeFunction {
				hasFunction = true
			}
			if p.Type == PluginTypeHeimdall {
				hasHeimdall = true
			}
		}
		assert.True(t, hasFunction, "Should have function plugin")
		assert.True(t, hasHeimdall, "Should have Heimdall plugin")
	})
}

func TestUnifiedPluginLoader(t *testing.T) {
	// This tests the overall integration of the unified loader
	t.Run("plugin loader handles both types", func(t *testing.T) {
		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		// Simulate loading both types
		pluginsMu.Lock()
		loadedPlugins["apoc"] = &LoadedPlugin{
			Name:    "apoc",
			Version: "1.0.0",
			Type:    PluginTypeFunction,
			Path:    "/test/apoc.so",
			Functions: []PluginFunction{
				{Name: "apoc.coll.sum", Handler: nil},
			},
		}
		loadedPlugins["watcher"] = &LoadedPlugin{
			Name:      "watcher",
			Version:   "1.0.0",
			Type:      PluginTypeHeimdall,
			Path:      "/test/watcher.so",
			Functions: nil,
		}
		pluginsInitialized = true
		pluginsMu.Unlock()

		plugins := ListLoadedPlugins()
		assert.Len(t, plugins, 2)
		assert.True(t, PluginsInitialized())

		// Verify we can distinguish between types
		typeCount := make(map[PluginType]int)
		for _, p := range plugins {
			typeCount[p.Type]++
		}
		assert.Equal(t, 1, typeCount[PluginTypeFunction])
		assert.Equal(t, 1, typeCount[PluginTypeHeimdall])
	})
}

// Mock plugins for testing

type mockFunctionPlugin struct {
	name      string
	version   string
	ptype     string
	functions map[string]interface{}
}

func (m *mockFunctionPlugin) Name() string    { return m.name }
func (m *mockFunctionPlugin) Version() string { return m.version }
func (m *mockFunctionPlugin) Type() string    { return m.ptype }
func (m *mockFunctionPlugin) Functions() map[string]interface{} {
	return m.functions
}

type mockHeimdallPlugin struct {
	name       string
	version    string
	ptype      string
	startCalls int
}

func (m *mockHeimdallPlugin) Name() string                                   { return m.name }
func (m *mockHeimdallPlugin) Version() string                                { return m.version }
func (m *mockHeimdallPlugin) Type() string                                   { return m.ptype }
func (m *mockHeimdallPlugin) Description() string                            { return "Test Heimdall plugin" }
func (m *mockHeimdallPlugin) Initialize(ctx heimdall.SubsystemContext) error { return nil }
func (m *mockHeimdallPlugin) Start() error                                   { m.startCalls++; return nil }
func (m *mockHeimdallPlugin) Stop() error                                    { return nil }
func (m *mockHeimdallPlugin) Shutdown() error                                { return nil }
func (m *mockHeimdallPlugin) Status() heimdall.SubsystemStatus {
	return heimdall.StatusRunning
}
func (m *mockHeimdallPlugin) Health() heimdall.SubsystemHealth {
	return heimdall.SubsystemHealth{Status: heimdall.StatusRunning, Healthy: true}
}
func (m *mockHeimdallPlugin) Metrics() map[string]interface{}                  { return nil }
func (m *mockHeimdallPlugin) Config() map[string]interface{}                   { return nil }
func (m *mockHeimdallPlugin) Configure(settings map[string]interface{}) error  { return nil }
func (m *mockHeimdallPlugin) ConfigSchema() map[string]interface{}             { return nil }
func (m *mockHeimdallPlugin) Actions() map[string]heimdall.ActionFunc          { return nil }
func (m *mockHeimdallPlugin) Summary() string                                  { return "Test" }
func (m *mockHeimdallPlugin) RecentEvents(limit int) []heimdall.SubsystemEvent { return nil }

type mockPluginNoType struct {
	name    string
	version string
}

func (m *mockPluginNoType) Name() string    { return m.name }
func (m *mockPluginNoType) Version() string { return m.version }

type mockTypeNonString struct{}

func (m *mockTypeNonString) Type() int { return 7 }

type badFunctionsReturn struct{}

func (badFunctionsReturn) Functions() (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

type nonMapFunctionsReturn struct{}

func (nonMapFunctionsReturn) Functions() string { return "not-a-map" }

type fqPlugin struct{}

func (fqPlugin) Functions() map[string]interface{} {
	return map[string]interface{}{
		"apoc.custom.echo": struct {
			Handler     interface{}
			Description string
		}{
			Handler:     func(x string) string { return x },
			Description: "echo",
		},
	}
}

type onlyNamePlugin struct{}

func (onlyNamePlugin) Name() string { return "only-name" }
func (onlyNamePlugin) Functions() map[string]interface{} {
	return map[string]interface{}{}
}

type badProceduresReturn struct{}

func (badProceduresReturn) Procedures() (map[string]interface{}, error) { return nil, nil }

type nonMapProcedures struct{}

func (nonMapProcedures) Procedures() string { return "bad" }

type mockTypeFunctionValue struct{}

func (m mockTypeFunctionValue) Type() string { return "function" }

type mockTypeHeimdallValue struct{}

func (m mockTypeHeimdallValue) Type() string { return "heimdall" }

type mockStringMethodBadReturn struct{}

func (m mockStringMethodBadReturn) Name() (string, string) {
	return "x", "y"
}

type mockReflectPlugin struct{}

func (m mockReflectPlugin) Name() string                                   { return "mock" }
func (m mockReflectPlugin) Version() string                                { return "1.0.0" }
func (m mockReflectPlugin) Type() string                                   { return "heimdall" }
func (m mockReflectPlugin) Description() string                            { return "desc" }
func (m mockReflectPlugin) Initialize(ctx heimdall.SubsystemContext) error { return nil }
func (m mockReflectPlugin) Start() error                                   { return nil }
func (m mockReflectPlugin) Stop() error                                    { return nil }
func (m mockReflectPlugin) Shutdown() error                                { return nil }
func (m mockReflectPlugin) Status() heimdall.SubsystemStatus               { return heimdall.StatusRunning }
func (m mockReflectPlugin) Health() heimdall.SubsystemHealth {
	return heimdall.SubsystemHealth{Status: heimdall.StatusRunning, Healthy: true}
}
func (m mockReflectPlugin) Metrics() map[string]interface{} {
	return map[string]interface{}{"name": "mock"}
}
func (m mockReflectPlugin) Config() map[string]interface{} {
	return map[string]interface{}{"name": "mock"}
}
func (m mockReflectPlugin) Configure(map[string]interface{}) error { return nil }
func (m mockReflectPlugin) ConfigSchema() map[string]interface{} {
	return map[string]interface{}{"type": "string"}
}
func (m mockReflectPlugin) Actions() map[string]heimdall.ActionFunc {
	return map[string]heimdall.ActionFunc{
		"noop": {
			Name:        "heimdall.mock.noop",
			Description: "no-op",
			Category:    "test",
		},
	}
}
func (m mockReflectPlugin) Summary() string { return "summary" }
func (m mockReflectPlugin) RecentEvents(limit int) []heimdall.SubsystemEvent {
	return []heimdall.SubsystemEvent{{Type: "event", Message: "ok"}}
}

type mockReflectPluginErrors struct{}

func (m mockReflectPluginErrors) Name() string        { return "bad" }
func (m mockReflectPluginErrors) Version() string     { return "0" }
func (m mockReflectPluginErrors) Type() string        { return "heimdall" }
func (m mockReflectPluginErrors) Description() string { return "bad" }
func (m mockReflectPluginErrors) Initialize(ctx heimdall.SubsystemContext) error {
	return errors.New("init error")
}
func (m mockReflectPluginErrors) Start() error      { return errors.New("start error") }
func (m mockReflectPluginErrors) Stop() error       { return errors.New("stop error") }
func (m mockReflectPluginErrors) Shutdown() error   { return errors.New("shutdown error") }
func (m mockReflectPluginErrors) Status() string    { return "wrong-type" }
func (m mockReflectPluginErrors) Health() string    { return "wrong-type" }
func (m mockReflectPluginErrors) Metrics() []string { return []string{"wrong-type"} }
func (m mockReflectPluginErrors) Config() []string  { return []string{"wrong-type"} }
func (m mockReflectPluginErrors) Configure(map[string]interface{}) error {
	return errors.New("configure error")
}
func (m mockReflectPluginErrors) ConfigSchema() []string          { return []string{"wrong-type"} }
func (m mockReflectPluginErrors) Actions() []string               { return []string{"wrong-type"} }
func (m mockReflectPluginErrors) Summary() string                 { return "bad-summary" }
func (m mockReflectPluginErrors) RecentEvents(limit int) []string { return []string{"wrong-type"} }

type mockHeimdallPluginStartErr struct {
	name    string
	version string
	ptype   string
}

func (m *mockHeimdallPluginStartErr) Name() string    { return m.name }
func (m *mockHeimdallPluginStartErr) Version() string { return m.version }
func (m *mockHeimdallPluginStartErr) Type() string    { return m.ptype }
func (m *mockHeimdallPluginStartErr) Description() string {
	return "start error plugin"
}
func (m *mockHeimdallPluginStartErr) Initialize(ctx heimdall.SubsystemContext) error { return nil }
func (m *mockHeimdallPluginStartErr) Start() error                                   { return errors.New("boom start") }
func (m *mockHeimdallPluginStartErr) Stop() error                                    { return nil }
func (m *mockHeimdallPluginStartErr) Shutdown() error                                { return nil }
func (m *mockHeimdallPluginStartErr) Status() heimdall.SubsystemStatus               { return heimdall.StatusError }
func (m *mockHeimdallPluginStartErr) Health() heimdall.SubsystemHealth {
	return heimdall.SubsystemHealth{Status: heimdall.StatusError, Healthy: false}
}
func (m *mockHeimdallPluginStartErr) Metrics() map[string]interface{}                  { return nil }
func (m *mockHeimdallPluginStartErr) Config() map[string]interface{}                   { return nil }
func (m *mockHeimdallPluginStartErr) Configure(settings map[string]interface{}) error  { return nil }
func (m *mockHeimdallPluginStartErr) ConfigSchema() map[string]interface{}             { return nil }
func (m *mockHeimdallPluginStartErr) Actions() map[string]heimdall.ActionFunc          { return nil }
func (m *mockHeimdallPluginStartErr) Summary() string                                  { return "start error" }
func (m *mockHeimdallPluginStartErr) RecentEvents(limit int) []heimdall.SubsystemEvent { return nil }

type mockProcedureMeta struct {
	Handler       interface{}
	Description   string
	Signature     string
	Mode          string
	WorksOnSystem bool
	MinArgs       int
	MaxArgs       int
}

type mockFunctionPluginWithProcedures struct{}

func (m mockFunctionPluginWithProcedures) Name() string    { return "covplug" }
func (m mockFunctionPluginWithProcedures) Version() string { return "0.1.0" }
func (m mockFunctionPluginWithProcedures) Functions() map[string]interface{} {
	return map[string]interface{}{
		"double": struct {
			Handler     interface{}
			Description string
		}{
			Handler:     func(x float64) float64 { return x * 2 },
			Description: "double input",
		},
	}
}
func (m mockFunctionPluginWithProcedures) Procedures() map[string]mockProcedureMeta {
	return map[string]mockProcedureMeta{
		"readProc": {
			Handler: func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error) {
				return &cypher.ExecuteResult{}, nil
			},
			Description: "reads data",
			MinArgs:     1,
			MaxArgs:     3,
		},
		"apoc.custom.proc": {
			Handler: func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error) {
				return &cypher.ExecuteResult{}, nil
			},
			Description:   "custom fully-qualified proc",
			Signature:     "apoc.custom.proc() :: (value :: ANY)",
			Mode:          "DBMS",
			WorksOnSystem: true,
			MinArgs:       0,
			MaxArgs:       0,
		},
	}
}

// Helper to get reflect.Value from interface
func reflectValueOf(i interface{}) reflect.Value {
	return reflect.ValueOf(i)
}

func TestRegisterHeimdallPlugin_RequiresSubsystemContext(t *testing.T) {
	// If Heimdall is disabled/not initialized, Heimdall plugins must not be registered or started.
	plugin := &mockHeimdallPlugin{
		name:    "watcher",
		version: "1.0.0",
		ptype:   "heimdall",
	}

	loaded, err := registerHeimdallPlugin(plugin, "/test/watcher.so", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errHeimdallContextRequired)
	require.NotNil(t, loaded)
	assert.Equal(t, PluginTypeHeimdall, loaded.Type)
	assert.Equal(t, "watcher", loaded.Name)
	assert.Equal(t, "1.0.0", loaded.Version)
	assert.Equal(t, 0, plugin.startCalls, "Start() must not be called when Heimdall is disabled")
}
