package nornicdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnifiedPluginLoading tests that the unified loader correctly handles
// both APOC function plugins and Heimdall subsystem plugins from actual .so files
func TestUnifiedPluginLoading(t *testing.T) {
	// Skip if plugin files don't exist
	apocPlugin := "apoc/built-plugins/apoc.so"
	heimdallPlugin := "plugins/heimdall/built-plugins/watcher.so"

	// Check if plugins exist (relative to project root)
	if _, err := os.Stat(apocPlugin); os.IsNotExist(err) {
		t.Skip("APOC plugin not built - run 'make plugins' first")
	}
	if _, err := os.Stat(heimdallPlugin); os.IsNotExist(err) {
		t.Skip("Heimdall watcher plugin not built - run 'make plugins' first")
	}

	t.Run("loads APOC function plugin from .so", func(t *testing.T) {
		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		// Load APOC plugin
		apocDir := filepath.Dir(apocPlugin)
		err := LoadPluginsFromDir(apocDir, nil)
		require.NoError(t, err)

		// Verify plugin loaded
		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1, "Should load exactly 1 APOC plugin")

		apoc := plugins[0]
		assert.Equal(t, "apoc", apoc.Name)
		assert.Equal(t, "1.0.0", apoc.Version)
		assert.Equal(t, PluginTypeFunction, apoc.Type)
		assert.Greater(t, len(apoc.Functions), 100, "APOC should have many functions")

		// Verify some known APOC functions are available
		knownFuncs := []string{
			"apoc.coll.sum",
			"apoc.coll.reverse",
			"apoc.text.join",
		}

		availableFuncs := ListPluginFunctions()
		for _, funcName := range knownFuncs {
			assert.Contains(t, availableFuncs, funcName, "Should have %s", funcName)

			fn, found := GetPluginFunction(funcName)
			assert.True(t, found, "Should find %s", funcName)
			assert.NotNil(t, fn.Handler, "%s handler should not be nil", funcName)
		}
	})

	t.Run("loads Heimdall watcher plugin from .so", func(t *testing.T) {
		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		// Create minimal Heimdall context
		ctx := &heimdall.SubsystemContext{
			Config: heimdall.Config{
				Model:        "test-model",
				ModelsDir:    "models",
				GPULayers:    0,
				ContextSize: 2048,
			},
		}

		// Load Heimdall plugin
		heimdallDir := filepath.Dir(heimdallPlugin)
		err := LoadPluginsFromDir(heimdallDir, ctx)
		require.NoError(t, err)

		// Verify plugin loaded
		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1, "Should load exactly 1 Heimdall plugin")

		watcher := plugins[0]
		assert.Equal(t, "watcher", watcher.Name)
		assert.Equal(t, "1.0.0", watcher.Version)
		assert.Equal(t, PluginTypeHeimdall, watcher.Type)
		assert.Nil(t, watcher.Functions, "Heimdall plugins don't export functions")

		// Verify plugin registered with Heimdall subsystem manager
		mgr := heimdall.GetSubsystemManager()
		watcherPlugin, found := mgr.GetPlugin("watcher")
		require.True(t, found, "Watcher plugin should be registered with SubsystemManager")

		// Verify plugin has actions
		actions := watcherPlugin.Actions()
		assert.Greater(t, len(actions), 5, "Watcher should have several actions")

		// Check some known actions
		knownActions := []string{"status", "health", "metrics"}
		for _, actionName := range knownActions {
			_, found := actions[actionName]
			assert.True(t, found, "Should have action: %s", actionName)
		}

		// Cleanup
		watcherPlugin.Stop()
	})

	t.Run("loads both plugin types from mixed directory", func(t *testing.T) {
		// Skip if we can't create a temp directory with both plugins
		tmpDir := t.TempDir()

		// Copy both plugins to temp directory
		copyFile := func(src, dst string) error {
			data, err := os.ReadFile(src)
			if err != nil {
				return err
			}
			return os.WriteFile(dst, data, 0755)
		}

		err := copyFile(apocPlugin, filepath.Join(tmpDir, "apoc.so"))
		require.NoError(t, err)
		err = copyFile(heimdallPlugin, filepath.Join(tmpDir, "watcher.so"))
		require.NoError(t, err)

		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		// Create Heimdall context
		ctx := &heimdall.SubsystemContext{
			Config: heimdall.Config{
				Model:        "test-model",
				ModelsDir:    "models",
				GPULayers:    0,
				ContextSize: 2048,
			},
		}

		// Load all plugins from the mixed directory
		err = LoadPluginsFromDir(tmpDir, ctx)
		require.NoError(t, err)

		// Verify both plugins loaded
		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 2, "Should load both APOC and Heimdall plugins")

		// Count by type
		typeCount := make(map[PluginType]int)
		for _, p := range plugins {
			typeCount[p.Type]++
		}
		assert.Equal(t, 1, typeCount[PluginTypeFunction], "Should have 1 function plugin")
		assert.Equal(t, 1, typeCount[PluginTypeHeimdall], "Should have 1 Heimdall plugin")

		// Verify APOC functions available
		assert.Greater(t, len(ListPluginFunctions()), 100, "Should have many APOC functions")

		// Verify Heimdall actions available
		mgr := heimdall.GetSubsystemManager()
		watcherPlugin, found := mgr.GetPlugin("watcher")
		require.True(t, found)
		assert.Greater(t, len(watcherPlugin.Actions()), 5, "Should have watcher actions")

		// Cleanup
		watcherPlugin.Stop()
	})
}

// TestPluginAutoDetection verifies that plugins self-identify correctly
func TestPluginAutoDetection(t *testing.T) {
	apocPlugin := "apoc/built-plugins/apoc.so"
	heimdallPlugin := "plugins/heimdall/built-plugins/watcher.so"

	if _, err := os.Stat(apocPlugin); os.IsNotExist(err) {
		t.Skip("Plugins not built - run 'make plugins' first")
	}

	t.Run("APOC plugin identifies as function type", func(t *testing.T) {
		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		apocDir := filepath.Dir(apocPlugin)
		err := LoadPluginsFromDir(apocDir, nil)
		require.NoError(t, err)

		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1)
		assert.Equal(t, PluginTypeFunction, plugins[0].Type)
	})

	t.Run("Heimdall plugin identifies as heimdall type", func(t *testing.T) {
		if _, err := os.Stat(heimdallPlugin); os.IsNotExist(err) {
			t.Skip("Heimdall plugin not built")
		}

		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		ctx := &heimdall.SubsystemContext{
			Config: heimdall.Config{
				Model:        "test-model",
				ModelsDir:    "models",
				GPULayers:    0,
				ContextSize: 2048,
			},
		}

		heimdallDir := filepath.Dir(heimdallPlugin)
		err := LoadPluginsFromDir(heimdallDir, ctx)
		require.NoError(t, err)

		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1)
		assert.Equal(t, PluginTypeHeimdall, plugins[0].Type)

		// Cleanup
		mgr := heimdall.GetSubsystemManager()
		if p, ok := mgr.GetPlugin("watcher"); ok {
			p.Stop()
		}
	})
}

// TestPluginFunctionsStillWork verifies that the refactored system
// didn't break existing APOC function calls
func TestPluginFunctionsStillWork(t *testing.T) {
	apocPlugin := "apoc/built-plugins/apoc.so"
	if _, err := os.Stat(apocPlugin); os.IsNotExist(err) {
		t.Skip("APOC plugin not built - run 'make plugins' first")
	}

	t.Run("APOC functions work after unified loader refactor", func(t *testing.T) {
		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		// Load APOC plugin
		apocDir := filepath.Dir(apocPlugin)
		err := LoadPluginsFromDir(apocDir, nil)
		require.NoError(t, err)

		// Test specific functions still work
		testCases := []struct {
			name     string
			funcName string
		}{
			{"collection sum", "apoc.coll.sum"},
			{"collection reverse", "apoc.coll.reverse"},
			{"text join", "apoc.text.join"},
			{"text upper", "apoc.text.upper"},
			{"math abs", "apoc.math.abs"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fn, found := GetPluginFunction(tc.funcName)
				assert.True(t, found, "Function %s should exist", tc.funcName)
				if found {
					assert.NotNil(t, fn.Handler, "Handler should not be nil")
					assert.Equal(t, tc.funcName, fn.Name)
					assert.NotEmpty(t, fn.Description, "Should have description")
				}
			})
		}
	})
}

// TestHeimdallPluginLifecycle verifies the Heimdall plugin lifecycle
func TestHeimdallPluginLifecycle(t *testing.T) {
	heimdallPlugin := "plugins/heimdall/built-plugins/watcher.so"
	if _, err := os.Stat(heimdallPlugin); os.IsNotExist(err) {
		t.Skip("Heimdall plugin not built - run 'make plugins' first")
	}

	t.Run("Heimdall plugin lifecycle works", func(t *testing.T) {
		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		// Create Heimdall context
		ctx := &heimdall.SubsystemContext{
			Config: heimdall.Config{
				Model:        "test-model",
				ModelsDir:    "models",
				GPULayers:    0,
				ContextSize: 2048,
			},
		}

		// Load plugin
		heimdallDir := filepath.Dir(heimdallPlugin)
		err := LoadPluginsFromDir(heimdallDir, ctx)
		require.NoError(t, err)

		// Get plugin from SubsystemManager
		mgr := heimdall.GetSubsystemManager()
		plugin, found := mgr.GetPlugin("watcher")
		require.True(t, found, "Watcher plugin should be registered")

		// Test lifecycle
		t.Run("plugin is running after load", func(t *testing.T) {
			status := plugin.Status()
			assert.Equal(t, heimdall.StatusRunning, status)
		})

		t.Run("plugin has health check", func(t *testing.T) {
			health := plugin.Health()
			assert.True(t, health.Healthy)
			assert.Equal(t, heimdall.StatusRunning, health.Status)
		})

		t.Run("plugin has actions", func(t *testing.T) {
			actions := plugin.Actions()
			assert.Greater(t, len(actions), 5, "Should have multiple actions")

			// Verify action structure
			for name, actionFunc := range actions {
				assert.NotEmpty(t, name)
				assert.NotNil(t, actionFunc.Handler, "Action %s should have handler", name)
			}
		})

		t.Run("plugin can be stopped", func(t *testing.T) {
			err := plugin.Stop()
			assert.NoError(t, err)
		})
	})
}

// TestPluginLoadingSequence verifies plugins load in the correct order
// and that the unified loader is called at the right times
func TestPluginLoadingSequence(t *testing.T) {
	apocPlugin := "apoc/built-plugins/apoc.so"
	heimdallPlugin := "plugins/heimdall/built-plugins/watcher.so"

	if _, err := os.Stat(apocPlugin); os.IsNotExist(err) {
		t.Skip("Plugins not built - run 'make plugins' first")
	}
	if _, err := os.Stat(heimdallPlugin); os.IsNotExist(err) {
		t.Skip("Plugins not built - run 'make plugins' first")
	}

	t.Run("APOC loads during DB init without Heimdall context", func(t *testing.T) {
		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		// Load APOC without Heimdall context (simulates DB init)
		apocDir := filepath.Dir(apocPlugin)
		err := LoadPluginsFromDir(apocDir, nil)
		require.NoError(t, err)

		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1)
		assert.Equal(t, PluginTypeFunction, plugins[0].Type)
	})

	t.Run("Heimdall loads after Heimdall init with context", func(t *testing.T) {
		// Simulate loading Heimdall plugins after Heimdall is ready
		// Clear Heimdall plugins only
		pluginsMu.Lock()
		// Keep APOC plugins, clear Heimdall
		newLoaded := make(map[string]*LoadedPlugin)
		for name, p := range loadedPlugins {
			if p.Type == PluginTypeFunction {
				newLoaded[name] = p
			}
		}
		loadedPlugins = newLoaded
		pluginsMu.Unlock()

		ctx := &heimdall.SubsystemContext{
			Config: heimdall.Config{
				Model:        "test-model",
				ModelsDir:    "models",
				GPULayers:    0,
				ContextSize: 2048,
			},
		}

		heimdallDir := filepath.Dir(heimdallPlugin)
		err := LoadPluginsFromDir(heimdallDir, ctx)
		require.NoError(t, err)

		// Now should have both types
		plugins := ListLoadedPlugins()
		assert.GreaterOrEqual(t, len(plugins), 2, "Should have at least APOC + watcher")

		typeCount := make(map[PluginType]int)
		for _, p := range plugins {
			typeCount[p.Type]++
		}
		assert.GreaterOrEqual(t, typeCount[PluginTypeFunction], 1, "Should have function plugin(s)")
		assert.GreaterOrEqual(t, typeCount[PluginTypeHeimdall], 1, "Should have Heimdall plugin(s)")

		// Cleanup
		mgr := heimdall.GetSubsystemManager()
		if p, ok := mgr.GetPlugin("watcher"); ok {
			p.Stop()
		}
	})
}

// TestPluginIsolation verifies that function and Heimdall plugins
// don't interfere with each other
func TestPluginIsolation(t *testing.T) {
	apocPlugin := "apoc/built-plugins/apoc.so"
	heimdallPlugin := "plugins/heimdall/built-plugins/watcher.so"

	if _, err := os.Stat(apocPlugin); os.IsNotExist(err) {
		t.Skip("Plugins not built - run 'make plugins' first")
	}
	if _, err := os.Stat(heimdallPlugin); os.IsNotExist(err) {
		t.Skip("Plugins not built - run 'make plugins' first")
	}

	t.Run("function plugins don't affect Heimdall", func(t *testing.T) {
		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		// Load APOC first
		apocDir := filepath.Dir(apocPlugin)
		err := LoadPluginsFromDir(apocDir, nil)
		require.NoError(t, err)

		// Heimdall subsystem should be clean
		plugins := heimdall.ListHeimdallPlugins()
		assert.Empty(t, plugins, "No Heimdall plugins should be loaded yet")
	})

	t.Run("Heimdall plugins don't create Cypher functions", func(t *testing.T) {
		// Clear all
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		ctx := &heimdall.SubsystemContext{
			Config: heimdall.Config{
				Model:        "test-model",
				ModelsDir:    "models",
				GPULayers:    0,
				ContextSize: 2048,
			},
		}

		// Load only Heimdall plugin
		heimdallDir := filepath.Dir(heimdallPlugin)
		err := LoadPluginsFromDir(heimdallDir, ctx)
		require.NoError(t, err)

		// Should have no Cypher functions
		funcs := ListPluginFunctions()
		assert.Empty(t, funcs, "Heimdall plugins should not create Cypher functions")

		// But should have Heimdall actions
		mgr := heimdall.GetSubsystemManager()
		plugin, found := mgr.GetPlugin("watcher")
		require.True(t, found)
		actions := plugin.Actions()
		assert.Greater(t, len(actions), 0, "Should have Heimdall actions")

		// Cleanup
		plugin.Stop()
	})
}
