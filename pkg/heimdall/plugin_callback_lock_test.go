package heimdall

import (
	"sync/atomic"
	"testing"
	"time"
)

type managerCallbackPlugin struct {
	*MockHeimdallPlugin
	callback func(string)
}

func (p *managerCallbackPlugin) Name() string {
	p.callback("Name")
	return p.MockHeimdallPlugin.Name()
}

func (p *managerCallbackPlugin) Type() string {
	p.callback("Type")
	return PluginTypeHeimdall
}

func (p *managerCallbackPlugin) Initialize(ctx SubsystemContext) error {
	p.callback("Initialize")
	return p.MockHeimdallPlugin.Initialize(ctx)
}

func (p *managerCallbackPlugin) Actions() map[string]ActionFunc {
	p.callback("Actions")
	return p.MockHeimdallPlugin.Actions()
}

func (p *managerCallbackPlugin) Priority() int {
	p.callback("Priority")
	return 0
}

func (p *managerCallbackPlugin) Before() []string {
	p.callback("Before")
	return nil
}

func (p *managerCallbackPlugin) After() []string {
	p.callback("After")
	return nil
}

func (p *managerCallbackPlugin) Shutdown() error {
	p.callback("Shutdown")
	return p.MockHeimdallPlugin.Shutdown()
}

func waitManagerOperation(t *testing.T, operation func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- operation() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("plugin callback deadlocked while consulting its manager")
	}
}

func TestSubsystemManagerCallbacksReenter(t *testing.T) {
	for _, operation := range []string{"register", "list", "start", "stop", "health", "summaries", "shutdown"} {
		t.Run(operation, func(t *testing.T) {
			manager := &SubsystemManager{
				plugins: make(map[string]*LoadedHeimdallPlugin),
				actions: make(map[string]ActionFunc),
			}
			seen := make(map[string]bool)
			plugin := &managerCallbackPlugin{
				MockHeimdallPlugin: NewMockPlugin("reentrant"),
				callback: func(method string) {
					_, installed := manager.GetPlugin("reentrant")
					if (method == "Priority" || method == "Before" || method == "After") && !installed {
						t.Errorf("%s could not consult installed peers", method)
					}
					// A write also proves callbacks are outside read locks.
					manager.SetContext(SubsystemContext{})
					seen[method] = true
				},
			}
			if operation != "register" {
				manager.plugins["reentrant"] = &LoadedHeimdallPlugin{Plugin: plugin}
			}
			waitManagerOperation(t, func() error {
				switch operation {
				case "register":
					return manager.RegisterPlugin(plugin, "", true)
				case "list":
					manager.listOrderedPlugins()
				case "start":
					return manager.StartAll()
				case "stop":
					return manager.StopAll()
				case "health":
					manager.AllHealth()
				case "summaries":
					manager.AllSummaries()
				case "shutdown":
					return manager.ShutdownAll()
				}
				return nil
			})
			methods := []string{"Priority", "Before", "After"}
			if operation == "register" {
				methods = []string{"Name", "Type", "Initialize", "Actions"}
			} else if operation == "shutdown" {
				methods = append(methods, "Shutdown")
			}
			for _, method := range methods {
				if !seen[method] {
					t.Errorf("%s callback was not exercised", method)
				}
			}
		})
	}
}

func TestSubsystemManagerOrderCacheGeneration(t *testing.T) {
	for _, change := range []string{"register", "shutdown"} {
		t.Run(change, func(t *testing.T) {
			manager := &SubsystemManager{
				plugins: make(map[string]*LoadedHeimdallPlugin),
				actions: make(map[string]ActionFunc),
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var blocked atomic.Bool
			plugin := &managerCallbackPlugin{
				MockHeimdallPlugin: NewMockPlugin("original"),
				callback: func(method string) {
					if method == "Priority" && blocked.CompareAndSwap(false, true) {
						close(entered)
						<-release
					}
				},
			}
			manager.plugins["original"] = &LoadedHeimdallPlugin{Plugin: plugin}
			done := make(chan []*LoadedHeimdallPlugin, 1)
			go func() { done <- manager.listOrderedPlugins() }()
			<-entered
			// Mutate the registry while an old ordering calculation is in flight.
			waitManagerOperation(t, func() error {
				if change == "shutdown" {
					return manager.ShutdownAll()
				}
				return manager.RegisterPlugin(NewMockPlugin("new"), "", true)
			})
			manager.listOrderedPlugins() // Publish the new generation before the old fill returns.
			close(release)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("ordering calculation did not finish")
			}
			plugins := manager.listOrderedPlugins()
			want := 2
			if change == "shutdown" {
				want = 0
			}
			if len(plugins) != want {
				t.Fatalf("stale ordering cache: got %d plugins, want %d", len(plugins), want)
			}
		})
	}
}

func TestSubsystemManagerShutdownCallbackRegistersPlugin(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}
	plugin := &managerCallbackPlugin{
		MockHeimdallPlugin: NewMockPlugin("original"),
		callback: func(method string) {
			if method == "Shutdown" {
				if err := manager.RegisterPlugin(NewMockPlugin("replacement"), "", true); err != nil {
					t.Error(err)
				}
			}
		},
	}
	if err := manager.RegisterPlugin(plugin, "", true); err != nil {
		t.Fatal(err)
	}
	waitManagerOperation(t, manager.ShutdownAll)
	plugins := manager.listOrderedPlugins()
	if len(plugins) != 1 || plugins[0].Plugin.Name() != "replacement" {
		t.Fatalf("shutdown removed a plugin registered by its callback: %#v", plugins)
	}
	if _, ok := manager.GetAction("heimdall_replacement_test_action"); !ok {
		t.Fatal("shutdown removed the replacement plugin's action")
	}
}

func TestSubsystemManagerInitializeRejectsDuplicate(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}
	plugin := &managerCallbackPlugin{
		MockHeimdallPlugin: NewMockPlugin("original"),
		callback: func(method string) {
			if method == "Initialize" {
				if err := manager.RegisterPlugin(NewMockPlugin("original"), "", true); err == nil {
					t.Error("duplicate registration succeeded during initialization")
				}
			}
		},
	}
	waitManagerOperation(t, func() error { return manager.RegisterPlugin(plugin, "", true) })
	if got, ok := manager.GetPlugin("original"); !ok || got != plugin {
		t.Fatal("initializing plugin was replaced by a duplicate")
	}
}
