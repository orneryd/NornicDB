// Author: Codex NOR-184 lane. Public-route test fixtures only; no arithmetic.
package apocintegers

import (
	"fmt"

	"github.com/orneryd/nornicdb/apoc"
	apocmath "github.com/orneryd/nornicdb/apoc/math"
	"github.com/orneryd/nornicdb/apoc/number"
	apocplugin "github.com/orneryd/nornicdb/apoc/plugin"
	"github.com/orneryd/nornicdb/apoc/registry"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
)

// Export is a real arithmetic export. Function deliberately accommodates both
// the release's scalar signature and the corrected checked signature.
type Export struct {
	Name, Operation string
	Function        interface{}
}

var Exports = []Export{
	{"math_gcd", "gcd", apocmath.Gcd}, {"number_gcd", "gcd", number.GCD},
	{"math_lcm", "lcm", apocmath.Lcm}, {"number_lcm", "lcm", number.LCM},
}

// Consumer is one public registration/invocation class from the compiled census.
// Handler is either the real export/descriptor, or an error-returning adapter
// that delegates to the named public API without inspecting its result.
type Consumer struct {
	Class, Name, Operation string
	Handler                interface{}
}

type registrationPlugin struct{ name string }

func (p registrationPlugin) Name() string                       { return p.name }
func (registrationPlugin) Version() string                      { return "1" }
func (registrationPlugin) Description() string                  { return "NOR-184 public contract fixture" }
func (registrationPlugin) Initialize(apocstorage.Storage) error { return nil }
func (registrationPlugin) Cleanup() error                       { return nil }
func (p registrationPlugin) Register(r *registry.FunctionRegistry) error {
	for _, entry := range Exports {
		if err := r.RegisterFunction(p.name+"."+entry.Name, p.name, entry.Function, "", nil); err != nil {
			return err
		}
	}
	return nil
}

// DescriptorCall routes both direct-export and published-descriptor fixtures
// through the same checked owner. Legacy baseline adapters live only in the
// frozen red overlays, never beside the current callable implementation.
func DescriptorCall(handler interface{}, args []interface{}) (interface{}, error) {
	callable, err := registry.Compile(handler)
	if err != nil {
		return nil, err
	}
	return callable.Call(args)
}

// Consumers registers the real exports through each maintained public route.
// The returned cleanup removes only this fixture's global registrations.
func Consumers(scope string) ([]Consumer, func() error, error) {
	if err := apoc.Initialize(apocstorage.NewInMemoryStorage(), nil); err != nil {
		return nil, nil, err
	}
	instance := registry.NewFunctionRegistry()
	manager := apocplugin.NewPluginManager(apocstorage.NewInMemoryStorage())
	pluginName := "nor184_" + scope + "_plugin"
	if err := manager.Load(registrationPlugin{pluginName}); err != nil {
		return nil, nil, err
	}
	var globals []string
	cleanup := func() error {
		for _, name := range globals {
			registry.GetGlobalRegistry().Unregister(name)
		}
		return manager.Unload(pluginName)
	}
	var consumers []Consumer
	for _, entry := range Exports {
		rootName := "apoc.math." + entry.Operation
		if entry.Name[:6] == "number" {
			rootName = "apoc.number." + entry.Operation
		}
		info, ok := apoc.GetInfo(rootName)
		if !ok {
			_ = cleanup()
			return nil, nil, fmt.Errorf("missing %s", rootName)
		}
		name := "nor184_" + scope + "_" + entry.Name
		if err := instance.RegisterFunction(name, "nor184", entry.Function, "", nil); err != nil {
			_ = cleanup()
			return nil, nil, err
		}
		if err := registry.Register(name, "nor184", entry.Function, "", nil); err != nil {
			_ = cleanup()
			return nil, nil, err
		}
		globals = append(globals, name)
		descriptor, ok := instance.Get(name)
		if !ok {
			_ = cleanup()
			return nil, nil, fmt.Errorf("missing descriptor %s", name)
		}
		add := func(class string, handler interface{}) {
			consumers = append(consumers, Consumer{class, entry.Name, entry.Operation, handler})
		}
		add("export", entry.Function)
		add("root_descriptor", info.Handler)
		add("root_call", func(a, b interface{}) (interface{}, error) { return apoc.Call(rootName, []interface{}{a, b}) })
		add("registry_instance", func(a, b interface{}) (interface{}, error) { return instance.CallFunction(name, []interface{}{a, b}) })
		add("registry_global", func(a, b interface{}) (interface{}, error) { return registry.Call(name, []interface{}{a, b}) })
		add("registry_descriptor", func(a, b interface{}) (interface{}, error) {
			return DescriptorCall(descriptor.Handler, []interface{}{a, b})
		})
		add("plugin_manager", func(a, b interface{}) (interface{}, error) {
			return registry.Call(pluginName+"."+entry.Name, []interface{}{a, b})
		})
	}
	return consumers, cleanup, nil
}
