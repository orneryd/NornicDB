// Plugin loading for NornicDB
// Automatically detects and loads .so plugins from configured directories.
// Supports two plugin types:
//   - Function plugins (APOC-style): Provide Cypher functions
//   - Heimdall plugins: Provide SLM subsystem management actions
package nornicdb

import (
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/apoc/registry"
	"os"
	"path/filepath"
	"plugin"
	"reflect"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// PluginType identifies the kind of plugin.
type PluginType string

const (
	PluginTypeFunction PluginType = "function" // APOC-style function plugins
	PluginTypeHeimdall PluginType = "heimdall" // Heimdall subsystem plugins
	PluginTypeUnknown  PluginType = "unknown"
)

// PluginFunction represents a Cypher function provided by a plugin.
type PluginFunction struct {
	Name        string
	Handler     *registry.Callable
	Description string
	Category    string
}

// PluginProcedure represents a Cypher procedure provided by a plugin.
type PluginProcedure struct {
	Name          string
	Handler       interface{}
	Signature     string
	Description   string
	Mode          string
	WorksOnSystem bool
	MinArgs       int
	MaxArgs       int
	Category      string
}

// LoadedPlugin represents a loaded plugin with its metadata.
type LoadedPlugin struct {
	Name       string
	Version    string
	Type       PluginType
	Path       string
	Functions  []PluginFunction // Only for function plugins
	Procedures []PluginProcedure
}

var (
	loadedPlugins      = make(map[string]*LoadedPlugin)
	pluginFunctions    = make(map[string]PluginFunction)
	pluginProcedures   = make(map[string]PluginProcedure)
	pluginsMu          sync.RWMutex
	pluginsInitialized bool
)

var errHeimdallContextRequired = errors.New("heimdall plugin requires subsystem context (host context not initialized)")

// LoadPluginsFromDir scans a directory for .so files and loads them.
// Auto-detects plugin types and registers appropriately:
//   - Function plugins → Register with APOC/Cypher executor
//   - Heimdall plugins → Register with Heimdall SubsystemManager
//
// A plugin that fails to load doesn't stop the others: every other plugin in
// the directory still loads, and the failures are returned together, one
// NornicDBCorePluginLoadFailed per file, after the directory is done. A
// Heimdall plugin without a subsystem context is skipped, not a failure.
// Callers treat the returned error as a warning; returning nil after a failed
// file made a plugin that didn't load look like an empty directory (#867).
func LoadPluginsFromDir(dir string, heimdallCtx *heimdall.SubsystemContext) error {
	diagnosticCredentials := observability.ClientCredentials(dir)
	if heimdallCtx != nil {
		diagnosticCredentials = append(diagnosticCredentials, observability.ClientCredentials(heimdallCtx.Config.APIURL, heimdallCtx.Config.APIKey)...)
	}
	if dir == "" {
		fmt.Printf("   [Plugin Debug] LoadPluginsFromDir called with empty directory\n")
		return nil
	}

	// Resolve relative paths to absolute
	absDir, err := filepath.Abs(dir)
	if err != nil {
		safeDir := observability.RedactText(dir, diagnosticCredentials...)
		safeErr := observability.RedactError(err, diagnosticCredentials...)
		fmt.Printf("   [Plugin Debug] Failed to resolve path %s: %v\n", safeDir, safeErr)
		return observability.RedactError(fmt.Errorf("resolving plugins directory: %w", err), diagnosticCredentials...)
	}
	safeAbsDir := observability.RedactText(absDir, diagnosticCredentials...)
	safeDir := observability.RedactText(dir, diagnosticCredentials...)
	fmt.Printf("   [Plugin Debug] Loading plugins from: %s (resolved from %s)\n", safeAbsDir, safeDir)

	info, err := os.Stat(absDir)
	if os.IsNotExist(err) {
		fmt.Printf("   [Plugin Debug] Directory does not exist: %s\n", safeAbsDir)
		return nil // No plugins directory
	}
	if err != nil {
		return observability.RedactError(fmt.Errorf("checking plugins directory '%s': %w", safeAbsDir, err), diagnosticCredentials...)
	}
	if !info.IsDir() {
		return fmt.Errorf("not a directory: %s", safeAbsDir)
	}

	matches, err := filepath.Glob(filepath.Join(absDir, "*.so"))
	if err != nil {
		return observability.RedactError(fmt.Errorf("scanning directory '%s': %w", safeAbsDir, err), diagnosticCredentials...)
	}
	safeMatches := make([]string, len(matches))
	for i, match := range matches {
		safeMatches[i] = observability.RedactText(match, diagnosticCredentials...)
	}
	fmt.Printf("   [Plugin Debug] Found %d .so files: %v\n", len(matches), safeMatches)
	if len(matches) == 0 {
		return nil
	}

	pluginsMu.Lock()
	defer pluginsMu.Unlock()

	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║ Loading Plugins                                              ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")

	stats := struct {
		total     int
		function  int
		heimdall  int
		skipped   int
		failed    int
		functions int
		actions   int
	}{}

	var failures []error
	for _, path := range matches {
		loaded, err := loadPluginFile(path, heimdallCtx)
		if err != nil {
			// Embedded startup may scan plugins before a host context is supplied.
			// Server plugin-only startup supplies that context even without inference.
			if errors.Is(err, errHeimdallContextRequired) {
				stats.skipped++
				if loaded != nil && loaded.Name != "" {
					fmt.Printf("║ - [HEIM] %-15s v%-8s deferred (no host context)      ║\n",
						observability.RedactText(loaded.Name, diagnosticCredentials...), observability.RedactText(loaded.Version, diagnosticCredentials...))
				} else {
					fmt.Printf("║ - [HEIM] %-47s deferred (no host context) ║\n", observability.RedactText(filepath.Base(path), diagnosticCredentials...))
				}
				continue
			}

			safePath := observability.RedactText(filepath.Base(path), diagnosticCredentials...)
			safeErr := observability.RedactError(err, diagnosticCredentials...)
			fmt.Printf("║ ⚠️  %-56s ║\n", safePath+": "+safeErr.Error())
			stats.failed++
			failures = append(failures, localizedError(localization.NornicDBCorePluginLoadFailed(safePath, safeErr), safeErr))
			continue
		}

		loadedPlugins[loaded.Name] = loaded
		stats.total++

		switch loaded.Type {
		case PluginTypeFunction:
			stats.function++
			stats.functions += len(loaded.Functions)
			for _, fn := range loaded.Functions {
				pluginFunctions[fn.Name] = fn
			}
			for _, proc := range loaded.Procedures {
				pluginProcedures[proc.Name] = proc
			}
			fmt.Printf("║ ✓ [FUNC] %-15s v%-8s %3d functions %12s ║\n",
				observability.RedactText(loaded.Name, diagnosticCredentials...), observability.RedactText(loaded.Version, diagnosticCredentials...), len(loaded.Functions), "")

		case PluginTypeHeimdall:
			stats.heimdall++
			// Actions count comes from the registered plugin
			if heimdallCtx != nil {
				mgr := heimdall.GetSubsystemManager()
				if p, ok := mgr.GetPlugin(loaded.Name); ok {
					actionCount := len(p.Actions())
					stats.actions += actionCount
					fmt.Printf("║ ✓ [HEIM] %-15s v%-8s %3d actions  %12s ║\n",
						observability.RedactText(loaded.Name, diagnosticCredentials...), observability.RedactText(loaded.Version, diagnosticCredentials...), actionCount, "")
				}
			}
		}
	}

	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Loaded: %d plugins (%d function, %d heimdall) %15s ║\n",
		stats.total, stats.function, stats.heimdall, "")
	if stats.skipped > 0 {
		fmt.Printf("║ Deferred: %d plugins (no host context) %24s ║\n", stats.skipped, "")
	}
	if stats.functions > 0 {
		fmt.Printf("║         %d Cypher functions available %23s ║\n", stats.functions, "")
	}
	if stats.actions > 0 {
		fmt.Printf("║         %d Heimdall actions available %23s ║\n", stats.actions, "")
	}
	if stats.failed > 0 {
		fmt.Printf("║ Failed: %d plugins %42s ║\n", stats.failed, "")
	}
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	pluginsInitialized = true
	return errors.Join(failures...)
}

// loadPluginFile loads a single .so plugin and auto-detects its type.
func loadPluginFile(path string, heimdallCtx *heimdall.SubsystemContext) (*LoadedPlugin, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	sym, err := p.Lookup("Plugin")
	if err != nil {
		return nil, fmt.Errorf("no Plugin symbol")
	}

	// Detect plugin type using reflection
	pluginType := detectPluginType(sym)

	switch pluginType {
	case PluginTypeFunction:
		return loadFunctionPlugin(sym, path)
	case PluginTypeHeimdall:
		return loadHeimdallPlugin(sym, path, heimdallCtx)
	default:
		return nil, fmt.Errorf("unknown plugin type (missing Type() method?)")
	}
}

// detectPluginType inspects the plugin to determine its type.
func detectPluginType(sym interface{}) PluginType {
	val := reflect.ValueOf(sym)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	typeMethod := val.MethodByName("Type")
	if !typeMethod.IsValid() {
		return PluginTypeUnknown
	}

	result := typeMethod.Call(nil)
	if len(result) != 1 || result[0].Kind() != reflect.String {
		return PluginTypeUnknown
	}

	typeStr := result[0].String()
	switch typeStr {
	case "heimdall":
		return PluginTypeHeimdall
	case "function", "apoc", "": // Empty defaults to function for compatibility
		return PluginTypeFunction
	default:
		return PluginTypeUnknown
	}
}

// loadFunctionPlugin loads an APOC-style function plugin.
func loadFunctionPlugin(sym interface{}, path string) (*LoadedPlugin, error) {
	val := reflect.ValueOf(sym)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	name, err := callStringMethod(val, "Name")
	if err != nil {
		return nil, err
	}

	version, err := callStringMethod(val, "Version")
	if err != nil {
		return nil, err
	}

	functions, err := extractFunctions(val, name)
	if err != nil {
		return nil, err
	}
	procedures, err := extractProcedures(val, name)
	if err != nil {
		return nil, err
	}
	if err := registerPluginProcedures(procedures); err != nil {
		return nil, err
	}

	return &LoadedPlugin{
		Name:       name,
		Version:    version,
		Type:       PluginTypeFunction,
		Path:       path,
		Functions:  functions,
		Procedures: procedures,
	}, nil
}

// loadHeimdallPlugin loads a Heimdall subsystem plugin.
func loadHeimdallPlugin(sym interface{}, path string, ctx *heimdall.SubsystemContext) (*LoadedPlugin, error) {
	// Try direct type assertion first
	if hp, ok := sym.(heimdall.HeimdallPlugin); ok {
		return registerHeimdallPlugin(hp, path, ctx)
	}

	// Try pointer to interface
	if hpp, ok := sym.(*heimdall.HeimdallPlugin); ok && hpp != nil {
		return registerHeimdallPlugin(*hpp, path, ctx)
	}

	// Fall back to reflection wrapper
	val := reflect.ValueOf(sym)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	// Verify it has all required methods
	requiredMethods := []string{
		"Name", "Version", "Type", "Description", "Initialize",
		"Start", "Stop", "Shutdown", "Status", "Health", "Metrics",
		"Config", "Configure", "ConfigSchema", "Actions", "Summary", "RecentEvents",
	}
	for _, method := range requiredMethods {
		if !val.MethodByName(method).IsValid() {
			return nil, fmt.Errorf("missing %s() - not a valid HeimdallPlugin", method)
		}
	}

	// Wrap in reflection adapter
	wrapper := &reflectHeimdallWrapper{val: val}
	return registerHeimdallPlugin(wrapper, path, ctx)
}

// registerHeimdallPlugin registers the plugin with Heimdall's subsystem manager.
func registerHeimdallPlugin(hp heimdall.HeimdallPlugin, path string, ctx *heimdall.SubsystemContext) (*LoadedPlugin, error) {
	// A host context is required; it can be plugin-only with no inference or chat.
	// Embedded scans with a nil context defer initialization to the host.
	if ctx == nil {
		return &LoadedPlugin{
			Name:    hp.Name(),
			Version: hp.Version(),
			Type:    PluginTypeHeimdall,
			Path:    path,
		}, errHeimdallContextRequired
	}

	mgr := heimdall.GetSubsystemManager()

	// Set context if provided
	mgr.SetContext(*ctx)

	// Register with Heimdall
	if err := mgr.RegisterPlugin(hp, path, false); err != nil {
		return nil, fmt.Errorf("register: %w", err)
	}

	// Start the plugin
	if err := hp.Start(); err != nil {
		mgr.UnregisterPlugin(hp.Name())
		return nil, errors.Join(fmt.Errorf("start: %w", err), hp.Shutdown())
	}

	return &LoadedPlugin{
		Name:    hp.Name(),
		Version: hp.Version(),
		Type:    PluginTypeHeimdall,
		Path:    path,
	}, nil
}

// extractFunctions parses the Functions() map from a function plugin.
func extractFunctions(val reflect.Value, pluginName string) ([]PluginFunction, error) {
	funcsMethod := val.MethodByName("Functions")
	if !funcsMethod.IsValid() {
		return nil, fmt.Errorf("no Functions() method")
	}

	// Author: Codex NOR-184 lane. Admission errors and Functions() errors must
	// survive plugin loading, just like errors from each published handler.
	provider, err := registry.Compile(funcsMethod.Interface())
	if err != nil {
		return nil, fmt.Errorf("Functions() invalid return: %w", err)
	}
	result, err := provider.Call(nil)
	if err != nil {
		return nil, fmt.Errorf("Functions(): %w", err)
	}
	var functions []PluginFunction
	mapVal := reflect.ValueOf(result)
	if !mapVal.IsValid() {
		return functions, nil
	}
	if mapVal.Kind() != reflect.Map {
		return functions, nil
	}

	for _, key := range mapVal.MapKeys() {
		funcName := key.String()
		funcVal := mapVal.MapIndex(key)
		for funcVal.Kind() == reflect.Interface && !funcVal.IsNil() {
			funcVal = funcVal.Elem()
		}

		// Both direct functions and descriptor structs are supported plugin
		// transports. Their result protocol is admitted below before publication.
		handler := funcVal.Interface()
		var desc string

		if funcVal.Kind() == reflect.Struct {
			if field := funcVal.FieldByName("Handler"); field.IsValid() {
				handler = field.Interface()
			}
			if field := funcVal.FieldByName("Description"); field.IsValid() && field.Kind() == reflect.String {
				desc = field.String()
			}
		}

		// Use fully qualified name if provided, otherwise add plugin prefix
		fullName := funcName
		if len(funcName) < 5 || funcName[:5] != "apoc." {
			fullName = fmt.Sprintf("apoc.%s.%s", pluginName, funcName)
		}

		checked, err := registry.Compile(handler)
		if err != nil {
			return nil, fmt.Errorf("function %s: %w", fullName, err)
		}
		functions = append(functions, PluginFunction{
			Name:        fullName,
			Handler:     checked,
			Description: desc,
			Category:    pluginName,
		})
	}

	return functions, nil
}

// extractProcedures parses the optional Procedures() map from a function plugin.
func extractProcedures(val reflect.Value, pluginName string) ([]PluginProcedure, error) {
	procsMethod := val.MethodByName("Procedures")
	if !procsMethod.IsValid() {
		return []PluginProcedure{}, nil
	}

	result := procsMethod.Call(nil)
	if len(result) != 1 {
		return nil, fmt.Errorf("Procedures() invalid return")
	}

	var procedures []PluginProcedure
	mapVal := result[0]
	if mapVal.Kind() != reflect.Map {
		return procedures, nil
	}

	for _, key := range mapVal.MapKeys() {
		procName := key.String()
		procVal := mapVal.MapIndex(key)

		var handler interface{}
		var desc, signature, mode string
		worksOnSystem := false
		minArgs, maxArgs := 0, -1

		if procVal.Kind() == reflect.Struct {
			if field := procVal.FieldByName("Handler"); field.IsValid() {
				handler = field.Interface()
			}
			if field := procVal.FieldByName("Description"); field.IsValid() && field.Kind() == reflect.String {
				desc = field.String()
			}
			if field := procVal.FieldByName("Signature"); field.IsValid() && field.Kind() == reflect.String {
				signature = field.String()
			}
			if field := procVal.FieldByName("Mode"); field.IsValid() && field.Kind() == reflect.String {
				mode = field.String()
			}
			if field := procVal.FieldByName("WorksOnSystem"); field.IsValid() && field.Kind() == reflect.Bool {
				worksOnSystem = field.Bool()
			}
			if field := procVal.FieldByName("MinArgs"); field.IsValid() && (field.Kind() == reflect.Int || field.Kind() == reflect.Int64) {
				minArgs = int(field.Int())
			}
			if field := procVal.FieldByName("MaxArgs"); field.IsValid() && (field.Kind() == reflect.Int || field.Kind() == reflect.Int64) {
				maxArgs = int(field.Int())
			}
		}

		fullName := procName
		if !strings.Contains(procName, ".") {
			fullName = fmt.Sprintf("%s.%s", pluginName, procName)
		}
		if signature == "" {
			signature = fmt.Sprintf("%s(...) :: (value :: ANY)", fullName)
		}
		if mode == "" {
			mode = "READ"
		}
		procedures = append(procedures, PluginProcedure{
			Name:          fullName,
			Handler:       handler,
			Signature:     signature,
			Description:   desc,
			Mode:          mode,
			WorksOnSystem: worksOnSystem,
			MinArgs:       minArgs,
			MaxArgs:       maxArgs,
			Category:      pluginName,
		})
	}

	return procedures, nil
}

// callStringMethod calls a method and returns its string result.
func callStringMethod(val reflect.Value, methodName string) (string, error) {
	method := val.MethodByName(methodName)
	if !method.IsValid() {
		return "", fmt.Errorf("no %s() method", methodName)
	}

	result := method.Call(nil)
	if len(result) != 1 {
		return "", fmt.Errorf("%s() invalid return", methodName)
	}

	return result[0].String(), nil
}

// reflectHeimdallWrapper wraps a plugin loaded via reflection.
type reflectHeimdallWrapper struct {
	val reflect.Value
}

func (w *reflectHeimdallWrapper) Name() string { s, _ := callStringMethod(w.val, "Name"); return s }
func (w *reflectHeimdallWrapper) Version() string {
	s, _ := callStringMethod(w.val, "Version")
	return s
}
func (w *reflectHeimdallWrapper) Type() string { s, _ := callStringMethod(w.val, "Type"); return s }
func (w *reflectHeimdallWrapper) Description() string {
	s, _ := callStringMethod(w.val, "Description")
	return s
}

func (w *reflectHeimdallWrapper) Initialize(ctx heimdall.SubsystemContext) error {
	method := w.val.MethodByName("Initialize")
	result := method.Call([]reflect.Value{reflect.ValueOf(ctx)})
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *reflectHeimdallWrapper) Start() error    { return w.callErrorMethod("Start") }
func (w *reflectHeimdallWrapper) Stop() error     { return w.callErrorMethod("Stop") }
func (w *reflectHeimdallWrapper) Shutdown() error { return w.callErrorMethod("Shutdown") }

func (w *reflectHeimdallWrapper) Status() heimdall.SubsystemStatus {
	result := w.val.MethodByName("Status").Call(nil)
	if s, ok := result[0].Interface().(heimdall.SubsystemStatus); ok {
		return s
	}
	return heimdall.StatusError
}

func (w *reflectHeimdallWrapper) Health() heimdall.SubsystemHealth {
	result := w.val.MethodByName("Health").Call(nil)
	if h, ok := result[0].Interface().(heimdall.SubsystemHealth); ok {
		return h
	}
	return heimdall.SubsystemHealth{Status: heimdall.StatusError, Healthy: false}
}

func (w *reflectHeimdallWrapper) Metrics() map[string]interface{} {
	return w.callMapMethod("Metrics")
}

func (w *reflectHeimdallWrapper) Config() map[string]interface{} {
	return w.callMapMethod("Config")
}

func (w *reflectHeimdallWrapper) Configure(settings map[string]interface{}) error {
	method := w.val.MethodByName("Configure")
	result := method.Call([]reflect.Value{reflect.ValueOf(settings)})
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *reflectHeimdallWrapper) ConfigSchema() map[string]interface{} {
	return w.callMapMethod("ConfigSchema")
}

func (w *reflectHeimdallWrapper) Actions() map[string]heimdall.ActionFunc {
	result := w.val.MethodByName("Actions").Call(nil)
	if m, ok := result[0].Interface().(map[string]heimdall.ActionFunc); ok {
		return m
	}
	return nil
}

func (w *reflectHeimdallWrapper) Summary() string {
	s, _ := callStringMethod(w.val, "Summary")
	return s
}

func (w *reflectHeimdallWrapper) RecentEvents(limit int) []heimdall.SubsystemEvent {
	result := w.val.MethodByName("RecentEvents").Call([]reflect.Value{reflect.ValueOf(limit)})
	if e, ok := result[0].Interface().([]heimdall.SubsystemEvent); ok {
		return e
	}
	return nil
}

func (w *reflectHeimdallWrapper) callErrorMethod(name string) error {
	result := w.val.MethodByName(name).Call(nil)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *reflectHeimdallWrapper) callMapMethod(name string) map[string]interface{} {
	result := w.val.MethodByName(name).Call(nil)
	if m, ok := result[0].Interface().(map[string]interface{}); ok {
		return m
	}
	return nil
}

// GetPluginFunction returns a plugin function by name.
func GetPluginFunction(name string) (PluginFunction, bool) {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	fn, ok := pluginFunctions[name]
	return fn, ok
}

// pluginFunctionInfos lists the registered plugin functions for SHOW
// FUNCTIONS (#1168): name, category, description and a signature from each
// handler's Go type.
func pluginFunctionInfos() []cypher.PluginFunctionInfo {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	infos := make([]cypher.PluginFunctionInfo, 0, len(pluginFunctions))
	for name, fn := range pluginFunctions {
		infos = append(infos, cypher.PluginFunctionInfo{
			Name:        name,
			Category:    fn.Category,
			Description: fn.Description,
			Signature:   pluginFunctionSignature(name, fn.Handler),
		})
	}
	return infos
}

// pluginFunctionSignature is a plugin function's signature in Neo4j's form,
// name(arg1 :: T1, …) :: R, from its handler's Go type; a variadic last
// parameter is a list.
func pluginFunctionSignature(name string, handler *registry.Callable) string {
	t := handler.Signature()
	if t == nil || t.Kind() != reflect.Func {
		return name + "() :: ANY"
	}
	parts := make([]string, 0, t.NumIn())
	for i := 0; i < t.NumIn(); i++ {
		parts = append(parts, fmt.Sprintf("arg%d :: %s", i+1, pluginCypherType(t.In(i))))
	}
	returns := "ANY"
	if t.NumOut() > 0 {
		returns = pluginCypherType(t.Out(0))
	}
	return fmt.Sprintf("%s(%s) :: %s", name, strings.Join(parts, ", "), returns)
}

// pluginCypherType is the Cypher type a plugin handler's Go type stands for.
func pluginCypherType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "STRING"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "FLOAT"
	case reflect.Slice, reflect.Array:
		return "LIST<" + pluginCypherType(t.Elem()) + ">"
	case reflect.Map:
		return "MAP"
	}
	return "ANY"
}

// ListPluginFunctions returns all registered function names.
func ListPluginFunctions() []string {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	names := make([]string, 0, len(pluginFunctions))
	for name := range pluginFunctions {
		names = append(names, name)
	}
	return names
}

// GetPluginProcedure returns a plugin procedure by name.
func GetPluginProcedure(name string) (PluginProcedure, bool) {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	p, ok := pluginProcedures[name]
	return p, ok
}

// ListPluginProcedures returns all registered plugin procedure names.
func ListPluginProcedures() []string {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	names := make([]string, 0, len(pluginProcedures))
	for name := range pluginProcedures {
		names = append(names, name)
	}
	return names
}

// ListLoadedPlugins returns all loaded plugins.
func ListLoadedPlugins() []*LoadedPlugin {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	result := make([]*LoadedPlugin, 0, len(loadedPlugins))
	for _, p := range loadedPlugins {
		result = append(result, p)
	}
	return result
}

// PluginsInitialized returns true if plugins have been loaded.
func PluginsInitialized() bool {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	return pluginsInitialized
}
