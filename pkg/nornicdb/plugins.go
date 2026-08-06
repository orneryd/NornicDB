// Plugin loading for NornicDB
// Automatically detects and loads .so plugins from configured directories.
// Supports two plugin types:
//   - Function plugins (APOC-style): Provide Cypher functions
//   - Heimdall plugins: Provide SLM subsystem management actions
package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"reflect"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
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
	Handler     interface{}
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

var errHeimdallContextRequired = errors.New("heimdall plugin requires subsystem context (Heimdall disabled or not initialized)")

// LoadPluginsFromDir scans a directory for .so files and loads them.
// Auto-detects plugin types and registers appropriately:
//   - Function plugins → Register with APOC/Cypher executor
//   - Heimdall plugins → Register with Heimdall SubsystemManager
func LoadPluginsFromDir(dir string, heimdallCtx *heimdall.SubsystemContext) error {
	if dir == "" {
		fmt.Printf("   [Plugin Debug] LoadPluginsFromDir called with empty directory\n")
		return nil
	}

	// Resolve relative paths to absolute
	absDir, err := filepath.Abs(dir)
	if err != nil {
		fmt.Printf("   [Plugin Debug] Failed to resolve path %s: %v\n", dir, err)
		return fmt.Errorf("resolving plugins directory: %w", err)
	}
	fmt.Printf("   [Plugin Debug] Loading plugins from: %s (resolved from %s)\n", absDir, dir)

	info, err := os.Stat(absDir)
	if os.IsNotExist(err) {
		fmt.Printf("   [Plugin Debug] Directory does not exist: %s\n", absDir)
		return nil // No plugins directory
	}
	if err != nil {
		return fmt.Errorf("checking plugins directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("not a directory: %s", absDir)
	}

	matches, err := filepath.Glob(filepath.Join(absDir, "*.so"))
	if err != nil {
		return fmt.Errorf("scanning directory: %w", err)
	}
	fmt.Printf("   [Plugin Debug] Found %d .so files: %v\n", len(matches), matches)
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

	for _, path := range matches {
		loaded, err := loadPluginFile(path, heimdallCtx)
		if err != nil {
			// Heimdall plugins require an initialized subsystem context; when Heimdall is disabled
			// (or not initialized yet), we skip loading them rather than treating as a failure.
			if errors.Is(err, errHeimdallContextRequired) {
				stats.skipped++
				if loaded != nil && loaded.Name != "" {
					fmt.Printf("║ - [HEIM] %-15s v%-8s skipped (Heimdall disabled)      ║\n",
						loaded.Name, loaded.Version)
				} else {
					fmt.Printf("║ - [HEIM] %-47s skipped (Heimdall disabled) ║\n", filepath.Base(path))
				}
				continue
			}

			fmt.Printf("║ ⚠️  %-56s ║\n", filepath.Base(path)+": "+err.Error())
			stats.failed++
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
				loaded.Name, loaded.Version, len(loaded.Functions), "")

		case PluginTypeHeimdall:
			stats.heimdall++
			// Actions count comes from the registered plugin
			if heimdallCtx != nil {
				mgr := heimdall.GetSubsystemManager()
				if p, ok := mgr.GetPlugin(loaded.Name); ok {
					actionCount := len(p.Actions())
					stats.actions += actionCount
					fmt.Printf("║ ✓ [HEIM] %-15s v%-8s %3d actions  %12s ║\n",
						loaded.Name, loaded.Version, actionCount, "")
				}
			}
		}
	}

	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Loaded: %d plugins (%d function, %d heimdall) %15s ║\n",
		stats.total, stats.function, stats.heimdall, "")
	if stats.skipped > 0 {
		fmt.Printf("║ Skipped: %d plugins (Heimdall disabled) %24s ║\n", stats.skipped, "")
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
	return nil
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
	for _, proc := range procedures {
		registerProcedureWithCypher(proc)
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
	// Heimdall plugins depend on an initialized Heimdall subsystem context (Bifrost, DB reader, etc).
	// When Heimdall is disabled, we should *not* register/start Heimdall plugins — doing so can
	// start background goroutines and make `heimdall.enabled: false` a no-op in practice.
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
		return nil, fmt.Errorf("start: %w", err)
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

	result := funcsMethod.Call(nil)
	if len(result) != 1 {
		return nil, fmt.Errorf("Functions() invalid return")
	}

	var functions []PluginFunction
	mapVal := result[0]
	if mapVal.Kind() != reflect.Map {
		return functions, nil
	}

	for _, key := range mapVal.MapKeys() {
		funcName := key.String()
		funcVal := mapVal.MapIndex(key)

		var handler interface{}
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

		functions = append(functions, PluginFunction{
			Name:        fullName,
			Handler:     handler,
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

func registerProcedureWithCypher(proc PluginProcedure) {
	handler, ok := proc.Handler.(func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error))
	if !ok {
		return
	}
	mode := cypher.ProcedureModeRead
	switch strings.ToUpper(proc.Mode) {
	case "WRITE":
		mode = cypher.ProcedureModeWrite
	case "SCHEMA":
		mode = cypher.ProcedureModeSchema
	case "ADMIN":
		mode = cypher.ProcedureModeAdmin
	case "DBMS":
		mode = cypher.ProcedureModeDBMS
	}
	_ = cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name:          proc.Name,
		Signature:     proc.Signature,
		Description:   proc.Description,
		Mode:          mode,
		WorksOnSystem: proc.WorksOnSystem,
		MinArgs:       proc.MinArgs,
		MaxArgs:       proc.MaxArgs,
	}, func(ctx context.Context, _ *cypher.StorageExecutor, cypherQuery string, args []interface{}) (*cypher.ExecuteResult, error) {
		return handler(ctx, cypherQuery, args)
	})
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
