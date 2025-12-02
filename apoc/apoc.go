// Package apoc provides APOC (Awesome Procedures On Cypher) functions for NornicDB.
//
// All functions are compiled into the binary and controlled via configuration,
// following Neo4j's APOC architecture.
package apoc

import (
	"fmt"
	"sync"

	"github.com/orneryd/nornicdb/apoc/agg"
	"github.com/orneryd/nornicdb/apoc/coll"
	"github.com/orneryd/nornicdb/apoc/convert"
	"github.com/orneryd/nornicdb/apoc/date"
	"github.com/orneryd/nornicdb/apoc/json"
	maputil "github.com/orneryd/nornicdb/apoc/map"
	"github.com/orneryd/nornicdb/apoc/math"
	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/apoc/text"
	"github.com/orneryd/nornicdb/apoc/util"
)

// Global state
var (
	functions map[string]Function
	config    *Config
	store     storage.Storage
	mu        sync.RWMutex
	initOnce  sync.Once
)

// Function represents a registered APOC function.
type Function struct {
	Name        string
	Category    string
	Handler     interface{}
	Description string
	Examples    []string
}

// Initialize sets up APOC with storage and configuration.
// This should be called once at application startup.
//
// If Config.PluginsDir is set, all .so files in that directory are
// automatically loaded as plugins (Option 3: Best of Both Worlds).
func Initialize(storage storage.Storage, cfg *Config) error {
	var err error
	initOnce.Do(func() {
		store = storage
		if cfg == nil {
			config = DefaultConfig()
		} else {
			config = cfg
		}

		functions = make(map[string]Function)
		err = registerAllFunctions()
		if err != nil {
			return
		}

		// Auto-load plugins from directory if configured (Option 3)
		if config.PluginsDir != "" {
			if pluginErr := LoadPluginsFromDir(config.PluginsDir); pluginErr != nil {
				// Log warning but don't fail - plugins are optional
				fmt.Printf("Warning: failed to load plugins from %s: %v\n", config.PluginsDir, pluginErr)
			}
		}
	})
	return err
}

// SetStorage updates the storage backend (for testing or reconfiguration).
func SetStorage(s storage.Storage) {
	mu.Lock()
	defer mu.Unlock()
	store = s
}

// Call executes an APOC function by name.
//
// Example:
//
//	result, err := apoc.Call("apoc.coll.sum", []interface{}{[]interface{}{1, 2, 3}})
func Call(name string, args []interface{}) (interface{}, error) {
	mu.RLock()
	defer mu.RUnlock()

	// Check if function is enabled
	if !config.IsEnabled(name) {
		return nil, fmt.Errorf("function %s is disabled by configuration", name)
	}

	// Get function
	fn, exists := functions[name]
	if !exists {
		return nil, fmt.Errorf("function %s not found", name)
	}

	// Call function based on type
	return callFunction(fn.Handler, args)
}

// IsAvailable checks if a function is registered and enabled.
func IsAvailable(name string) bool {
	mu.RLock()
	defer mu.RUnlock()

	_, exists := functions[name]
	return exists && config.IsEnabled(name)
}

// List returns all registered function names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(functions))
	for name := range functions {
		if config.IsEnabled(name) {
			names = append(names, name)
		}
	}
	return names
}

// ListByCategory returns all function names in a category.
func ListByCategory(category string) []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0)
	for name, fn := range functions {
		if fn.Category == category && config.IsEnabled(name) {
			names = append(names, name)
		}
	}
	return names
}

// Categories returns all registered categories.
func Categories() []string {
	mu.RLock()
	defer mu.RUnlock()

	cats := make(map[string]bool)
	for _, fn := range functions {
		if config.IsCategoryEnabled(fn.Category) {
			cats[fn.Category] = true
		}
	}

	result := make([]string, 0, len(cats))
	for cat := range cats {
		result = append(result, cat)
	}
	return result
}

// GetInfo returns information about a function.
func GetInfo(name string) (*Function, bool) {
	mu.RLock()
	defer mu.RUnlock()

	fn, exists := functions[name]
	if !exists || !config.IsEnabled(name) {
		return nil, false
	}

	return &fn, true
}

// register adds a function to the registry.
func register(name, category string, handler interface{}, description string, examples []string) {
	functions[name] = Function{
		Name:        name,
		Category:    category,
		Handler:     handler,
		Description: description,
		Examples:    examples,
	}
}

// registerAllFunctions registers all APOC functions at startup.
// This is called once during initialization.
func registerAllFunctions() error {
	// Collection functions
	register("apoc.coll.sum", "coll", coll.Sum, "Sum numeric values", []string{"apoc.coll.sum([1,2,3]) => 6"})
	register("apoc.coll.avg", "coll", coll.Avg, "Average of numeric values", []string{"apoc.coll.avg([1,2,3,4,5]) => 3.0"})
	register("apoc.coll.min", "coll", coll.Min, "Minimum value", []string{"apoc.coll.min([5,2,8,1,9]) => 1"})
	register("apoc.coll.max", "coll", coll.Max, "Maximum value", []string{"apoc.coll.max([5,2,8,1,9]) => 9"})
	register("apoc.coll.sort", "coll", coll.Sort, "Sort list ascending", []string{"apoc.coll.sort([3,1,4,1,5]) => [1,1,3,4,5]"})
	register("apoc.coll.reverse", "coll", coll.Reverse, "Reverse a list", []string{"apoc.coll.reverse([1,2,3]) => [3,2,1]"})
	register("apoc.coll.contains", "coll", coll.Contains, "Check if list contains value", []string{"apoc.coll.contains([1,2,3], 2) => true"})
	register("apoc.coll.containsAll", "coll", coll.ContainsAll, "Check if list contains all values", []string{"apoc.coll.containsAll([1,2,3,4], [2,4]) => true"})
	register("apoc.coll.union", "coll", coll.Union, "Union of lists", []string{"apoc.coll.union([1,2], [2,3]) => [1,2,3]"})
	register("apoc.coll.intersection", "coll", coll.Intersection, "Intersection of lists", []string{"apoc.coll.intersection([1,2,3], [2,3,4]) => [2,3]"})
	register("apoc.coll.subtract", "coll", coll.Subtract, "Elements in first not in second", []string{"apoc.coll.subtract([1,2,3], [2,4]) => [1,3]"})
	register("apoc.coll.flatten", "coll", coll.Flatten, "Flatten nested lists", []string{"apoc.coll.flatten([[1,2],[3,4]], true) => [1,2,3,4]"})
	register("apoc.coll.toSet", "coll", coll.ToSet, "Remove duplicates", []string{"apoc.coll.toSet([1,2,1,3]) => [1,2,3]"})
	register("apoc.coll.frequencies", "coll", coll.Frequencies, "Count occurrences", []string{"apoc.coll.frequencies([1,2,2,3]) => {1:1, 2:2, 3:1}"})

	// Text functions
	register("apoc.text.join", "text", text.Join, "Join strings", []string{"apoc.text.join(['Hello', 'World'], ' ') => 'Hello World'"})
	register("apoc.text.split", "text", text.Split, "Split string", []string{"apoc.text.split('Hello World', ' ') => ['Hello', 'World']"})
	register("apoc.text.replace", "text", text.Replace, "Replace substring", []string{"apoc.text.replace('Hello', 'llo', 'y') => 'Hey'"})
	register("apoc.text.capitalize", "text", text.Capitalize, "Capitalize words", []string{"apoc.text.capitalize('hello world') => 'Hello World'"})
	register("apoc.text.camelCase", "text", text.CamelCase, "Convert to camelCase", []string{"apoc.text.camelCase('hello world') => 'helloWorld'"})
	register("apoc.text.snakeCase", "text", text.SnakeCase, "Convert to snake_case", []string{"apoc.text.snakeCase('HelloWorld') => 'hello_world'"})
	register("apoc.text.distance", "text", text.Distance, "Levenshtein distance", []string{"apoc.text.distance('kitten', 'sitting') => 3"})
	register("apoc.text.jaroWinklerDistance", "text", text.JaroWinklerDistance, "Jaro-Winkler similarity", []string{"apoc.text.jaroWinklerDistance('martha', 'marhta') => 0.96"})
	register("apoc.text.phonetic", "text", text.Phonetic, "Soundex encoding", []string{"apoc.text.phonetic('Smith') => 'S530'"})
	register("apoc.text.slug", "text", text.Slug, "URL-friendly slug", []string{"apoc.text.slug('Hello World!') => 'hello-world'"})

	// Math functions
	register("apoc.math.maxLong", "math", math.MaxLong, "Maximum integer", []string{"apoc.math.maxLong(5, 2, 8) => 8"})
	register("apoc.math.minLong", "math", math.MinLong, "Minimum integer", []string{"apoc.math.minLong(5, 2, 8) => 2"})
	register("apoc.math.round", "math", math.Round, "Round number", []string{"apoc.math.round(3.14159, 2) => 3.14"})
	register("apoc.math.ceil", "math", math.Ceil, "Round up", []string{"apoc.math.ceil(3.14) => 4.0"})
	register("apoc.math.floor", "math", math.Floor, "Round down", []string{"apoc.math.floor(3.14) => 3.0"})
	register("apoc.math.abs", "math", math.Abs, "Absolute value", []string{"apoc.math.abs(-5.5) => 5.5"})
	register("apoc.math.pow", "math", math.Pow, "Power", []string{"apoc.math.pow(2, 8) => 256.0"})
	register("apoc.math.sqrt", "math", math.Sqrt, "Square root", []string{"apoc.math.sqrt(16) => 4.0"})
	register("apoc.math.mean", "math", math.Mean, "Arithmetic mean", []string{"apoc.math.mean([1,2,3,4,5]) => 3.0"})
	register("apoc.math.median", "math", math.Median, "Median value", []string{"apoc.math.median([1,2,3,4,5]) => 3.0"})

	// Convert functions
	register("apoc.convert.toBoolean", "convert", convert.ToBoolean, "Convert to boolean", []string{"apoc.convert.toBoolean('true') => true"})
	register("apoc.convert.toInteger", "convert", convert.ToInteger, "Convert to integer", []string{"apoc.convert.toInteger('42') => 42"})
	register("apoc.convert.toFloat", "convert", convert.ToFloat, "Convert to float", []string{"apoc.convert.toFloat('3.14') => 3.14"})
	register("apoc.convert.toString", "convert", convert.ToString, "Convert to string", []string{"apoc.convert.toString(42) => '42'"})
	register("apoc.convert.toJson", "convert", convert.ToJson, "Convert to JSON", []string{"apoc.convert.toJson({name:'Alice'}) => '{\"name\":\"Alice\"}'"})

	// Map functions
	register("apoc.map.fromPairs", "map", maputil.FromPairs, "Create map from pairs", []string{"apoc.map.fromPairs([['a',1],['b',2]]) => {a:1, b:2}"})
	register("apoc.map.merge", "map", maputil.Merge, "Merge maps", []string{"apoc.map.merge({a:1}, {b:2}) => {a:1, b:2}"})
	register("apoc.map.keys", "map", maputil.Keys, "Get map keys", []string{"apoc.map.keys({a:1, b:2}) => ['a', 'b']"})
	register("apoc.map.values", "map", maputil.Values, "Get map values", []string{"apoc.map.values({a:1, b:2}) => [1, 2]"})

	// Date functions
	register("apoc.date.currentTimestamp", "date", date.CurrentTimestamp, "Current timestamp", []string{"apoc.date.currentTimestamp() => 1705276800"})
	register("apoc.date.format", "date", date.Format, "Format timestamp", []string{"apoc.date.format(1705276800, 'yyyy-MM-dd') => '2024-01-15'"})
	register("apoc.date.parse", "date", date.Parse, "Parse date string", []string{"apoc.date.parse('2024-01-15', 'yyyy-MM-dd') => 1705276800"})

	// JSON functions
	register("apoc.json.parse", "json", json.Parse, "Parse JSON", []string{"apoc.json.parse('{\"name\":\"Alice\"}') => {name:'Alice'}"})
	register("apoc.json.stringify", "json", json.Stringify, "Convert to JSON string", []string{"apoc.json.stringify({name:'Alice'}) => '{\"name\":\"Alice\"}'"})
	register("apoc.json.validate", "json", json.Validate, "Validate JSON", []string{"apoc.json.validate('{\"name\":\"Alice\"}') => true"})

	// Util functions
	register("apoc.util.md5", "util", util.MD5, "MD5 hash", []string{"apoc.util.md5('hello') => '5d41402abc4b2a76b9719d911017c592'"})
	register("apoc.util.sha1", "util", util.SHA1, "SHA1 hash", []string{"apoc.util.sha1('hello') => 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'"})
	register("apoc.util.sha256", "util", util.SHA256, "SHA256 hash", []string{"apoc.util.sha256('hello') => '2cf24dba5fb0a30e...'"})
	register("apoc.util.uuid", "util", util.UUID, "Generate UUID", []string{"apoc.util.uuid() => '550e8400-e29b-41d4-a716-446655440000'"})

	// Aggregation functions
	register("apoc.agg.median", "agg", agg.Median, "Median value", []string{"apoc.agg.median([1,2,3,4,5]) => 3.0"})
	register("apoc.agg.percentile", "agg", agg.Percentile, "Percentile value", []string{"apoc.agg.percentile([1,2,3,4,5], 0.95) => 4.8"})
	register("apoc.agg.stdev", "agg", agg.StdDev, "Standard deviation", []string{"apoc.agg.stdev([2,4,4,4,5,5,7,9]) => 2.0"})

	// TODO: Add remaining 400+ functions
	// This is a starter set - add more as needed

	return nil
}

// callFunction invokes a function with type conversion.
func callFunction(handler interface{}, args []interface{}) (interface{}, error) {
	// Simple type-based dispatch
	// In production, use reflection for full type safety

	switch fn := handler.(type) {
	// Collection functions
	case func([]interface{}) float64:
		if len(args) != 1 {
			return nil, fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		list, ok := args[0].([]interface{})
		if !ok {
			return nil, fmt.Errorf("expected list argument")
		}
		return fn(list), nil

	case func([]interface{}) interface{}:
		if len(args) != 1 {
			return nil, fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		list, ok := args[0].([]interface{})
		if !ok {
			return nil, fmt.Errorf("expected list argument")
		}
		return fn(list), nil

	case func([]interface{}, interface{}) bool:
		if len(args) != 2 {
			return nil, fmt.Errorf("expected 2 arguments, got %d", len(args))
		}
		list, ok := args[0].([]interface{})
		if !ok {
			return nil, fmt.Errorf("expected list as first argument")
		}
		return fn(list, args[1]), nil

	// Text functions
	case func([]string, string) string:
		if len(args) != 2 {
			return nil, fmt.Errorf("expected 2 arguments, got %d", len(args))
		}
		list, ok := args[0].([]string)
		if !ok {
			return nil, fmt.Errorf("expected string list")
		}
		delim, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("expected string delimiter")
		}
		return fn(list, delim), nil

	case func(string, string) []string:
		if len(args) != 2 {
			return nil, fmt.Errorf("expected 2 arguments, got %d", len(args))
		}
		text, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("expected string")
		}
		delim, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("expected string delimiter")
		}
		return fn(text, delim), nil

	case func(string) string:
		if len(args) != 1 {
			return nil, fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		text, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("expected string")
		}
		return fn(text), nil

	case func(string, string) int:
		if len(args) != 2 {
			return nil, fmt.Errorf("expected 2 arguments, got %d", len(args))
		}
		s1, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("expected string")
		}
		s2, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("expected string")
		}
		return fn(s1, s2), nil

	// Add more type signatures as needed

	default:
		return nil, fmt.Errorf("unsupported function signature")
	}
}
