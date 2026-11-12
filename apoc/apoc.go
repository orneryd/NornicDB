// Package apoc provides APOC (Awesome Procedures On Cypher) functions for NornicDB.
//
// All functions are compiled into the binary and controlled via configuration,
// following Neo4j's APOC architecture.
package apoc

import (
	"errors"
	"fmt"
	"sync"

	"github.com/orneryd/nornicdb/apoc/registry"

	"github.com/orneryd/nornicdb/apoc/agg"
	"github.com/orneryd/nornicdb/apoc/algo"
	"github.com/orneryd/nornicdb/apoc/atomic"
	"github.com/orneryd/nornicdb/apoc/bitwise"
	"github.com/orneryd/nornicdb/apoc/coll"
	"github.com/orneryd/nornicdb/apoc/community"
	"github.com/orneryd/nornicdb/apoc/convert"
	"github.com/orneryd/nornicdb/apoc/create"
	apoccypher "github.com/orneryd/nornicdb/apoc/cypher"
	"github.com/orneryd/nornicdb/apoc/date"
	"github.com/orneryd/nornicdb/apoc/diff"
	apocexport "github.com/orneryd/nornicdb/apoc/export"
	"github.com/orneryd/nornicdb/apoc/graph"
	"github.com/orneryd/nornicdb/apoc/hashing"
	apocimport "github.com/orneryd/nornicdb/apoc/import"
	"github.com/orneryd/nornicdb/apoc/json"
	"github.com/orneryd/nornicdb/apoc/label"
	"github.com/orneryd/nornicdb/apoc/load"
	"github.com/orneryd/nornicdb/apoc/lock"
	apoclog "github.com/orneryd/nornicdb/apoc/log"
	maputil "github.com/orneryd/nornicdb/apoc/map"
	"github.com/orneryd/nornicdb/apoc/math"
	"github.com/orneryd/nornicdb/apoc/merge"
	"github.com/orneryd/nornicdb/apoc/meta"
	"github.com/orneryd/nornicdb/apoc/neighbors"
	apocnode "github.com/orneryd/nornicdb/apoc/node"
	"github.com/orneryd/nornicdb/apoc/nodes"
	"github.com/orneryd/nornicdb/apoc/number"
	"github.com/orneryd/nornicdb/apoc/path"
	"github.com/orneryd/nornicdb/apoc/paths"
	"github.com/orneryd/nornicdb/apoc/periodic"
	"github.com/orneryd/nornicdb/apoc/refactor"
	apocrel "github.com/orneryd/nornicdb/apoc/rel"
	"github.com/orneryd/nornicdb/apoc/schema"
	"github.com/orneryd/nornicdb/apoc/scoring"
	"github.com/orneryd/nornicdb/apoc/search"
	"github.com/orneryd/nornicdb/apoc/spatial"
	"github.com/orneryd/nornicdb/apoc/stats"
	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/apoc/temporal"
	"github.com/orneryd/nornicdb/apoc/text"
	"github.com/orneryd/nornicdb/apoc/trigger"
	"github.com/orneryd/nornicdb/apoc/util"
	"github.com/orneryd/nornicdb/apoc/warmup"
	"github.com/orneryd/nornicdb/apoc/xml"
)

// Global state. The registered functions are one immutable registry bound to
// one storage backend; a reconfiguration publishes a complete replacement
// instead of rebinding the functions an admitted caller already holds.
var (
	config   *Config
	mu       sync.RWMutex
	initOnce sync.Once
	initErr  error
	active   *functionRegistry
)

// functionRegistry is one complete publication of the APOC function table bound
// to a single storage backend. Nothing mutates it after publication, so a
// retained *registry.Callable always answers from the backend it was built for.
type functionRegistry struct {
	backend   storage.Storage
	functions map[string]Function
}

// boundFunctions are the backend-bound module instances one registry uses.
// Helpers with no backend or instance-state dependency stay free functions and
// are registered directly.
type boundFunctions struct {
	label     *label.Functions
	meta      *meta.BoundFunctions
	neighbors *neighbors.Functions
	node      *apocnode.Functions
	nodes     *nodes.Functions
	path      *path.Functions
	paths     *paths.Functions
	refactor  *refactor.Functions
	rel       *apocrel.Functions
	schema    *schema.Functions
	search    *search.Functions
	stats     *stats.Functions
	warmup    *warmup.Functions
}

// newBoundFunctions builds every module instance for one backend.
func newBoundFunctions(backend storage.Storage) *boundFunctions {
	return &boundFunctions{
		label:     label.New(backend),
		meta:      meta.New(backend),
		neighbors: neighbors.New(backend),
		node:      apocnode.New(backend),
		nodes:     nodes.New(backend),
		path:      path.New(backend),
		paths:     paths.New(backend),
		refactor:  refactor.New(backend),
		rel:       apocrel.New(backend),
		schema:    schema.New(backend),
		search:    search.New(backend),
		stats:     stats.New(backend),
		warmup:    warmup.New(backend),
	}
}

// buildRegistry compiles a complete function table for backend. A registration
// failure returns the error and publishes nothing.
func buildRegistry(backend storage.Storage) (*functionRegistry, error) {
	functions := make(map[string]Function)
	if err := registerAllFunctions(functions, newBoundFunctions(backend)); err != nil {
		return nil, err
	}
	return &functionRegistry{backend: backend, functions: functions}, nil
}

// Function represents a registered APOC function.
type Function struct {
	Name        string
	Category    string
	Handler     *registry.Callable
	Description string
	Examples    []string
}

// Initialize sets up APOC with storage and configuration.
// This should be called once at application startup. The registry it publishes
// is bound to the supplied backend; module helpers keep that backend for their
// whole lifetime.
func Initialize(storage storage.Storage, cfg *Config) error {
	initOnce.Do(func() {
		if cfg == nil {
			config = DefaultConfig()
		} else {
			config = cfg
		}

		built, err := buildRegistry(storage)
		if err != nil {
			initErr = err
			return
		}

		mu.Lock()
		active = built
		mu.Unlock()
	})
	return initErr
}

// SetStorage publishes a new default registry bound to s for future lookups.
// Handlers that were already admitted keep the backend they were built with:
// a *registry.Callable retains its own bound function, and nothing rebinds it.
// A registration failure publishes nothing and leaves the previous registry
// authoritative, so the error is the only effect of a rejected rebuild.
func SetStorage(s storage.Storage) error {
	built, err := buildRegistry(s)
	if err != nil {
		return err
	}

	mu.Lock()
	active = built
	mu.Unlock()
	return nil
}

// GetStorage returns the storage backend of the published default registry.
// Handlers are bound to that backend when the registry is built; this is the
// registry's own backend, not a rebindable owner. It is nil before Initialize
// publishes a registry.
func GetStorage() storage.Storage {
	mu.RLock()
	defer mu.RUnlock()

	if active == nil {
		return nil
	}
	return active.backend
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

	// Get function from the registry published for this backend
	if active == nil {
		return nil, fmt.Errorf("function %s not found", name)
	}
	fn, exists := active.functions[name]
	if !exists {
		return nil, fmt.Errorf("function %s not found", name)
	}

	// Invoke the admitted owner with the root catalog's argument policy.
	return fn.Handler.CallWithPolicy(args, nil, registry.APOCArguments)
}

// IsAvailable checks if a function is registered and enabled.
func IsAvailable(name string) bool {
	mu.RLock()
	defer mu.RUnlock()

	if active == nil {
		return false
	}

	_, exists := active.functions[name]
	return exists && config.IsEnabled(name)
}

// List returns all registered function names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	if active == nil {
		return []string{}
	}

	names := make([]string, 0, len(active.functions))
	for name := range active.functions {
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

	if active == nil {
		return []string{}
	}

	names := make([]string, 0)
	for name, fn := range active.functions {
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
	if active == nil {
		return []string{}
	}
	for _, fn := range active.functions {
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

	if active == nil {
		return nil, false
	}
	fn, exists := active.functions[name]
	if !exists || !config.IsEnabled(name) {
		return nil, false
	}

	return &fn, true
}

// register adds a function to the table under construction.
// Author: Codex NOR-184 lane. Admission occurs before publishing a handler.
func registerFunction(functions map[string]Function, name, category string, function interface{}, observation registry.ReadObservation, description string, examples []string) error {
	handler, err := registry.Compile(function, observation)
	if errors.Is(err, registry.ErrNonScalarResult) {
		// Existing tuple declarations remain metadata-only, as before: neither
		// their values nor an error result can be silently selected.
		handler, err = registry.DescribeNonScalar(function)
	}
	if err != nil {
		return fmt.Errorf("register %s: %w", name, err)
	}
	functions[name] = Function{
		Name:        name,
		Category:    category,
		Handler:     handler,
		Description: description,
		Examples:    examples,
	}
	return nil
}

// registerAllFunctions builds one complete function table for the backend-bound
// module instances in fns. It writes only into the table it was given, so a
// registration failure leaves the published registry untouched.
//
// Every registration states the read-observation policy its own implementation
// was reviewed under. A bundled helper that reads no database state declares
// registry.CanonicalReads; a backend-bound module handler that reads storage
// through a path which does not report logical rows and access units declares
// registry.OpaqueReads until its reads are instrumented. Helpers that invoke
// caller-supplied callbacks also stay opaque. A name, a category and
// a sibling's policy prove nothing about an implementation.
func registerAllFunctions(functions map[string]Function, fns *boundFunctions) error {
	var registrationError error
	register := func(name, category string, function interface{}, observation registry.ReadObservation, description string, examples []string) {
		registrationError = errors.Join(registrationError, registerFunction(functions, name, category, function, observation, description, examples))
	}
	// ========================================
	// Collection Functions (apoc.coll.*)
	// ========================================
	register("apoc.coll.sum", "coll", coll.Sum, registry.CanonicalReads, "Sum numeric values", []string{"apoc.coll.sum([1,2,3]) => 6"})
	register("apoc.coll.avg", "coll", coll.Avg, registry.CanonicalReads, "Average of numeric values", []string{"apoc.coll.avg([1,2,3,4,5]) => 3.0"})
	register("apoc.coll.min", "coll", coll.Min, registry.CanonicalReads, "Minimum value", []string{"apoc.coll.min([5,2,8,1,9]) => 1"})
	register("apoc.coll.max", "coll", coll.Max, registry.CanonicalReads, "Maximum value", []string{"apoc.coll.max([5,2,8,1,9]) => 9"})
	register("apoc.coll.sort", "coll", coll.Sort, registry.CanonicalReads, "Sort list ascending", []string{"apoc.coll.sort([3,1,4,1,5]) => [1,1,3,4,5]"})
	register("apoc.coll.sortMaps", "coll", coll.SortMaps, registry.CanonicalReads, "Sort maps by key", []string{"apoc.coll.sortMaps([{age:30},{age:20}], 'age') => [{age:20},{age:30}]"})
	register("apoc.coll.reverse", "coll", coll.Reverse, registry.CanonicalReads, "Reverse a list", []string{"apoc.coll.reverse([1,2,3]) => [3,2,1]"})
	register("apoc.coll.contains", "coll", coll.Contains, registry.CanonicalReads, "Check if list contains value", []string{"apoc.coll.contains([1,2,3], 2) => true"})
	register("apoc.coll.containsAll", "coll", coll.ContainsAll, registry.CanonicalReads, "Check if list contains all values", []string{"apoc.coll.containsAll([1,2,3,4], [2,4]) => true"})
	register("apoc.coll.containsAny", "coll", coll.ContainsAny, registry.CanonicalReads, "Check if list contains any value", []string{"apoc.coll.containsAny([1,2,3], [3,4,5]) => true"})
	register("apoc.coll.containsSorted", "coll", coll.ContainsSorted, registry.CanonicalReads, "Binary search in sorted list", []string{"apoc.coll.containsSorted([1,2,3,4,5], 3) => true"})
	register("apoc.coll.containsDuplicates", "coll", coll.ContainsDuplicates, registry.CanonicalReads, "Check for duplicates", []string{"apoc.coll.containsDuplicates([1,2,3,2]) => true"})
	register("apoc.coll.union", "coll", coll.Union, registry.CanonicalReads, "Union of lists", []string{"apoc.coll.union([1,2], [2,3]) => [1,2,3]"})
	register("apoc.coll.unionAll", "coll", coll.UnionAll, registry.CanonicalReads, "Union with duplicates", []string{"apoc.coll.unionAll([1,2], [2,3]) => [1,2,2,3]"})
	register("apoc.coll.intersection", "coll", coll.Intersection, registry.CanonicalReads, "Intersection of lists", []string{"apoc.coll.intersection([1,2,3], [2,3,4]) => [2,3]"})
	register("apoc.coll.subtract", "coll", coll.Subtract, registry.CanonicalReads, "Elements in first not in second", []string{"apoc.coll.subtract([1,2,3], [2,4]) => [1,3]"})
	register("apoc.coll.disjunction", "coll", coll.Disjunction, registry.CanonicalReads, "Elements in either but not both", []string{"apoc.coll.disjunction([1,2,3], [2,3,4]) => [1,4]"})
	register("apoc.coll.flatten", "coll", coll.Flatten, registry.CanonicalReads, "Flatten nested lists", []string{"apoc.coll.flatten([[1,2],[3,4]], true) => [1,2,3,4]"})
	register("apoc.coll.toSet", "coll", coll.ToSet, registry.CanonicalReads, "Remove duplicates", []string{"apoc.coll.toSet([1,2,1,3]) => [1,2,3]"})
	register("apoc.coll.indexOf", "coll", coll.IndexOf, registry.CanonicalReads, "Find index of value", []string{"apoc.coll.indexOf([1,2,3], 2) => 1"})
	register("apoc.coll.split", "coll", coll.Split, registry.CanonicalReads, "Split list into chunks", []string{"apoc.coll.split([1,2,3,4,5], 2) => [[1,2],[3,4],[5]]"})
	register("apoc.coll.pairs", "coll", coll.Pairs, registry.CanonicalReads, "Consecutive pairs", []string{"apoc.coll.pairs([1,2,3]) => [[1,2],[2,3]]"})
	register("apoc.coll.pairsMin", "coll", coll.PairsMin, registry.CanonicalReads, "Non-overlapping pairs", []string{"apoc.coll.pairsMin([1,2,3,4]) => [[1,2],[3,4]]"})
	register("apoc.coll.zip", "coll", coll.Zip, registry.CanonicalReads, "Zip multiple lists", []string{"apoc.coll.zip([1,2], ['a','b']) => [[1,'a'],[2,'b']]"})
	register("apoc.coll.frequencies", "coll", coll.Frequencies, registry.CanonicalReads, "Count occurrences", []string{"apoc.coll.frequencies([1,2,2,3]) => [{item:1,count:1},{item:2,count:2},{item:3,count:1}]"})
	register("apoc.coll.frequenciesAsMap", "coll", coll.FrequenciesAsMap, registry.CanonicalReads, "Frequencies as a map", []string{"apoc.coll.frequenciesAsMap([1,2,2]) => {1:1, 2:2}"})
	register("apoc.coll.occurrences", "coll", coll.Occurrences, registry.CanonicalReads, "Count specific value", []string{"apoc.coll.occurrences([1,2,2,3], 2) => 2"})
	register("apoc.coll.duplicates", "coll", coll.Duplicates, registry.CanonicalReads, "Find duplicate values", []string{"apoc.coll.duplicates([1,2,3,2,3]) => [2,3]"})
	register("apoc.coll.duplicatesWithCount", "coll", coll.DuplicatesWithCount, registry.CanonicalReads, "Duplicates with counts", []string{"apoc.coll.duplicatesWithCount([1,2,2,3,3,3]) => [{item:2,count:2},{item:3,count:3}]"})
	register("apoc.coll.dropDuplicateNeighbors", "coll", coll.DropDuplicateNeighbors, registry.CanonicalReads, "Remove consecutive duplicates", []string{"apoc.coll.dropDuplicateNeighbors([1,1,2,2,3]) => [1,2,3]"})
	register("apoc.coll.fill", "coll", coll.Fill, registry.CanonicalReads, "Create list with repeated value", []string{"apoc.coll.fill('x', 3) => ['x','x','x']"})
	register("apoc.coll.insert", "coll", coll.Insert, registry.CanonicalReads, "Insert value at index", []string{"apoc.coll.insert([1,2,4], 2, 3) => [1,2,3,4]"})
	register("apoc.coll.insertAll", "coll", coll.InsertAll, registry.CanonicalReads, "Insert list at index", []string{"apoc.coll.insertAll([1,4], 1, [2,3]) => [1,2,3,4]"})
	register("apoc.coll.remove", "coll", coll.Remove, registry.CanonicalReads, "Remove at index", []string{"apoc.coll.remove([1,2,3], 1) => [1,3]"})
	register("apoc.coll.removeAll", "coll", coll.RemoveAll, registry.CanonicalReads, "Remove all occurrences", []string{"apoc.coll.removeAll([1,2,3,2], 2) => [1,3]"})
	register("apoc.coll.set", "coll", coll.Set, registry.CanonicalReads, "Set value at index", []string{"apoc.coll.set([1,2,3], 1, 9) => [1,9,3]"})
	register("apoc.coll.slice", "coll", coll.Slice, registry.CanonicalReads, "Sublist", []string{"apoc.coll.slice([1,2,3,4,5], 1, 4) => [2,3,4]"})
	register("apoc.coll.shuffle", "coll", coll.Shuffle, registry.CanonicalReads, "Shuffle list", []string{"apoc.coll.shuffle([1,2,3,4,5]) => [3,1,5,2,4]"})
	register("apoc.coll.randomItem", "coll", coll.RandomItem, registry.CanonicalReads, "Random item", []string{"apoc.coll.randomItem([1,2,3]) => 2"})
	register("apoc.coll.randomItems", "coll", coll.RandomItems, registry.CanonicalReads, "Random items", []string{"apoc.coll.randomItems([1,2,3,4,5], 2) => [3,1]"})
	register("apoc.coll.isEmpty", "coll", coll.IsEmpty, registry.CanonicalReads, "Check if empty", []string{"apoc.coll.isEmpty([]) => true"})
	register("apoc.coll.isNotEmpty", "coll", coll.IsNotEmpty, registry.CanonicalReads, "Check if not empty", []string{"apoc.coll.isNotEmpty([1]) => true"})
	register("apoc.coll.different", "coll", coll.Different, registry.CanonicalReads, "Elements in first not in second", []string{"apoc.coll.different([1,2,3], [2,4]) => [1,3]"})
	register("apoc.coll.sumLongs", "coll", coll.SumLongs, registry.CanonicalReads, "Sum as integer", []string{"apoc.coll.sumLongs([1,2,3]) => 6"})
	register("apoc.coll.partition", "coll", coll.Partition, registry.OpaqueReads, "Partition list by predicate", []string{"apoc.coll.partition([1,2,3,4], fn) => [[evens],[odds]]"})

	// ========================================
	// Text Functions (apoc.text.*)
	// ========================================
	register("apoc.text.join", "text", text.Join, registry.CanonicalReads, "Join strings", []string{"apoc.text.join(['Hello', 'World'], ' ') => 'Hello World'"})
	register("apoc.text.split", "text", text.Split, registry.CanonicalReads, "Split string", []string{"apoc.text.split('Hello World', ' ') => ['Hello', 'World']"})
	register("apoc.text.replace", "text", text.Replace, registry.CanonicalReads, "Replace substring", []string{"apoc.text.replace('Hello', 'llo', 'y') => 'Hey'"})
	register("apoc.text.regexGroups", "text", text.RegexGroups, registry.CanonicalReads, "Extract regex groups", []string{"apoc.text.regexGroups('abc123', '([a-z]+)([0-9]+)') => [['abc','123']]"})
	register("apoc.text.capitalize", "text", text.Capitalize, registry.CanonicalReads, "Capitalize first letter", []string{"apoc.text.capitalize('hello') => 'Hello'"})
	register("apoc.text.capitalizeAll", "text", text.CapitalizeAll, registry.CanonicalReads, "Capitalize all words", []string{"apoc.text.capitalizeAll('hello world') => 'Hello World'"})
	register("apoc.text.decapitalize", "text", text.Decapitalize, registry.CanonicalReads, "Lowercase first letter", []string{"apoc.text.decapitalize('Hello') => 'hello'"})
	register("apoc.text.decapitalizeAll", "text", text.DecapitalizeAll, registry.CanonicalReads, "Lowercase all words", []string{"apoc.text.decapitalizeAll('Hello World') => 'hello world'"})
	register("apoc.text.swapCase", "text", text.SwapCase, registry.CanonicalReads, "Swap case", []string{"apoc.text.swapCase('Hello') => 'hELLO'"})
	register("apoc.text.camelCase", "text", text.CamelCase, registry.CanonicalReads, "Convert to camelCase", []string{"apoc.text.camelCase('hello world') => 'helloWorld'"})
	register("apoc.text.snakeCase", "text", text.SnakeCase, registry.CanonicalReads, "Convert to snake_case", []string{"apoc.text.snakeCase('HelloWorld') => 'hello_world'"})
	register("apoc.text.upperCamelCase", "text", text.UpperCamelCase, registry.CanonicalReads, "Convert to UpperCamelCase", []string{"apoc.text.upperCamelCase('hello world') => 'HelloWorld'"})
	register("apoc.text.clean", "text", text.Clean, registry.CanonicalReads, "Clean whitespace", []string{"apoc.text.clean('  hello  world  ') => 'hello world'"})
	register("apoc.text.compareCleaned", "text", text.CompareCleaned, registry.CanonicalReads, "Compare cleaned strings", []string{"apoc.text.compareCleaned(' A ', 'a') => true"})
	register("apoc.text.distance", "text", text.Distance, registry.CanonicalReads, "Levenshtein distance", []string{"apoc.text.distance('kitten', 'sitting') => 3"})
	register("apoc.text.fuzzyMatch", "text", text.FuzzyMatch, registry.CanonicalReads, "Fuzzy string match", []string{"apoc.text.fuzzyMatch('hello', 'helo') => true"})
	register("apoc.text.hammingDistance", "text", text.HammingDistance, registry.CanonicalReads, "Hamming distance", []string{"apoc.text.hammingDistance('karolin', 'kathrin') => 3"})
	register("apoc.text.jaroWinklerDistance", "text", text.JaroWinklerDistance, registry.CanonicalReads, "Jaro-Winkler similarity", []string{"apoc.text.jaroWinklerDistance('martha', 'marhta') => 0.96"})
	register("apoc.text.lpad", "text", text.Lpad, registry.CanonicalReads, "Left pad string", []string{"apoc.text.lpad('123', 5, '0') => '00123'"})
	register("apoc.text.rpad", "text", text.Rpad, registry.CanonicalReads, "Right pad string", []string{"apoc.text.rpad('123', 5, '0') => '12300'"})
	register("apoc.text.format", "text", text.Format, registry.CanonicalReads, "Format string", []string{"apoc.text.format('Hello %s', ['World']) => 'Hello World'"})
	register("apoc.text.repeat", "text", text.Repeat, registry.CanonicalReads, "Repeat string", []string{"apoc.text.repeat('ab', 3) => 'ababab'"})
	register("apoc.text.reverse", "text", text.Reverse, registry.CanonicalReads, "Reverse string", []string{"apoc.text.reverse('hello') => 'olleh'"})
	register("apoc.text.slug", "text", text.Slug, registry.CanonicalReads, "URL-friendly slug", []string{"apoc.text.slug('Hello World!') => 'hello-world'"})
	register("apoc.text.sorensenDiceSimilarity", "text", text.SorensenDiceSimilarity, registry.CanonicalReads, "Sorensen-Dice similarity", []string{"apoc.text.sorensenDiceSimilarity('night', 'nacht') => 0.25"})
	register("apoc.text.trim", "text", text.Trim, registry.CanonicalReads, "Trim whitespace", []string{"apoc.text.trim('  hello  ') => 'hello'"})
	register("apoc.text.ltrim", "text", text.Ltrim, registry.CanonicalReads, "Trim left whitespace", []string{"apoc.text.ltrim('  hello') => 'hello'"})
	register("apoc.text.rtrim", "text", text.Rtrim, registry.CanonicalReads, "Trim right whitespace", []string{"apoc.text.rtrim('hello  ') => 'hello'"})
	register("apoc.text.urlencode", "text", text.Urlencode, registry.CanonicalReads, "URL encode", []string{"apoc.text.urlencode('hello world') => 'hello%20world'"})
	register("apoc.text.urldecode", "text", text.Urldecode, registry.CanonicalReads, "URL decode", []string{"apoc.text.urldecode('hello%20world') => 'hello world'"})
	register("apoc.text.base64Encode", "text", text.Base64Encode, registry.CanonicalReads, "Base64 encode", []string{"apoc.text.base64Encode('hello') => 'aGVsbG8='"})
	register("apoc.text.base64Decode", "text", text.Base64Decode, registry.CanonicalReads, "Base64 decode", []string{"apoc.text.base64Decode('aGVsbG8=') => 'hello'"})
	register("apoc.text.indexOf", "text", text.IndexOf, registry.CanonicalReads, "Find index of substring", []string{"apoc.text.indexOf('hello', 'l') => 2"})
	register("apoc.text.indexesOf", "text", text.IndexesOf, registry.CanonicalReads, "Find all indexes", []string{"apoc.text.indexesOf('hello', 'l') => [2,3]"})
	register("apoc.text.code", "text", text.Code, registry.CanonicalReads, "Get Unicode code point", []string{"apoc.text.code('A') => 65"})
	register("apoc.text.fromCodePoint", "text", text.FromCodePoint, registry.CanonicalReads, "From code point", []string{"apoc.text.fromCodePoint(65) => 'A'"})
	register("apoc.text.bytes", "text", text.Bytes, registry.CanonicalReads, "Get bytes", []string{"apoc.text.bytes('hello') => [104,101,108,108,111]"})
	register("apoc.text.bytesToString", "text", text.BytesToString, registry.CanonicalReads, "Bytes to string", []string{"apoc.text.bytesToString([104,101,108,108,111]) => 'hello'"})
	register("apoc.text.phonetic", "text", text.Phonetic, registry.CanonicalReads, "Soundex encoding", []string{"apoc.text.phonetic('Smith') => 'S530'"})
	register("apoc.text.phoneticDelta", "text", text.PhoneticDelta, registry.CanonicalReads, "Phonetic similarity", []string{"apoc.text.phoneticDelta('Smith', 'Smythe') => 4"})
	register("apoc.text.doubleMetaphone", "text", text.DoubleMetaphone, registry.CanonicalReads, "Double Metaphone encoding", []string{"apoc.text.doubleMetaphone('Smith') => 'SM0'"})

	// ========================================
	// Math Functions (apoc.math.*)
	// ========================================
	register("apoc.math.maxLong", "math", math.MaxLong, registry.CanonicalReads, "Maximum integer", []string{"apoc.math.maxLong(5, 2, 8) => 8"})
	register("apoc.math.minLong", "math", math.MinLong, registry.CanonicalReads, "Minimum integer", []string{"apoc.math.minLong(5, 2, 8) => 2"})
	register("apoc.math.maxDouble", "math", math.MaxDouble, registry.CanonicalReads, "Maximum float", []string{"apoc.math.maxDouble(5.5, 2.2, 8.8) => 8.8"})
	register("apoc.math.minDouble", "math", math.MinDouble, registry.CanonicalReads, "Minimum float", []string{"apoc.math.minDouble(5.5, 2.2, 8.8) => 2.2"})
	register("apoc.math.round", "math", math.Round, registry.CanonicalReads, "Round number", []string{"apoc.math.round(3.14159, 2) => 3.14"})
	register("apoc.math.ceil", "math", math.Ceil, registry.CanonicalReads, "Round up", []string{"apoc.math.ceil(3.14) => 4.0"})
	register("apoc.math.floor", "math", math.Floor, registry.CanonicalReads, "Round down", []string{"apoc.math.floor(3.14) => 3.0"})
	register("apoc.math.abs", "math", math.Abs, registry.CanonicalReads, "Absolute value", []string{"apoc.math.abs(-5.5) => 5.5"})
	register("apoc.math.pow", "math", math.Pow, registry.CanonicalReads, "Power", []string{"apoc.math.pow(2, 8) => 256.0"})
	register("apoc.math.sqrt", "math", math.Sqrt, registry.CanonicalReads, "Square root", []string{"apoc.math.sqrt(16) => 4.0"})
	register("apoc.math.log", "math", math.Log, registry.CanonicalReads, "Natural logarithm", []string{"apoc.math.log(2.718) => 1.0"})
	register("apoc.math.log10", "math", math.Log10, registry.CanonicalReads, "Base-10 logarithm", []string{"apoc.math.log10(100) => 2.0"})
	register("apoc.math.exp", "math", math.Exp, registry.CanonicalReads, "Exponential e^x", []string{"apoc.math.exp(1) => 2.718"})
	register("apoc.math.sin", "math", math.Sin, registry.CanonicalReads, "Sine", []string{"apoc.math.sin(0) => 0.0"})
	register("apoc.math.cos", "math", math.Cos, registry.CanonicalReads, "Cosine", []string{"apoc.math.cos(0) => 1.0"})
	register("apoc.math.tan", "math", math.Tan, registry.CanonicalReads, "Tangent", []string{"apoc.math.tan(0) => 0.0"})
	register("apoc.math.asin", "math", math.Asin, registry.CanonicalReads, "Arc sine", []string{"apoc.math.asin(0) => 0.0"})
	register("apoc.math.acos", "math", math.Acos, registry.CanonicalReads, "Arc cosine", []string{"apoc.math.acos(1) => 0.0"})
	register("apoc.math.atan", "math", math.Atan, registry.CanonicalReads, "Arc tangent", []string{"apoc.math.atan(0) => 0.0"})
	register("apoc.math.atan2", "math", math.Atan2, registry.CanonicalReads, "Arc tangent 2", []string{"apoc.math.atan2(1, 1) => 0.785"})
	register("apoc.math.sinh", "math", math.Sinh, registry.CanonicalReads, "Hyperbolic sine", []string{"apoc.math.sinh(0) => 0.0"})
	register("apoc.math.cosh", "math", math.Cosh, registry.CanonicalReads, "Hyperbolic cosine", []string{"apoc.math.cosh(0) => 1.0"})
	register("apoc.math.tanh", "math", math.Tanh, registry.CanonicalReads, "Hyperbolic tangent", []string{"apoc.math.tanh(0) => 0.0"})
	register("apoc.math.sigmoid", "math", math.Sigmoid, registry.CanonicalReads, "Sigmoid function", []string{"apoc.math.sigmoid(0) => 0.5"})
	register("apoc.math.logit", "math", math.Logit, registry.CanonicalReads, "Logit function", []string{"apoc.math.logit(0.5) => 0.0"})
	register("apoc.math.clamp", "math", math.Clamp, registry.CanonicalReads, "Clamp value to range", []string{"apoc.math.clamp(5, 0, 10) => 5"})
	register("apoc.math.lerp", "math", math.Lerp, registry.CanonicalReads, "Linear interpolation", []string{"apoc.math.lerp(0, 10, 0.5) => 5.0"})
	register("apoc.math.normalize", "math", math.Normalize, registry.CanonicalReads, "Normalize to 0-1 range", []string{"apoc.math.normalize(5, 0, 10) => 0.5"})
	register("apoc.math.gcd", "math", math.Gcd, registry.CanonicalReads, "Greatest common divisor", []string{"apoc.math.gcd(12, 8) => 4"})
	register("apoc.math.lcm", "math", math.Lcm, registry.CanonicalReads, "Least common multiple", []string{"apoc.math.lcm(4, 6) => 12"})
	register("apoc.math.factorial", "math", math.Factorial, registry.CanonicalReads, "Factorial", []string{"apoc.math.factorial(5) => 120"})
	register("apoc.math.fibonacci", "math", math.Fibonacci, registry.CanonicalReads, "Fibonacci number", []string{"apoc.math.fibonacci(10) => 55"})
	register("apoc.math.isPrime", "math", math.IsPrime, registry.CanonicalReads, "Check if prime", []string{"apoc.math.isPrime(7) => true"})
	register("apoc.math.nextPrime", "math", math.NextPrime, registry.CanonicalReads, "Next prime number", []string{"apoc.math.nextPrime(10) => 11"})
	register("apoc.math.random", "math", math.Random, registry.CanonicalReads, "Random float 0-1", []string{"apoc.math.random() => 0.42"})
	register("apoc.math.randomInt", "math", math.RandomInt, registry.CanonicalReads, "Random integer", []string{"apoc.math.randomInt(1, 100) => 42"})
	register("apoc.math.percentile", "math", math.Percentile, registry.CanonicalReads, "Percentile value", []string{"apoc.math.percentile([1,2,3,4,5], 0.5) => 3.0"})
	register("apoc.math.median", "math", math.Median, registry.CanonicalReads, "Median value", []string{"apoc.math.median([1,2,3,4,5]) => 3.0"})
	register("apoc.math.mean", "math", math.Mean, registry.CanonicalReads, "Arithmetic mean", []string{"apoc.math.mean([1,2,3,4,5]) => 3.0"})
	register("apoc.math.stdev", "math", math.StdDev, registry.CanonicalReads, "Standard deviation", []string{"apoc.math.stdev([1,2,3,4,5]) => 1.41"})
	register("apoc.math.variance", "math", math.Variance, registry.CanonicalReads, "Variance", []string{"apoc.math.variance([1,2,3,4,5]) => 2.0"})
	register("apoc.math.mode", "math", math.Mode, registry.CanonicalReads, "Mode value", []string{"apoc.math.mode([1,2,2,3]) => 2"})
	register("apoc.math.range", "math", math.Range, registry.CanonicalReads, "Generate range", []string{"apoc.math.range(1, 5) => [1,2,3,4,5]"})
	register("apoc.math.sum", "math", math.Sum, registry.CanonicalReads, "Sum values", []string{"apoc.math.sum([1,2,3,4,5]) => 15"})
	register("apoc.math.product", "math", math.Product, registry.CanonicalReads, "Product of values", []string{"apoc.math.product([1,2,3,4,5]) => 120"})

	// ========================================
	// Convert Functions (apoc.convert.*)
	// ========================================
	register("apoc.convert.toBoolean", "convert", convert.ToBoolean, registry.CanonicalReads, "Convert to boolean", []string{"apoc.convert.toBoolean('true') => true"})
	register("apoc.convert.toInteger", "convert", convert.ToInteger, registry.CanonicalReads, "Convert to integer", []string{"apoc.convert.toInteger('42') => 42"})
	register("apoc.convert.toFloat", "convert", convert.ToFloat, registry.CanonicalReads, "Convert to float", []string{"apoc.convert.toFloat('3.14') => 3.14"})
	register("apoc.convert.toString", "convert", convert.ToString, registry.CanonicalReads, "Convert to string", []string{"apoc.convert.toString(42) => '42'"})
	register("apoc.convert.toList", "convert", convert.ToList, registry.CanonicalReads, "Convert to list", []string{"apoc.convert.toList(value) => [value]"})
	register("apoc.convert.toMap", "convert", convert.ToMap, registry.CanonicalReads, "Convert to map", []string{"apoc.convert.toMap(node) => {props}"})
	register("apoc.convert.toJson", "convert", convert.ToJson, registry.CanonicalReads, "Convert to JSON", []string{"apoc.convert.toJson({name:'Alice'}) => '{\"name\":\"Alice\"}'"})
	register("apoc.convert.fromJsonList", "convert", convert.FromJsonList, registry.CanonicalReads, "Parse JSON list", []string{"apoc.convert.fromJsonList('[1,2,3]') => [1,2,3]"})
	register("apoc.convert.fromJsonMap", "convert", convert.FromJsonMap, registry.CanonicalReads, "Parse JSON map", []string{"apoc.convert.fromJsonMap('{\"a\":1}') => {a:1}"})
	register("apoc.convert.toSet", "convert", convert.ToSet, registry.CanonicalReads, "Convert to set", []string{"apoc.convert.toSet([1,2,2,3]) => [1,2,3]"})
	register("apoc.convert.toSortedJsonMap", "convert", convert.ToSortedJsonMap, registry.CanonicalReads, "To sorted JSON", []string{"apoc.convert.toSortedJsonMap(map) => json"})
	register("apoc.convert.toTree", "convert", convert.ToTree, registry.CanonicalReads, "Convert to tree", []string{"apoc.convert.toTree(paths) => tree"})
	register("apoc.convert.fromJsonNode", "convert", convert.FromJsonNode, registry.CanonicalReads, "JSON to node", []string{"apoc.convert.fromJsonNode(json) => node"})
	register("apoc.convert.toNode", "convert", convert.ToNode, registry.CanonicalReads, "Convert to node", []string{"apoc.convert.toNode(map, labels) => node"})
	register("apoc.convert.toRelationship", "convert", convert.ToRelationship, registry.CanonicalReads, "Convert to rel", []string{"apoc.convert.toRelationship(map, type) => rel"})
	register("apoc.convert.getJsonProperty", "convert", convert.GetJsonProperty, registry.CanonicalReads, "Get JSON property", []string{"apoc.convert.getJsonProperty(json, 'key') => value"})
	register("apoc.convert.getJsonPropertyMap", "convert", convert.GetJsonPropertyMap, registry.CanonicalReads, "Get JSON property map", []string{"apoc.convert.getJsonPropertyMap(json, 'key') => map"})
	register("apoc.convert.setJsonProperty", "convert", convert.SetJsonProperty, registry.CanonicalReads, "Set JSON property", []string{"apoc.convert.setJsonProperty(json, 'key', value) => json"})
	register("apoc.convert.toIntList", "convert", convert.ToIntList, registry.CanonicalReads, "Convert to int list", []string{"apoc.convert.toIntList(['1','2']) => [1,2]"})
	register("apoc.convert.toFloatList", "convert", convert.ToFloatList, registry.CanonicalReads, "Convert to float list", []string{"apoc.convert.toFloatList(['1.0','2.0']) => [1.0,2.0]"})
	register("apoc.convert.toStringList", "convert", convert.ToStringList, registry.CanonicalReads, "Convert to string list", []string{"apoc.convert.toStringList([1,2]) => ['1','2']"})
	register("apoc.convert.toBooleanList", "convert", convert.ToBooleanList, registry.CanonicalReads, "Convert to boolean list", []string{"apoc.convert.toBooleanList(['true']) => [true]"})
	register("apoc.convert.toNodeList", "convert", convert.ToNodeList, registry.CanonicalReads, "Convert to node list", []string{"apoc.convert.toNodeList(maps) => nodes"})
	register("apoc.convert.toRelationshipList", "convert", convert.ToRelationshipList, registry.CanonicalReads, "Convert to rel list", []string{"apoc.convert.toRelationshipList(maps) => rels"})

	// ========================================
	// Map Functions (apoc.map.*)
	// ========================================
	register("apoc.map.fromPairs", "map", maputil.FromPairs, registry.CanonicalReads, "Create map from pairs", []string{"apoc.map.fromPairs([['a',1],['b',2]]) => {a:1, b:2}"})
	register("apoc.map.fromLists", "map", maputil.FromLists, registry.CanonicalReads, "Create map from lists", []string{"apoc.map.fromLists(['a','b'], [1,2]) => {a:1, b:2}"})
	register("apoc.map.fromValues", "map", maputil.FromValues, registry.CanonicalReads, "Create map from values", []string{"apoc.map.fromValues(['a',1,'b',2]) => {a:1, b:2}"})
	register("apoc.map.merge", "map", maputil.Merge, registry.CanonicalReads, "Merge maps", []string{"apoc.map.merge({a:1}, {b:2}) => {a:1, b:2}"})
	register("apoc.map.mergeList", "map", maputil.MergeList, registry.CanonicalReads, "Merge list of maps", []string{"apoc.map.mergeList([{a:1},{b:2}]) => {a:1, b:2}"})
	register("apoc.map.setKey", "map", maputil.SetKey, registry.CanonicalReads, "Set key", []string{"apoc.map.setKey({a:1}, 'b', 2) => {a:1, b:2}"})
	register("apoc.map.setEntry", "map", maputil.SetEntry, registry.CanonicalReads, "Set entry", []string{"apoc.map.setEntry({a:1}, 'b', 2) => {a:1, b:2}"})
	register("apoc.map.setPairs", "map", maputil.SetPairs, registry.CanonicalReads, "Set pairs", []string{"apoc.map.setPairs({}, [['a',1]]) => {a:1}"})
	register("apoc.map.setLists", "map", maputil.SetLists, registry.CanonicalReads, "Set lists", []string{"apoc.map.setLists({}, ['a'], [1]) => {a:1}"})
	register("apoc.map.setValues", "map", maputil.SetValues, registry.CanonicalReads, "Set values", []string{"apoc.map.setValues({}, ['a',1]) => {a:1}"})
	register("apoc.map.removeKey", "map", maputil.RemoveKey, registry.CanonicalReads, "Remove key", []string{"apoc.map.removeKey({a:1,b:2}, 'b') => {a:1}"})
	register("apoc.map.removeKeys", "map", maputil.RemoveKeys, registry.CanonicalReads, "Remove keys", []string{"apoc.map.removeKeys({a:1,b:2}, ['b']) => {a:1}"})
	register("apoc.map.clean", "map", maputil.Clean, registry.CanonicalReads, "Clean nulls", []string{"apoc.map.clean({a:1,b:null}) => {a:1}"})
	register("apoc.map.get", "map", maputil.Get, registry.CanonicalReads, "Get value", []string{"apoc.map.get({a:1}, 'a') => 1"})
	register("apoc.map.mget", "map", maputil.MGet, registry.CanonicalReads, "Get multiple values", []string{"apoc.map.mget({a:1,b:2}, ['a','b']) => [1,2]"})
	register("apoc.map.subMap", "map", maputil.SubMap, registry.CanonicalReads, "Extract sub map", []string{"apoc.map.subMap({a:1,b:2}, ['a']) => {a:1}"})
	register("apoc.map.keys", "map", maputil.Keys, registry.CanonicalReads, "Get map keys", []string{"apoc.map.keys({a:1, b:2}) => ['a', 'b']"})
	register("apoc.map.values", "map", maputil.Values, registry.CanonicalReads, "Get map values", []string{"apoc.map.values({a:1, b:2}) => [1, 2]"})
	register("apoc.map.sortedProperties", "map", maputil.SortedProperties, registry.CanonicalReads, "Sorted properties", []string{"apoc.map.sortedProperties({b:2,a:1}) => [['a',1],['b',2]]"})
	register("apoc.map.flatten", "map", maputil.Flatten, registry.CanonicalReads, "Flatten nested map", []string{"apoc.map.flatten({a:{b:1}}) => {'a.b':1}"})
	register("apoc.map.unflatten", "map", maputil.Unflatten, registry.CanonicalReads, "Unflatten map", []string{"apoc.map.unflatten({'a.b':1}) => {a:{b:1}}"})
	register("apoc.map.groupBy", "map", maputil.GroupBy, registry.CanonicalReads, "Group by key", []string{"apoc.map.groupBy([{a:1},{a:2}], 'a') => {1:[...],2:[...]}"})
	register("apoc.map.groupByMulti", "map", maputil.GroupByMulti, registry.CanonicalReads, "Group by multiple keys", []string{"apoc.map.groupByMulti(maps, ['k1','k2']) => grouped"})
	register("apoc.map.updateTree", "map", maputil.UpdateTree, registry.CanonicalReads, "Update tree", []string{"apoc.map.updateTree(map, path, value) => map"})
	register("apoc.map.dropNullValues", "map", maputil.DropNullValues, registry.CanonicalReads, "Drop null values", []string{"apoc.map.dropNullValues({a:1,b:null}) => {a:1}"})

	// ========================================
	// Date Functions (apoc.date.*)
	// ========================================
	register("apoc.date.parse", "date", date.Parse, registry.CanonicalReads, "Parse date string", []string{"apoc.date.parse('2024-01-15', 'yyyy-MM-dd') => 1705276800"})
	register("apoc.date.format", "date", date.Format, registry.CanonicalReads, "Format timestamp", []string{"apoc.date.format(1705276800, 'yyyy-MM-dd') => '2024-01-15'"})
	register("apoc.date.currentTimestamp", "date", date.CurrentTimestamp, registry.CanonicalReads, "Current timestamp", []string{"apoc.date.currentTimestamp() => 1705276800"})
	register("apoc.date.field", "date", date.Field, registry.CanonicalReads, "Get date field", []string{"apoc.date.field(timestamp, 'year') => 2024"})
	register("apoc.date.fields", "date", date.Fields, registry.CanonicalReads, "Get all date fields", []string{"apoc.date.fields(timestamp) => {year:2024, month:1, ...}"})
	register("apoc.date.add", "date", date.Add, registry.CanonicalReads, "Add to date", []string{"apoc.date.add(timestamp, 1, 'day') => timestamp"})
	register("apoc.date.convert", "date", date.Convert, registry.CanonicalReads, "Convert date", []string{"apoc.date.convert(value, fromUnit, toUnit) => converted"})
	register("apoc.date.convertFormat", "date", date.ConvertFormat, registry.CanonicalReads, "Convert format", []string{"apoc.date.convertFormat(date, from, to) => formatted"})
	register("apoc.date.fromISO8601", "date", date.FromISO8601, registry.CanonicalReads, "From ISO8601", []string{"apoc.date.fromISO8601('2024-01-15T10:00:00Z') => timestamp"})
	register("apoc.date.toISO8601", "date", date.ToISO8601, registry.CanonicalReads, "To ISO8601", []string{"apoc.date.toISO8601(timestamp) => '2024-01-15T10:00:00Z'"})
	register("apoc.date.toYears", "date", date.ToYears, registry.CanonicalReads, "To years", []string{"apoc.date.toYears(timestamp) => years"})
	register("apoc.date.systemTimezone", "date", date.SystemTimezone, registry.CanonicalReads, "System timezone", []string{"apoc.date.systemTimezone() => 'UTC'"})
	register("apoc.date.parseAsZonedDateTime", "date", date.ParseAsZonedDateTime, registry.CanonicalReads, "Parse zoned datetime", []string{"apoc.date.parseAsZonedDateTime(str, format) => zdt"})
	register("apoc.date.toUnixTime", "date", date.ToUnixTime, registry.CanonicalReads, "To unix time", []string{"apoc.date.toUnixTime(datetime) => 1705276800"})
	register("apoc.date.fromUnixTime", "date", date.FromUnixTime, registry.CanonicalReads, "From unix time", []string{"apoc.date.fromUnixTime(1705276800) => datetime"})

	// ========================================
	// JSON Functions (apoc.json.*)
	// ========================================
	register("apoc.json.path", "json", json.Path, registry.CanonicalReads, "JSON path query", []string{"apoc.json.path(json, '$.name') => 'Alice'"})
	register("apoc.json.validate", "json", json.Validate, registry.CanonicalReads, "Validate JSON", []string{"apoc.json.validate('{\"name\":\"Alice\"}') => true"})
	register("apoc.json.parse", "json", json.Parse, registry.CanonicalReads, "Parse JSON", []string{"apoc.json.parse('{\"name\":\"Alice\"}') => {name:'Alice'}"})
	register("apoc.json.stringify", "json", json.Stringify, registry.CanonicalReads, "Convert to JSON string", []string{"apoc.json.stringify({name:'Alice'}) => '{\"name\":\"Alice\"}'"})
	register("apoc.json.pretty", "json", json.Pretty, registry.CanonicalReads, "Pretty print JSON", []string{"apoc.json.pretty(json) => formattedJson"})
	register("apoc.json.compact", "json", json.Compact, registry.CanonicalReads, "Compact JSON", []string{"apoc.json.compact(json) => compactJson"})
	register("apoc.json.keys", "json", json.Keys, registry.CanonicalReads, "Get JSON keys", []string{"apoc.json.keys('{\"a\":1}') => ['a']"})
	register("apoc.json.values", "json", json.Values, registry.CanonicalReads, "Get JSON values", []string{"apoc.json.values('{\"a\":1}') => [1]"})
	register("apoc.json.type", "json", json.Type, registry.CanonicalReads, "Get JSON type", []string{"apoc.json.type(value) => 'object'"})
	register("apoc.json.size", "json", json.Size, registry.CanonicalReads, "Get JSON size", []string{"apoc.json.size('[1,2,3]') => 3"})
	register("apoc.json.merge", "json", json.Merge, registry.CanonicalReads, "Merge JSON", []string{"apoc.json.merge(json1, json2) => merged"})
	register("apoc.json.set", "json", json.Set, registry.CanonicalReads, "Set JSON value", []string{"apoc.json.set(json, 'key', value) => json"})
	register("apoc.json.delete", "json", json.Delete, registry.CanonicalReads, "Delete JSON key", []string{"apoc.json.delete(json, 'key') => json"})
	register("apoc.json.flatten", "json", json.Flatten, registry.CanonicalReads, "Flatten JSON", []string{"apoc.json.flatten(json) => flat"})
	register("apoc.json.unflatten", "json", json.Unflatten, registry.CanonicalReads, "Unflatten JSON", []string{"apoc.json.unflatten(flat) => json"})
	register("apoc.json.filter", "json", json.Filter, registry.OpaqueReads, "Filter JSON", []string{"apoc.json.filter(json, predicate) => filtered"})
	register("apoc.json.map", "json", json.Map, registry.OpaqueReads, "Map JSON", []string{"apoc.json.map(json, transform) => mapped"})
	register("apoc.json.reduce", "json", json.Reduce, registry.OpaqueReads, "Reduce JSON", []string{"apoc.json.reduce(json, reducer, init) => result"})

	// ========================================
	// Util Functions (apoc.util.*)
	// ========================================
	register("apoc.util.sleep", "util", util.Sleep, registry.CanonicalReads, "Sleep", []string{"apoc.util.sleep(1000) => (waits 1s)"})
	register("apoc.util.md5", "util", util.MD5, registry.CanonicalReads, "MD5 hash", []string{"apoc.util.md5(['hello']) => '5d41402abc4b2a76b9719d911017c592'"})
	register("apoc.util.sha1", "util", util.SHA1, registry.CanonicalReads, "SHA1 hash", []string{"apoc.util.sha1(['hello']) => 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'"})
	register("apoc.util.sha256", "util", util.SHA256, registry.CanonicalReads, "SHA256 hash", []string{"apoc.util.sha256(['hello']) => '2cf24dba5fb0a30e...'"})
	register("apoc.util.compress", "util", util.Compress, registry.CanonicalReads, "Compress data", []string{"apoc.util.compress('data') => compressed"})
	register("apoc.util.decompress", "util", util.Decompress, registry.CanonicalReads, "Decompress data", []string{"apoc.util.decompress(compressed) => 'data'"})
	register("apoc.util.validate", "util", util.Validate, registry.CanonicalReads, "Validate value", []string{"apoc.util.validate(value, predicate) => bool"})
	register("apoc.util.validatePattern", "util", util.ValidatePattern, registry.CanonicalReads, "Validate pattern", []string{"apoc.util.validatePattern('test', '.*') => true"})
	register("apoc.util.uuid", "util", util.UUID, registry.CanonicalReads, "Generate UUID", []string{"apoc.util.uuid() => '550e8400-e29b-41d4-a716-446655440000'"})
	register("apoc.util.randomUUID", "util", util.RandomUUID, registry.CanonicalReads, "Random UUID", []string{"apoc.util.randomUUID() => '550e8400-...'"})
	register("apoc.util.merge", "util", util.Merge, registry.CanonicalReads, "Merge values", []string{"apoc.util.merge(v1, v2) => merged"})
	register("apoc.util.coalesce", "util", util.Coalesce, registry.CanonicalReads, "First non-null", []string{"apoc.util.coalesce(null, 1) => 1"})
	register("apoc.util.case", "util", util.Case, registry.CanonicalReads, "Case expression", []string{"apoc.util.case([[cond, val], ...]) => result"})
	register("apoc.util.when", "util", util.When, registry.CanonicalReads, "When expression", []string{"apoc.util.when(cond, thenVal, elseVal) => result"})
	register("apoc.util.typeOf", "util", util.TypeOf, registry.CanonicalReads, "Get type", []string{"apoc.util.typeOf(value) => 'STRING'"})
	register("apoc.util.isNode", "util", util.IsNode, registry.CanonicalReads, "Is node", []string{"apoc.util.isNode(value) => bool"})
	register("apoc.util.isRelationship", "util", util.IsRelationship, registry.CanonicalReads, "Is relationship", []string{"apoc.util.isRelationship(value) => bool"})
	register("apoc.util.isPath", "util", util.IsPath, registry.CanonicalReads, "Is path", []string{"apoc.util.isPath(value) => bool"})
	register("apoc.util.sha256Base64", "util", util.Sha256Base64, registry.CanonicalReads, "SHA256 base64", []string{"apoc.util.sha256Base64('hello') => base64"})
	register("apoc.util.sha1Base64", "util", util.Sha1Base64, registry.CanonicalReads, "SHA1 base64", []string{"apoc.util.sha1Base64('hello') => base64"})
	register("apoc.util.md5Base64", "util", util.Md5Base64, registry.CanonicalReads, "MD5 base64", []string{"apoc.util.md5Base64('hello') => base64"})
	register("apoc.util.sha256Hex", "util", util.Sha256Hex, registry.CanonicalReads, "SHA256 hex", []string{"apoc.util.sha256Hex('hello') => hex"})
	register("apoc.util.sha1Hex", "util", util.Sha1Hex, registry.CanonicalReads, "SHA1 hex", []string{"apoc.util.sha1Hex('hello') => hex"})
	register("apoc.util.md5Hex", "util", util.Md5Hex, registry.CanonicalReads, "MD5 hex", []string{"apoc.util.md5Hex('hello') => hex"})
	register("apoc.util.repeat", "util", util.Repeat, registry.CanonicalReads, "Repeat value", []string{"apoc.util.repeat('a', 3) => 'aaa'"})
	register("apoc.util.range", "util", util.Range, registry.CanonicalReads, "Generate range", []string{"apoc.util.range(1, 5) => [1,2,3,4,5]"})
	register("apoc.util.partition", "util", util.Partition, registry.OpaqueReads, "Partition list", []string{"apoc.util.partition([1,2,3,4], 2) => [[1,2],[3,4]]"})
	register("apoc.util.compressWithAlgorithm", "util", util.CompressWithAlgorithm, registry.CanonicalReads, "Compress with algo", []string{"apoc.util.compressWithAlgorithm(data, 'gzip') => compressed"})
	register("apoc.util.decompressWithAlgorithm", "util", util.DecompressWithAlgorithm, registry.CanonicalReads, "Decompress with algo", []string{"apoc.util.decompressWithAlgorithm(data, 'gzip') => decompressed"})
	register("apoc.util.encodeBase64", "util", util.EncodeBase64, registry.CanonicalReads, "Encode base64", []string{"apoc.util.encodeBase64('hello') => 'aGVsbG8='"})
	register("apoc.util.decodeBase64", "util", util.DecodeBase64, registry.CanonicalReads, "Decode base64", []string{"apoc.util.decodeBase64('aGVsbG8=') => 'hello'"})
	register("apoc.util.encodeURL", "util", util.EncodeURL, registry.CanonicalReads, "URL encode", []string{"apoc.util.encodeURL('hello world') => 'hello%20world'"})
	register("apoc.util.decodeURL", "util", util.DecodeURL, registry.CanonicalReads, "URL decode", []string{"apoc.util.decodeURL('hello%20world') => 'hello world'"})
	register("apoc.util.now", "util", util.Now, registry.CanonicalReads, "Current time", []string{"apoc.util.now() => timestamp"})
	register("apoc.util.nowInSeconds", "util", util.NowInSeconds, registry.CanonicalReads, "Current time in seconds", []string{"apoc.util.nowInSeconds() => seconds"})
	register("apoc.util.timestamp", "util", util.Timestamp, registry.CanonicalReads, "Timestamp", []string{"apoc.util.timestamp() => timestamp"})
	register("apoc.util.parseTimestamp", "util", util.ParseTimestamp, registry.CanonicalReads, "Parse timestamp", []string{"apoc.util.parseTimestamp(str) => timestamp"})
	register("apoc.util.formatTimestamp", "util", util.FormatTimestamp, registry.CanonicalReads, "Format timestamp", []string{"apoc.util.formatTimestamp(ts, fmt) => string"})

	// ========================================
	// Aggregation Functions (apoc.agg.*)
	// ========================================
	register("apoc.agg.first", "agg", agg.First, registry.CanonicalReads, "First value", []string{"apoc.agg.first([1,2,3]) => 1"})
	register("apoc.agg.last", "agg", agg.Last, registry.CanonicalReads, "Last value", []string{"apoc.agg.last([1,2,3]) => 3"})
	register("apoc.agg.nth", "agg", agg.Nth, registry.CanonicalReads, "Nth value", []string{"apoc.agg.nth([1,2,3], 1) => 2"})
	register("apoc.agg.slice", "agg", agg.Slice, registry.CanonicalReads, "Slice of values", []string{"apoc.agg.slice([1,2,3,4,5], 1, 3) => [2,3,4]"})
	register("apoc.agg.product", "agg", agg.Product, registry.CanonicalReads, "Product of values", []string{"apoc.agg.product([1,2,3,4]) => 24"})
	register("apoc.agg.median", "agg", agg.Median, registry.CanonicalReads, "Median value", []string{"apoc.agg.median([1,2,3,4,5]) => 3.0"})
	register("apoc.agg.percentile", "agg", agg.Percentile, registry.CanonicalReads, "Percentile value", []string{"apoc.agg.percentile([1,2,3,4,5], 0.95) => 4.8"})
	register("apoc.agg.stdev", "agg", agg.StdDev, registry.CanonicalReads, "Standard deviation", []string{"apoc.agg.stdev([2,4,4,4,5,5,7,9]) => 2.0"})
	register("apoc.agg.mode", "agg", agg.Mode, registry.CanonicalReads, "Mode value", []string{"apoc.agg.mode([1,2,2,3]) => 2"})
	register("apoc.agg.statistics", "agg", agg.Statistics, registry.CanonicalReads, "Full statistics", []string{"apoc.agg.statistics([1,2,3,4,5]) => {min:1, max:5, mean:3, ...}"})
	register("apoc.agg.graph", "agg", agg.Graph, registry.CanonicalReads, "Aggregate graph", []string{"apoc.agg.graph(nodes, rels) => graph"})
	register("apoc.agg.minItems", "agg", agg.MinItems, registry.CanonicalReads, "Items with minimum", []string{"apoc.agg.minItems([{v:1},{v:2}], 'v') => [{v:1}]"})
	register("apoc.agg.maxItems", "agg", agg.MaxItems, registry.CanonicalReads, "Items with maximum", []string{"apoc.agg.maxItems([{v:1},{v:2}], 'v') => [{v:2}]"})
	register("apoc.agg.histogram", "agg", agg.Histogram, registry.CanonicalReads, "Generate histogram", []string{"apoc.agg.histogram([1,2,2,3]) => {1:1,2:2,3:1}"})
	register("apoc.agg.frequencies", "agg", agg.Frequencies, registry.CanonicalReads, "Value frequencies", []string{"apoc.agg.frequencies([1,2,2,3]) => [{item:1,count:1},{item:2,count:2}]"})

	// ========================================
	// Algorithm Functions (apoc.algo.*)
	// ========================================
	register("apoc.algo.betweennessCentrality", "algo", algo.BetweennessCentrality, registry.CanonicalReads, "Betweenness centrality", []string{"apoc.algo.betweennessCentrality(nodes) => scores"})
	register("apoc.algo.closenessCentrality", "algo", algo.ClosenessCentrality, registry.CanonicalReads, "Closeness centrality", []string{"apoc.algo.closenessCentrality(nodes) => scores"})
	register("apoc.algo.degreeCentrality", "algo", algo.DegreeCentrality, registry.CanonicalReads, "Degree centrality", []string{"apoc.algo.degreeCentrality(nodes) => scores"})
	register("apoc.algo.community", "algo", algo.Community, registry.CanonicalReads, "Community detection", []string{"apoc.algo.community(nodes) => communities"})
	register("apoc.algo.aStar", "algo", algo.AStar, registry.CanonicalReads, "A* pathfinding", []string{"apoc.algo.aStar(start, end, config) => path"})
	register("apoc.algo.dijkstra", "algo", algo.Dijkstra, registry.CanonicalReads, "Dijkstra shortest path", []string{"apoc.algo.dijkstra(start, end) => path"})
	register("apoc.algo.allPairs", "algo", algo.AllPairs, registry.CanonicalReads, "All pairs shortest paths", []string{"apoc.algo.allPairs(nodes) => paths"})
	register("apoc.algo.cover", "algo", algo.Cover, registry.CanonicalReads, "Node cover", []string{"apoc.algo.cover(nodes) => cover"})

	// ========================================
	// Community Detection Functions (apoc.community.*)
	// ========================================
	register("apoc.community.louvain", "community", community.Louvain, registry.CanonicalReads, "Louvain community detection", []string{"apoc.community.louvain(nodes, rels, config) => communities"})
	register("apoc.community.labelPropagation", "community", community.LabelPropagation, registry.CanonicalReads, "Label propagation", []string{"apoc.community.labelPropagation(nodes, rels) => communities"})
	register("apoc.community.modularity", "community", community.Modularity, registry.CanonicalReads, "Calculate modularity", []string{"apoc.community.modularity(nodes, rels, communities) => score"})
	register("apoc.community.triangleCount", "community", community.TriangleCount, registry.CanonicalReads, "Count triangles per node", []string{"apoc.community.triangleCount(nodes, rels) => counts"})
	register("apoc.community.totalTriangles", "community", community.TotalTriangles, registry.CanonicalReads, "Total triangle count", []string{"apoc.community.totalTriangles(nodes, rels) => count"})
	register("apoc.community.clusteringCoefficient", "community", community.ClusteringCoefficient, registry.CanonicalReads, "Clustering coefficient per node", []string{"apoc.community.clusteringCoefficient(nodes, rels) => coefficients"})
	register("apoc.community.averageClusteringCoefficient", "community", community.AverageClusteringCoefficient, registry.CanonicalReads, "Average clustering coefficient", []string{"apoc.community.averageClusteringCoefficient(nodes, rels) => coefficient"})
	register("apoc.community.connectedComponents", "community", community.ConnectedComponents, registry.CanonicalReads, "Connected components", []string{"apoc.community.connectedComponents(nodes, rels) => components"})
	register("apoc.community.numComponents", "community", community.NumComponents, registry.CanonicalReads, "Number of components", []string{"apoc.community.numComponents(nodes, rels) => count"})
	register("apoc.community.stronglyConnectedComponents", "community", community.StronglyConnectedComponents, registry.CanonicalReads, "Strongly connected components", []string{"apoc.community.stronglyConnectedComponents(nodes, rels) => components"})
	register("apoc.community.weaklyConnectedComponents", "community", community.WeaklyConnectedComponents, registry.CanonicalReads, "Weakly connected components", []string{"apoc.community.weaklyConnectedComponents(nodes, rels) => components"})
	register("apoc.community.kCore", "community", community.KCore, registry.CanonicalReads, "K-core decomposition", []string{"apoc.community.kCore(nodes, rels, k) => core"})
	register("apoc.community.coreNumber", "community", community.CoreNumber, registry.CanonicalReads, "Core number for each node", []string{"apoc.community.coreNumber(nodes, rels) => coreNumbers"})
	register("apoc.community.conductance", "community", community.Conductance, registry.CanonicalReads, "Conductance of community", []string{"apoc.community.conductance(nodes, rels, community) => score"})
	register("apoc.community.density", "community", community.Density, registry.CanonicalReads, "Graph density", []string{"apoc.community.density(nodes, rels) => density"})
	register("apoc.community.fastGreedy", "community", community.FastGreedy, registry.CanonicalReads, "Fast greedy modularity optimization", []string{"apoc.community.fastGreedy(nodes, rels) => communities"})
	register("apoc.community.spinGlass", "community", community.SpinGlass, registry.CanonicalReads, "Spin-glass community detection", []string{"apoc.community.spinGlass(nodes, rels, config) => communities"})
	register("apoc.community.walkTrap", "community", community.WalkTrap, registry.CanonicalReads, "WalkTrap community detection", []string{"apoc.community.walkTrap(nodes, rels, steps) => communities"})
	register("apoc.community.infoMap", "community", community.InfoMap, registry.CanonicalReads, "InfoMap community detection", []string{"apoc.community.infoMap(nodes, rels, trials) => communities"})

	// ========================================
	// Atomic Functions (apoc.atomic.*)
	// ========================================
	register("apoc.atomic.add", "atomic", atomic.Add, registry.CanonicalReads, "Atomic add", []string{"apoc.atomic.add(node, 'prop', 1) => newValue"})
	register("apoc.atomic.subtract", "atomic", atomic.Subtract, registry.CanonicalReads, "Atomic subtract", []string{"apoc.atomic.subtract(node, 'prop', 1) => newValue"})
	register("apoc.atomic.concat", "atomic", atomic.Concat, registry.CanonicalReads, "Atomic concat", []string{"apoc.atomic.concat(node, 'prop', 'suffix') => newValue"})
	register("apoc.atomic.insert", "atomic", atomic.Insert, registry.CanonicalReads, "Atomic insert", []string{"apoc.atomic.insert(node, 'prop', 0, value) => newList"})
	register("apoc.atomic.remove", "atomic", atomic.Remove, registry.CanonicalReads, "Atomic remove", []string{"apoc.atomic.remove(node, 'prop', 0) => newList"})
	register("apoc.atomic.update", "atomic", atomic.Update, registry.CanonicalReads, "Atomic update", []string{"apoc.atomic.update(node, 'prop', fn) => newValue"})
	register("apoc.atomic.increment", "atomic", atomic.Increment, registry.CanonicalReads, "Atomic increment", []string{"apoc.atomic.increment(node, 'prop') => newValue"})
	register("apoc.atomic.decrement", "atomic", atomic.Decrement, registry.CanonicalReads, "Atomic decrement", []string{"apoc.atomic.decrement(node, 'prop') => newValue"})
	register("apoc.atomic.compareAndSwap", "atomic", atomic.CompareAndSwap, registry.CanonicalReads, "Atomic CAS", []string{"apoc.atomic.compareAndSwap(node, 'prop', old, new) => success"})

	// ========================================
	// Bitwise Functions (apoc.bitwise.*)
	// ========================================
	register("apoc.bitwise.op", "bitwise", bitwise.Op, registry.CanonicalReads, "Bitwise operation", []string{"apoc.bitwise.op(a, 'AND', b) => result"})
	register("apoc.bitwise.and", "bitwise", bitwise.And, registry.CanonicalReads, "Bitwise AND", []string{"apoc.bitwise.and(5, 3) => 1"})
	register("apoc.bitwise.or", "bitwise", bitwise.Or, registry.CanonicalReads, "Bitwise OR", []string{"apoc.bitwise.or(5, 3) => 7"})
	register("apoc.bitwise.xor", "bitwise", bitwise.Xor, registry.CanonicalReads, "Bitwise XOR", []string{"apoc.bitwise.xor(5, 3) => 6"})
	register("apoc.bitwise.not", "bitwise", bitwise.Not, registry.CanonicalReads, "Bitwise NOT", []string{"apoc.bitwise.not(5) => -6"})
	register("apoc.bitwise.leftShift", "bitwise", bitwise.LeftShift, registry.CanonicalReads, "Left shift", []string{"apoc.bitwise.leftShift(5, 2) => 20"})
	register("apoc.bitwise.rightShift", "bitwise", bitwise.RightShift, registry.CanonicalReads, "Right shift", []string{"apoc.bitwise.rightShift(20, 2) => 5"})
	register("apoc.bitwise.setBit", "bitwise", bitwise.SetBit, registry.CanonicalReads, "Set bit", []string{"apoc.bitwise.setBit(0, 2) => 4"})
	register("apoc.bitwise.clearBit", "bitwise", bitwise.ClearBit, registry.CanonicalReads, "Clear bit", []string{"apoc.bitwise.clearBit(7, 1) => 5"})
	register("apoc.bitwise.toggleBit", "bitwise", bitwise.ToggleBit, registry.CanonicalReads, "Toggle bit", []string{"apoc.bitwise.toggleBit(5, 1) => 7"})
	register("apoc.bitwise.testBit", "bitwise", bitwise.TestBit, registry.CanonicalReads, "Test bit", []string{"apoc.bitwise.testBit(5, 2) => true"})
	register("apoc.bitwise.countBits", "bitwise", bitwise.CountBits, registry.CanonicalReads, "Count set bits", []string{"apoc.bitwise.countBits(7) => 3"})
	register("apoc.bitwise.reverseBits", "bitwise", bitwise.ReverseBits, registry.CanonicalReads, "Reverse bits", []string{"apoc.bitwise.reverseBits(5, 8) => 160"})
	register("apoc.bitwise.rotateLeft", "bitwise", bitwise.RotateLeft, registry.CanonicalReads, "Rotate left", []string{"apoc.bitwise.rotateLeft(5, 2, 8) => 20"})
	register("apoc.bitwise.rotateRight", "bitwise", bitwise.RotateRight, registry.CanonicalReads, "Rotate right", []string{"apoc.bitwise.rotateRight(20, 2, 8) => 5"})

	// ========================================
	// Cypher Functions (apoc.cypher.*)
	// ========================================
	register("apoc.cypher.run", "cypher", apoccypher.Run, registry.CanonicalReads, "Run cypher query", []string{"apoc.cypher.run('MATCH (n) RETURN n') => results"})
	register("apoc.cypher.runMany", "cypher", apoccypher.RunMany, registry.CanonicalReads, "Run multiple queries", []string{"apoc.cypher.runMany(['q1','q2']) => results"})
	register("apoc.cypher.runFile", "cypher", apoccypher.RunFile, registry.CanonicalReads, "Run cypher file", []string{"apoc.cypher.runFile('/path/to/file.cypher') => results"})
	register("apoc.cypher.doIt", "cypher", apoccypher.DoIt, registry.CanonicalReads, "Execute cypher", []string{"apoc.cypher.doIt('CREATE (n)') => results"})
	register("apoc.cypher.parallel", "cypher", apoccypher.Parallel, registry.CanonicalReads, "Parallel execution", []string{"apoc.cypher.parallel('MATCH...', items) => results"})
	register("apoc.cypher.mapParallel", "cypher", apoccypher.MapParallel, registry.CanonicalReads, "Parallel map", []string{"apoc.cypher.mapParallel('MATCH...', items) => results"})
	register("apoc.cypher.runFirstColumn", "cypher", apoccypher.RunFirstColumn, registry.CanonicalReads, "First column", []string{"apoc.cypher.runFirstColumn('RETURN 1') => [1]"})
	register("apoc.cypher.runFirstColumnMany", "cypher", apoccypher.RunFirstColumnMany, registry.CanonicalReads, "First column many", []string{"apoc.cypher.runFirstColumnMany(['q1','q2']) => results"})
	register("apoc.cypher.runFirstColumnSingle", "cypher", apoccypher.RunFirstColumnSingle, registry.CanonicalReads, "First column single", []string{"apoc.cypher.runFirstColumnSingle('RETURN 1') => 1"})
	register("apoc.cypher.parse", "cypher", apoccypher.Parse, registry.CanonicalReads, "Parse cypher", []string{"apoc.cypher.parse('MATCH (n) RETURN n') => ast"})
	register("apoc.cypher.validate", "cypher", apoccypher.Validate, registry.CanonicalReads, "Validate cypher", []string{"apoc.cypher.validate('MATCH (n) RETURN n') => true"})
	register("apoc.cypher.explain", "cypher", apoccypher.Explain, registry.CanonicalReads, "Explain query", []string{"apoc.cypher.explain('MATCH (n) RETURN n') => plan"})
	register("apoc.cypher.profile", "cypher", apoccypher.Profile, registry.CanonicalReads, "Profile query", []string{"apoc.cypher.profile('MATCH (n) RETURN n') => profile"})
	register("apoc.cypher.toMap", "cypher", apoccypher.ToMap, registry.CanonicalReads, "Result to map", []string{"apoc.cypher.toMap(result) => map"})
	register("apoc.cypher.toList", "cypher", apoccypher.ToList, registry.CanonicalReads, "Result to list", []string{"apoc.cypher.toList(result) => list"})
	register("apoc.cypher.toJson", "cypher", apoccypher.ToJson, registry.CanonicalReads, "Result to JSON", []string{"apoc.cypher.toJson(result) => json"})

	// ========================================
	// Diff Functions (apoc.diff.*)
	// ========================================
	register("apoc.diff.nodes", "diff", diff.Nodes, registry.CanonicalReads, "Diff nodes", []string{"apoc.diff.nodes(node1, node2) => diff"})
	register("apoc.diff.relationships", "diff", diff.Relationships, registry.CanonicalReads, "Diff relationships", []string{"apoc.diff.relationships(rel1, rel2) => diff"})
	register("apoc.diff.maps", "diff", diff.Maps, registry.CanonicalReads, "Diff maps", []string{"apoc.diff.maps(map1, map2) => diff"})
	register("apoc.diff.lists", "diff", diff.Lists, registry.CanonicalReads, "Diff lists", []string{"apoc.diff.lists(list1, list2) => diff"})
	register("apoc.diff.strings", "diff", diff.Strings, registry.CanonicalReads, "Diff strings", []string{"apoc.diff.strings(str1, str2) => diff"})
	register("apoc.diff.deep", "diff", diff.Deep, registry.CanonicalReads, "Deep diff", []string{"apoc.diff.deep(obj1, obj2) => diff"})
	register("apoc.diff.patch", "diff", diff.Patch, registry.CanonicalReads, "Apply patch", []string{"apoc.diff.patch(obj, diff) => patched"})
	register("apoc.diff.merge", "diff", diff.Merge, registry.CanonicalReads, "Merge diffs", []string{"apoc.diff.merge(diff1, diff2) => merged"})
	register("apoc.diff.summary", "diff", diff.Summary, registry.CanonicalReads, "Diff summary", []string{"apoc.diff.summary(diff) => summary"})

	// ========================================
	// Graph Functions (apoc.graph.*)
	// ========================================
	register("apoc.graph.from", "graph", graph.From, registry.CanonicalReads, "Create graph from", []string{"apoc.graph.from(nodes, rels) => graph"})
	register("apoc.graph.fromData", "graph", graph.FromData, registry.CanonicalReads, "Graph from data", []string{"apoc.graph.fromData(data) => graph"})
	register("apoc.graph.fromPath", "graph", graph.FromPath, registry.CanonicalReads, "Graph from path", []string{"apoc.graph.fromPath(path) => graph"})
	register("apoc.graph.fromPaths", "graph", graph.FromPaths, registry.CanonicalReads, "Graph from paths", []string{"apoc.graph.fromPaths(paths) => graph"})
	register("apoc.graph.fromDocument", "graph", graph.FromDocument, registry.CanonicalReads, "Graph from document", []string{"apoc.graph.fromDocument(doc) => graph"})
	register("apoc.graph.fromCypher", "graph", graph.FromCypher, registry.CanonicalReads, "Graph from cypher", []string{"apoc.graph.fromCypher('MATCH...') => graph"})
	register("apoc.graph.validate", "graph", graph.Validate, registry.CanonicalReads, "Validate graph", []string{"apoc.graph.validate(graph) => valid"})
	register("apoc.graph.nodes", "graph", graph.Nodes, registry.CanonicalReads, "Get nodes", []string{"apoc.graph.nodes(graph) => nodes"})
	register("apoc.graph.relationships", "graph", graph.Relationships, registry.CanonicalReads, "Get relationships", []string{"apoc.graph.relationships(graph) => rels"})
	register("apoc.graph.merge", "graph", graph.Merge, registry.CanonicalReads, "Merge graphs", []string{"apoc.graph.merge(graph1, graph2) => merged"})
	register("apoc.graph.clone", "graph", graph.Clone, registry.CanonicalReads, "Clone graph", []string{"apoc.graph.clone(graph) => cloned"})
	register("apoc.graph.stats", "graph", graph.Stats, registry.CanonicalReads, "Graph statistics", []string{"apoc.graph.stats(graph) => stats"})
	register("apoc.graph.toMap", "graph", graph.ToMap, registry.CanonicalReads, "Graph to map", []string{"apoc.graph.toMap(graph) => map"})
	register("apoc.graph.fromMap", "graph", graph.FromMap, registry.CanonicalReads, "Graph from map", []string{"apoc.graph.fromMap(map) => graph"})
	register("apoc.graph.subgraph", "graph", graph.Subgraph, registry.CanonicalReads, "Extract subgraph", []string{"apoc.graph.subgraph(graph, filter) => subgraph"})

	// ========================================
	// Hashing Functions (apoc.hashing.*)
	// ========================================
	register("apoc.hashing.md5", "hashing", hashing.MD5, registry.CanonicalReads, "MD5 hash", []string{"apoc.hashing.md5('hello') => hash"})
	register("apoc.hashing.sha1", "hashing", hashing.SHA1, registry.CanonicalReads, "SHA1 hash", []string{"apoc.hashing.sha1('hello') => hash"})
	register("apoc.hashing.sha256", "hashing", hashing.SHA256, registry.CanonicalReads, "SHA256 hash", []string{"apoc.hashing.sha256('hello') => hash"})
	register("apoc.hashing.sha384", "hashing", hashing.SHA384, registry.CanonicalReads, "SHA384 hash", []string{"apoc.hashing.sha384('hello') => hash"})
	register("apoc.hashing.sha512", "hashing", hashing.SHA512, registry.CanonicalReads, "SHA512 hash", []string{"apoc.hashing.sha512('hello') => hash"})
	register("apoc.hashing.fnv1", "hashing", hashing.FNV1, registry.CanonicalReads, "FNV-1 hash", []string{"apoc.hashing.fnv1('hello') => hash"})
	register("apoc.hashing.fnv1a", "hashing", hashing.FNV1a, registry.CanonicalReads, "FNV-1a hash", []string{"apoc.hashing.fnv1a('hello') => hash"})
	register("apoc.hashing.fnv164", "hashing", hashing.FNV164, registry.CanonicalReads, "FNV-1 64-bit", []string{"apoc.hashing.fnv164('hello') => hash"})
	register("apoc.hashing.fnv1a64", "hashing", hashing.FNV1a64, registry.CanonicalReads, "FNV-1a 64-bit", []string{"apoc.hashing.fnv1a64('hello') => hash"})
	register("apoc.hashing.murmurHash3", "hashing", hashing.MurmurHash3, registry.CanonicalReads, "MurmurHash3", []string{"apoc.hashing.murmurHash3('hello') => hash"})
	register("apoc.hashing.cityHash64", "hashing", hashing.CityHash64, registry.CanonicalReads, "CityHash64", []string{"apoc.hashing.cityHash64('hello') => hash"})
	register("apoc.hashing.xxHash32", "hashing", hashing.XXHash32, registry.CanonicalReads, "xxHash32", []string{"apoc.hashing.xxHash32('hello') => hash"})
	register("apoc.hashing.xxHash64", "hashing", hashing.XXHash64, registry.CanonicalReads, "xxHash64", []string{"apoc.hashing.xxHash64('hello') => hash"})
	register("apoc.hashing.fingerprint", "hashing", hashing.Fingerprint, registry.CanonicalReads, "Fingerprint", []string{"apoc.hashing.fingerprint(node) => hash"})
	register("apoc.hashing.fingerprintGraph", "hashing", hashing.FingerprintGraph, registry.CanonicalReads, "Graph fingerprint", []string{"apoc.hashing.fingerprintGraph(graph) => hash"})
	register("apoc.hashing.consistentHash", "hashing", hashing.ConsistentHash, registry.CanonicalReads, "Consistent hash", []string{"apoc.hashing.consistentHash(key, buckets) => bucket"})
	register("apoc.hashing.rendezvousHash", "hashing", hashing.RendezvousHash, registry.CanonicalReads, "Rendezvous hash", []string{"apoc.hashing.rendezvousHash(key, nodes) => node"})
	register("apoc.hashing.jumpHash", "hashing", hashing.JumpHash, registry.CanonicalReads, "Jump consistent hash", []string{"apoc.hashing.jumpHash(key, buckets) => bucket"})

	// ========================================
	// Import Functions (apoc.import.*)
	// ========================================
	register("apoc.import.json", "import", apocimport.Json, registry.CanonicalReads, "Import JSON", []string{"apoc.import.json('/path/to/file.json') => graph"})
	register("apoc.import.jsonData", "import", apocimport.JsonData, registry.CanonicalReads, "Import JSON data", []string{"apoc.import.jsonData(jsonString) => graph"})
	register("apoc.import.csv", "import", apocimport.Csv, registry.CanonicalReads, "Import CSV", []string{"apoc.import.csv('/path/to/file.csv') => rows"})
	register("apoc.import.csvData", "import", apocimport.CsvData, registry.CanonicalReads, "Import CSV data", []string{"apoc.import.csvData(csvString) => rows"})
	register("apoc.import.graphML", "import", apocimport.GraphML, registry.CanonicalReads, "Import GraphML", []string{"apoc.import.graphML('/path/to/file.graphml') => graph"})
	register("apoc.import.graphMLData", "import", apocimport.GraphMLData, registry.CanonicalReads, "Import GraphML data", []string{"apoc.import.graphMLData(xmlString) => graph"})
	register("apoc.import.cypher", "import", apocimport.Cypher, registry.CanonicalReads, "Import via cypher", []string{"apoc.import.cypher('/path/to/file.cypher') => results"})
	register("apoc.import.cypherData", "import", apocimport.CypherData, registry.CanonicalReads, "Import cypher data", []string{"apoc.import.cypherData(queries) => results"})
	register("apoc.import.file", "import", apocimport.File, registry.CanonicalReads, "Import file", []string{"apoc.import.file('/path/to/file') => data"})
	register("apoc.import.url", "import", apocimport.Url, registry.CanonicalReads, "Import from URL", []string{"apoc.import.url('http://...') => data"})
	register("apoc.import.stream", "import", apocimport.Stream, registry.CanonicalReads, "Stream import", []string{"apoc.import.stream(reader) => data"})
	register("apoc.import.batch", "import", apocimport.Batch, registry.OpaqueReads, "Batch import", []string{"apoc.import.batch(items, batchSize) => results"})
	register("apoc.import.parseCsvLine", "import", apocimport.ParseCsvLine, registry.CanonicalReads, "Parse CSV line", []string{"apoc.import.parseCsvLine(line) => fields"})
	register("apoc.import.parseJsonLine", "import", apocimport.ParseJsonLine, registry.CanonicalReads, "Parse JSON line", []string{"apoc.import.parseJsonLine(line) => object"})
	register("apoc.import.convertType", "import", apocimport.ConvertType, registry.CanonicalReads, "Convert type", []string{"apoc.import.convertType(value, type) => converted"})
	register("apoc.import.validateSchema", "import", apocimport.ValidateSchema, registry.CanonicalReads, "Validate schema", []string{"apoc.import.validateSchema(data, schema) => valid"})
	register("apoc.import.transform", "import", apocimport.Transform, registry.OpaqueReads, "Transform data", []string{"apoc.import.transform(data, fn) => transformed"})
	register("apoc.import.filter", "import", apocimport.Filter, registry.OpaqueReads, "Filter data", []string{"apoc.import.filter(data, predicate) => filtered"})
	register("apoc.import.merge", "import", apocimport.Merge, registry.CanonicalReads, "Merge imports", []string{"apoc.import.merge(data1, data2) => merged"})

	// ========================================
	// Label Functions (apoc.label.*)
	// ========================================
	register("apoc.label.exists", "label", fns.label.Exists, registry.OpaqueReads, "Label exists", []string{"apoc.label.exists(node, 'Person') => true"})
	register("apoc.label.list", "label", fns.label.List, registry.OpaqueReads, "List labels", []string{"apoc.label.list() => ['Person', 'Company']"})
	register("apoc.label.count", "label", fns.label.Count, registry.OpaqueReads, "Count label", []string{"apoc.label.count('Person') => 100"})
	register("apoc.label.nodes", "label", fns.label.Nodes, registry.OpaqueReads, "Nodes with label", []string{"apoc.label.nodes('Person') => nodes"})
	register("apoc.label.add", "label", label.Add, registry.CanonicalReads, "Add label", []string{"apoc.label.add(node, 'Person') => node"})
	register("apoc.label.remove", "label", label.Remove, registry.CanonicalReads, "Remove label", []string{"apoc.label.remove(node, 'Person') => node"})
	register("apoc.label.replace", "label", label.Replace, registry.CanonicalReads, "Replace labels", []string{"apoc.label.replace(node, ['Old'], ['New']) => node"})
	register("apoc.label.has", "label", label.Has, registry.CanonicalReads, "Has label", []string{"apoc.label.has(node, 'Person') => true"})
	register("apoc.label.hasAny", "label", label.HasAny, registry.CanonicalReads, "Has any label", []string{"apoc.label.hasAny(node, ['Person','Company']) => true"})
	register("apoc.label.hasAll", "label", label.HasAll, registry.CanonicalReads, "Has all labels", []string{"apoc.label.hasAll(node, ['Person','Employee']) => true"})
	register("apoc.label.get", "label", label.Get, registry.CanonicalReads, "Get labels", []string{"apoc.label.get(node) => ['Person']"})
	register("apoc.label.set", "label", label.Set, registry.CanonicalReads, "Set labels", []string{"apoc.label.set(node, ['Person']) => node"})
	register("apoc.label.clear", "label", label.Clear, registry.CanonicalReads, "Clear labels", []string{"apoc.label.clear(node) => node"})
	register("apoc.label.merge", "label", label.Merge, registry.CanonicalReads, "Merge labels", []string{"apoc.label.merge(node, labels) => node"})
	register("apoc.label.diff", "label", label.Diff, registry.CanonicalReads, "Diff labels", []string{"apoc.label.diff(node1, node2) => diff"})
	register("apoc.label.union", "label", label.Union, registry.CanonicalReads, "Union labels", []string{"apoc.label.union(node1, node2) => labels"})
	register("apoc.label.intersection", "label", label.Intersection, registry.CanonicalReads, "Intersect labels", []string{"apoc.label.intersection(node1, node2) => labels"})
	register("apoc.label.validate", "label", label.Validate, registry.CanonicalReads, "Validate label", []string{"apoc.label.validate('Person') => true"})
	register("apoc.label.normalize", "label", label.Normalize, registry.CanonicalReads, "Normalize label", []string{"apoc.label.normalize('person') => 'Person'"})
	register("apoc.label.pattern", "label", label.Pattern, registry.CanonicalReads, "Label pattern", []string{"apoc.label.pattern('Person') => pattern"})
	register("apoc.label.fromPattern", "label", label.FromPattern, registry.CanonicalReads, "From pattern", []string{"apoc.label.fromPattern(pattern) => labels"})
	register("apoc.label.stats", "label", fns.label.Stats, registry.OpaqueReads, "Label stats", []string{"apoc.label.stats() => stats"})
	register("apoc.label.search", "label", fns.label.Search, registry.OpaqueReads, "Search labels", []string{"apoc.label.search('Pers*') => labels"})
	register("apoc.label.compare", "label", label.Compare, registry.CanonicalReads, "Compare labels", []string{"apoc.label.compare(node1, node2) => result"})
	register("apoc.label.toString", "label", label.ToString, registry.CanonicalReads, "Labels to string", []string{"apoc.label.toString(labels) => 'Person:Employee'"})
	register("apoc.label.fromString", "label", label.FromString, registry.CanonicalReads, "Labels from string", []string{"apoc.label.fromString('Person:Employee') => ['Person','Employee']"})
	register("apoc.label.format", "label", label.Format, registry.CanonicalReads, "Format label", []string{"apoc.label.format(label, style) => formatted"})

	// ========================================
	// Lock Functions (apoc.lock.*)
	// ========================================
	register("apoc.lock.nodes", "lock", lock.Nodes, registry.CanonicalReads, "Lock nodes", []string{"apoc.lock.nodes(nodes) => locked"})
	register("apoc.lock.readNodes", "lock", lock.ReadNodes, registry.CanonicalReads, "Read lock nodes", []string{"apoc.lock.readNodes(nodes) => locked"})
	register("apoc.lock.unlockNodes", "lock", lock.UnlockNodes, registry.CanonicalReads, "Unlock nodes", []string{"apoc.lock.unlockNodes(nodes) => unlocked"})
	register("apoc.lock.relationships", "lock", lock.Relationships, registry.CanonicalReads, "Lock relationships", []string{"apoc.lock.relationships(rels) => locked"})
	register("apoc.lock.readRelationships", "lock", lock.ReadRelationships, registry.CanonicalReads, "Read lock rels", []string{"apoc.lock.readRelationships(rels) => locked"})
	register("apoc.lock.unlockRelationships", "lock", lock.UnlockRelationships, registry.CanonicalReads, "Unlock rels", []string{"apoc.lock.unlockRelationships(rels) => unlocked"})
	register("apoc.lock.all", "lock", lock.All, registry.CanonicalReads, "Lock all", []string{"apoc.lock.all(nodes, rels) => locked"})
	register("apoc.lock.unlockAll", "lock", lock.UnlockAll, registry.CanonicalReads, "Unlock all", []string{"apoc.lock.unlockAll() => unlocked"})
	register("apoc.lock.tryLock", "lock", lock.TryLock, registry.CanonicalReads, "Try lock", []string{"apoc.lock.tryLock(node) => success"})
	register("apoc.lock.isLocked", "lock", lock.IsLocked, registry.CanonicalReads, "Is locked", []string{"apoc.lock.isLocked(node) => bool"})
	register("apoc.lock.waitFor", "lock", lock.WaitFor, registry.CanonicalReads, "Wait for lock", []string{"apoc.lock.waitFor(node, timeout) => acquired"})
	register("apoc.lock.withLock", "lock", lock.WithLock, registry.OpaqueReads, "With lock", []string{"apoc.lock.withLock(node, fn) => result"})
	register("apoc.lock.withReadLock", "lock", lock.WithReadLock, registry.OpaqueReads, "With read lock", []string{"apoc.lock.withReadLock(node, fn) => result"})
	register("apoc.lock.batch", "lock", lock.Batch, registry.CanonicalReads, "Batch lock", []string{"apoc.lock.batch(nodes) => locked"})
	register("apoc.lock.unlockBatch", "lock", lock.UnlockBatch, registry.CanonicalReads, "Unlock batch", []string{"apoc.lock.unlockBatch(nodes) => unlocked"})
	register("apoc.lock.stats", "lock", lock.Stats, registry.CanonicalReads, "Lock stats", []string{"apoc.lock.stats() => stats"})
	register("apoc.lock.clear", "lock", lock.Clear, registry.CanonicalReads, "Clear locks", []string{"apoc.lock.clear() => cleared"})
	register("apoc.lock.detectDeadlock", "lock", lock.DetectDeadlock, registry.CanonicalReads, "Detect deadlock", []string{"apoc.lock.detectDeadlock() => hasDeadlock"})
	register("apoc.lock.priority", "lock", lock.Priority, registry.CanonicalReads, "Set priority", []string{"apoc.lock.priority(node, priority) => set"})

	// ========================================
	// Merge Functions (apoc.merge.*)
	// ========================================
	register("apoc.merge.mergeNode", "merge", merge.MergeNode, registry.CanonicalReads, "Merge node", []string{"apoc.merge.mergeNode(['Person'], {name:'Alice'}) => node"})
	register("apoc.merge.nodeEager", "merge", merge.NodeEager, registry.CanonicalReads, "Eager merge node", []string{"apoc.merge.nodeEager(['Person'], {name:'Alice'}) => node"})
	register("apoc.merge.mergeRelationship", "merge", merge.MergeRelationship, registry.CanonicalReads, "Merge relationship", []string{"apoc.merge.mergeRelationship(n1, 'KNOWS', n2) => rel"})
	register("apoc.merge.relationshipEager", "merge", merge.RelationshipEager, registry.CanonicalReads, "Eager merge rel", []string{"apoc.merge.relationshipEager(n1, 'KNOWS', n2) => rel"})
	register("apoc.merge.nodes", "merge", merge.Nodes, registry.CanonicalReads, "Merge nodes", []string{"apoc.merge.nodes(nodes) => merged"})
	register("apoc.merge.properties", "merge", merge.Properties, registry.CanonicalReads, "Merge properties", []string{"apoc.merge.properties(node, props) => node"})
	register("apoc.merge.deepMerge", "merge", merge.DeepMerge, registry.CanonicalReads, "Deep merge", []string{"apoc.merge.deepMerge(map1, map2) => merged"})
	register("apoc.merge.labels", "merge", merge.Labels, registry.CanonicalReads, "Merge labels", []string{"apoc.merge.labels(node, labels) => node"})
	register("apoc.merge.pattern", "merge", merge.Pattern, registry.CanonicalReads, "Merge pattern", []string{"apoc.merge.pattern(pattern, props) => result"})
	register("apoc.merge.batch", "merge", merge.Batch, registry.CanonicalReads, "Batch merge", []string{"apoc.merge.batch(items, config) => results"})
	register("apoc.merge.conditional", "merge", merge.Conditional, registry.CanonicalReads, "Conditional merge", []string{"apoc.merge.conditional(condition, config) => result"})
	register("apoc.merge.strategy", "merge", merge.Strategy, registry.CanonicalReads, "Set merge strategy", []string{"apoc.merge.strategy('COMBINE') => strategy"})
	register("apoc.merge.conflict", "merge", merge.Conflict, registry.OpaqueReads, "Handle conflict", []string{"apoc.merge.conflict(node1, node2, strategy) => merged"})
	register("apoc.merge.validate", "merge", merge.Validate, registry.CanonicalReads, "Validate merge", []string{"apoc.merge.validate(props) => valid"})
	register("apoc.merge.preview", "merge", merge.Preview, registry.CanonicalReads, "Preview merge", []string{"apoc.merge.preview(config) => preview"})
	register("apoc.merge.rollback", "merge", merge.Rollback, registry.CanonicalReads, "Rollback merge", []string{"apoc.merge.rollback(mergeId) => rolledBack"})
	register("apoc.merge.snapshot", "merge", merge.Snapshot, registry.CanonicalReads, "Snapshot merge", []string{"apoc.merge.snapshot(mergeId) => snapshot"})

	// ========================================
	// Meta Functions (apoc.meta.*)
	// ========================================
	register("apoc.meta.schema", "meta", fns.meta.Schema, registry.OpaqueReads, "Database schema", []string{"apoc.meta.schema() => schema"})
	register("apoc.meta.graph", "meta", fns.meta.Graph, registry.OpaqueReads, "Meta graph", []string{"apoc.meta.graph() => graph"})
	register("apoc.meta.stats", "meta", fns.meta.Stats, registry.OpaqueReads, "Database stats", []string{"apoc.meta.stats() => stats"})
	register("apoc.meta.type", "meta", meta.Type, registry.CanonicalReads, "Get type", []string{"apoc.meta.type(value) => 'STRING'"})
	register("apoc.meta.typeOf", "meta", meta.TypeOf, registry.CanonicalReads, "Type of value", []string{"apoc.meta.typeOf(value) => 'STRING'"})
	register("apoc.meta.types", "meta", meta.Types, registry.CanonicalReads, "Get all types", []string{"apoc.meta.types(map) => {key:'STRING',...}"})
	register("apoc.meta.nodeTypeProperties", "meta", meta.NodeTypeProperties, registry.CanonicalReads, "Node type props", []string{"apoc.meta.nodeTypeProperties() => props"})
	register("apoc.meta.relTypeProperties", "meta", meta.RelTypeProperties, registry.CanonicalReads, "Rel type props", []string{"apoc.meta.relTypeProperties() => props"})
	register("apoc.meta.data", "meta", meta.Data, registry.CanonicalReads, "Meta data", []string{"apoc.meta.data() => data"})
	register("apoc.meta.subGraph", "meta", meta.SubGraph, registry.CanonicalReads, "Meta subgraph", []string{"apoc.meta.subGraph({labels:['Person']}) => graph"})
	register("apoc.meta.isType", "meta", meta.IsType, registry.CanonicalReads, "Check type", []string{"apoc.meta.isType(value, 'STRING') => true"})
	register("apoc.meta.cypherType", "meta", meta.Type, registry.CanonicalReads, "Cypher type", []string{"apoc.meta.cypherType(value) => 'STRING'"})
	register("apoc.meta.isNode", "meta", meta.IsNode, registry.CanonicalReads, "Is node", []string{"apoc.meta.isNode(value) => true"})
	register("apoc.meta.isRelationship", "meta", meta.IsRelationship, registry.CanonicalReads, "Is relationship", []string{"apoc.meta.isRelationship(value) => true"})
	register("apoc.meta.isPath", "meta", meta.IsPath, registry.CanonicalReads, "Is path", []string{"apoc.meta.isPath(value) => true"})
	register("apoc.meta.nodeLabels", "meta", fns.meta.NodeLabels, registry.OpaqueReads, "All labels", []string{"apoc.meta.nodeLabels() => ['Person',...]"})
	register("apoc.meta.relTypes", "meta", fns.meta.RelTypes, registry.OpaqueReads, "All rel types", []string{"apoc.meta.relTypes() => ['KNOWS',...]"})
	register("apoc.meta.propertyKeys", "meta", fns.meta.PropertyKeys, registry.OpaqueReads, "All prop keys", []string{"apoc.meta.propertyKeys() => ['name',...]"})
	register("apoc.meta.constraints", "meta", meta.Constraints, registry.CanonicalReads, "Get constraints", []string{"apoc.meta.constraints() => constraints"})
	register("apoc.meta.indexes", "meta", meta.Indexes, registry.CanonicalReads, "Get indexes", []string{"apoc.meta.indexes() => indexes"})
	register("apoc.meta.procedures", "meta", meta.Procedures, registry.CanonicalReads, "List procedures", []string{"apoc.meta.procedures() => procedures"})
	register("apoc.meta.functions", "meta", meta.Functions, registry.CanonicalReads, "List functions", []string{"apoc.meta.functions() => functions"})
	register("apoc.meta.version", "meta", meta.Version, registry.CanonicalReads, "Get version", []string{"apoc.meta.version() => '4.4.0'"})
	register("apoc.meta.config", "meta", meta.Config, registry.CanonicalReads, "Get config", []string{"apoc.meta.config() => config"})
	register("apoc.meta.compare", "meta", meta.Compare, registry.CanonicalReads, "Compare schemas", []string{"apoc.meta.compare(schema1, schema2) => diff"})
	register("apoc.meta.validate", "meta", meta.Validate, registry.CanonicalReads, "Validate schema", []string{"apoc.meta.validate(schema) => valid"})
	register("apoc.meta.export", "meta", fns.meta.Schema, registry.OpaqueReads, "Export schema", []string{"apoc.meta.export() => schema"})
	register("apoc.meta.import", "meta", meta.Import, registry.CanonicalReads, "Import schema", []string{"apoc.meta.import(schema) => result"})
	register("apoc.meta.cypherTypes", "meta", meta.Types, registry.CanonicalReads, "Cypher types", []string{"apoc.meta.cypherTypes(map) => types"})
	register("apoc.meta.diff", "meta", meta.Compare, registry.CanonicalReads, "Diff schemas", []string{"apoc.meta.diff(s1, s2) => diff"})
	register("apoc.meta.snapshot", "meta", fns.meta.Schema, registry.OpaqueReads, "Schema snapshot", []string{"apoc.meta.snapshot() => snapshot"})
	register("apoc.meta.restore", "meta", meta.Import, registry.CanonicalReads, "Restore schema", []string{"apoc.meta.restore(snapshot) => result"})
	register("apoc.meta.analyze", "meta", meta.Analyze, registry.CanonicalReads, "Analyze database", []string{"apoc.meta.analyze() => analysis"})
	register("apoc.meta.cardinality", "meta", meta.Cardinality, registry.CanonicalReads, "Get cardinality", []string{"apoc.meta.cardinality('Person') => count"})
	register("apoc.meta.pattern", "meta", meta.Pattern, registry.CanonicalReads, "Schema pattern", []string{"apoc.meta.pattern() => pattern"})
	register("apoc.meta.toString", "meta", meta.ToString, registry.CanonicalReads, "Schema to string", []string{"apoc.meta.toString() => string"})
	register("apoc.meta.fromString", "meta", meta.FromString, registry.CanonicalReads, "Schema from string", []string{"apoc.meta.fromString(str) => schema"})

	// ========================================
	// Nodes Functions (apoc.nodes.*)
	// ========================================
	register("apoc.nodes.get", "nodes", fns.nodes.Get, registry.OpaqueReads, "Get nodes", []string{"apoc.nodes.get([1,2,3]) => nodes"})
	register("apoc.nodes.delete", "nodes", fns.nodes.Delete, registry.OpaqueReads, "Delete nodes", []string{"apoc.nodes.delete(nodes) => count"})
	register("apoc.nodes.link", "nodes", fns.nodes.Link, registry.OpaqueReads, "Link nodes", []string{"apoc.nodes.link(nodes, 'NEXT') => rels"})
	register("apoc.nodes.collapse", "nodes", fns.nodes.Collapse, registry.OpaqueReads, "Collapse nodes", []string{"apoc.nodes.collapse(nodes) => node"})
	register("apoc.nodes.group", "nodes", nodes.Group, registry.CanonicalReads, "Group nodes", []string{"apoc.nodes.group(['label'], ['prop']) => groups"})
	register("apoc.nodes.distinct", "nodes", nodes.Distinct, registry.CanonicalReads, "Distinct nodes", []string{"apoc.nodes.distinct(nodes) => unique"})
	register("apoc.nodes.connected", "nodes", fns.nodes.Connected, registry.OpaqueReads, "Connected nodes", []string{"apoc.nodes.connected(node1, node2) => true"})
	register("apoc.nodes.isDense", "nodes", fns.nodes.IsDense, registry.OpaqueReads, "Is dense node", []string{"apoc.nodes.isDense(node) => true"})
	register("apoc.nodes.relationships", "nodes", fns.nodes.Relationships, registry.OpaqueReads, "Node relationships", []string{"apoc.nodes.relationships(node) => rels"})
	register("apoc.nodes.cycles", "nodes", fns.nodes.Cycles, registry.OpaqueReads, "Find cycles", []string{"apoc.nodes.cycles(nodes) => cycles"})
	register("apoc.nodes.partition", "nodes", nodes.Partition, registry.CanonicalReads, "Partition nodes", []string{"apoc.nodes.partition(nodes, predicate) => [matching, nonMatching]"})
	register("apoc.nodes.intersect", "nodes", nodes.Intersect, registry.CanonicalReads, "Intersect nodes", []string{"apoc.nodes.intersect(nodes1, nodes2) => common"})
	register("apoc.nodes.union", "nodes", nodes.Union, registry.CanonicalReads, "Union nodes", []string{"apoc.nodes.union(nodes1, nodes2) => all"})
	register("apoc.nodes.difference", "nodes", nodes.Difference, registry.CanonicalReads, "Difference nodes", []string{"apoc.nodes.difference(nodes1, nodes2) => diff"})
	register("apoc.nodes.sort", "nodes", nodes.Sort, registry.CanonicalReads, "Sort nodes", []string{"apoc.nodes.sort(nodes, prop) => sorted"})
	register("apoc.nodes.filter", "nodes", nodes.Filter, registry.OpaqueReads, "Filter nodes", []string{"apoc.nodes.filter(nodes, predicate) => filtered"})
	register("apoc.nodes.map", "nodes", nodes.Map, registry.OpaqueReads, "Map nodes", []string{"apoc.nodes.map(nodes, fn) => mapped"})
	register("apoc.nodes.reduce", "nodes", nodes.Reduce, registry.OpaqueReads, "Reduce nodes", []string{"apoc.nodes.reduce(nodes, fn, init) => result"})
	register("apoc.nodes.distinctRels", "nodes", nodes.DistinctRels, registry.CanonicalReads, "Distinct rels", []string{"apoc.nodes.distinctRels(node) => rels"})
	register("apoc.nodes.toMap", "nodes", nodes.ToMap, registry.CanonicalReads, "Nodes to map", []string{"apoc.nodes.toMap(nodes) => map"})
	register("apoc.nodes.fromMap", "nodes", fns.nodes.FromMap, registry.OpaqueReads, "Nodes from map", []string{"apoc.nodes.fromMap(map) => nodes"})
	register("apoc.nodes.batch", "nodes", nodes.Batch, registry.OpaqueReads, "Batch nodes", []string{"apoc.nodes.batch(nodes, batchSize, fn) => results"})

	// ========================================
	// Number Functions (apoc.number.*)
	// ========================================
	register("apoc.number.format", "number", number.Format, registry.CanonicalReads, "Format number", []string{"apoc.number.format(12345.67, '#,##0.00') => '12,345.67'"})
	register("apoc.number.parse", "number", number.Parse, registry.CanonicalReads, "Parse number", []string{"apoc.number.parse('12,345.67', '#,##0.00') => 12345.67"})
	register("apoc.number.parseInt", "number", number.ParseInt, registry.CanonicalReads, "Parse integer", []string{"apoc.number.parseInt('42') => 42"})
	register("apoc.number.parseFloat", "number", number.ParseFloat, registry.CanonicalReads, "Parse float", []string{"apoc.number.parseFloat('3.14') => 3.14"})
	register("apoc.number.exact", "number", number.Exact, registry.CanonicalReads, "Exact number", []string{"apoc.number.exact(3.14159, 2) => 3.14"})
	register("apoc.number.arabize", "number", number.Arabize, registry.CanonicalReads, "Roman to arabic", []string{"apoc.number.arabize('XIV') => 14"})
	register("apoc.number.romanize", "number", number.Romanize, registry.CanonicalReads, "Arabic to roman", []string{"apoc.number.romanize(14) => 'XIV'"})
	register("apoc.number.toHex", "number", number.ToHex, registry.CanonicalReads, "To hexadecimal", []string{"apoc.number.toHex(255) => 'FF'"})
	register("apoc.number.fromHex", "number", number.FromHex, registry.CanonicalReads, "From hexadecimal", []string{"apoc.number.fromHex('FF') => 255"})
	register("apoc.number.toOctal", "number", number.ToOctal, registry.CanonicalReads, "To octal", []string{"apoc.number.toOctal(8) => '10'"})
	register("apoc.number.fromOctal", "number", number.FromOctal, registry.CanonicalReads, "From octal", []string{"apoc.number.fromOctal('10') => 8"})
	register("apoc.number.toBinary", "number", number.ToBinary, registry.CanonicalReads, "To binary", []string{"apoc.number.toBinary(5) => '101'"})
	register("apoc.number.fromBinary", "number", number.FromBinary, registry.CanonicalReads, "From binary", []string{"apoc.number.fromBinary('101') => 5"})
	register("apoc.number.toBase", "number", number.ToBase, registry.CanonicalReads, "To base N", []string{"apoc.number.toBase(255, 16) => 'FF'"})
	register("apoc.number.fromBase", "number", number.FromBase, registry.CanonicalReads, "From base N", []string{"apoc.number.fromBase('FF', 16) => 255"})
	register("apoc.number.round", "number", number.Round, registry.CanonicalReads, "Round number", []string{"apoc.number.round(3.14159, 2) => 3.14"})
	register("apoc.number.ceil", "number", number.Ceil, registry.CanonicalReads, "Ceiling", []string{"apoc.number.ceil(3.14) => 4"})
	register("apoc.number.floor", "number", number.Floor, registry.CanonicalReads, "Floor", []string{"apoc.number.floor(3.14) => 3"})
	register("apoc.number.abs", "number", number.Abs, registry.CanonicalReads, "Absolute value", []string{"apoc.number.abs(-5) => 5"})
	register("apoc.number.sign", "number", number.Sign, registry.CanonicalReads, "Get sign", []string{"apoc.number.sign(-5) => -1"})
	register("apoc.number.clamp", "number", number.Clamp, registry.CanonicalReads, "Clamp value", []string{"apoc.number.clamp(15, 0, 10) => 10"})
	register("apoc.number.lerp", "number", number.Lerp, registry.CanonicalReads, "Linear interpolation", []string{"apoc.number.lerp(0, 10, 0.5) => 5"})
	register("apoc.number.normalize", "number", number.Normalize, registry.CanonicalReads, "Normalize", []string{"apoc.number.normalize(5, 0, 10) => 0.5"})
	register("apoc.number.map", "number", number.Map, registry.CanonicalReads, "Map range", []string{"apoc.number.map(0.5, 0, 1, 0, 100) => 50"})
	register("apoc.number.isEven", "number", number.IsEven, registry.CanonicalReads, "Is even", []string{"apoc.number.isEven(4) => true"})
	register("apoc.number.isOdd", "number", number.IsOdd, registry.CanonicalReads, "Is odd", []string{"apoc.number.isOdd(3) => true"})
	register("apoc.number.isPrime", "number", number.IsPrime, registry.CanonicalReads, "Is prime", []string{"apoc.number.isPrime(7) => true"})
	register("apoc.number.gcd", "number", number.GCD, registry.CanonicalReads, "GCD", []string{"apoc.number.gcd(12, 8) => 4"})
	register("apoc.number.lcm", "number", number.LCM, registry.CanonicalReads, "LCM", []string{"apoc.number.lcm(4, 6) => 12"})
	register("apoc.number.factorial", "number", number.Factorial, registry.CanonicalReads, "Factorial", []string{"apoc.number.factorial(5) => 120"})
	register("apoc.number.fibonacci", "number", number.Fibonacci, registry.CanonicalReads, "Fibonacci", []string{"apoc.number.fibonacci(10) => 55"})
	register("apoc.number.power", "number", number.Power, registry.CanonicalReads, "Power", []string{"apoc.number.power(2, 8) => 256"})
	register("apoc.number.sqrt", "number", number.Sqrt, registry.CanonicalReads, "Square root", []string{"apoc.number.sqrt(16) => 4"})
	register("apoc.number.log", "number", number.Log, registry.CanonicalReads, "Natural log", []string{"apoc.number.log(2.718) => 1"})
	register("apoc.number.log10", "number", number.Log10, registry.CanonicalReads, "Log base 10", []string{"apoc.number.log10(100) => 2"})
	register("apoc.number.exp", "number", number.Exp, registry.CanonicalReads, "Exponential", []string{"apoc.number.exp(1) => 2.718"})
	register("apoc.number.random", "number", number.Random, registry.CanonicalReads, "Random float", []string{"apoc.number.random() => 0.42"})
	register("apoc.number.randomInt", "number", number.RandomInt, registry.CanonicalReads, "Random int", []string{"apoc.number.randomInt(1, 100) => 42"})

	// ========================================
	// Path Functions (apoc.path.*)
	// ========================================
	register("apoc.path.subgraphNodes", "path", fns.path.SubgraphNodes, registry.OpaqueReads, "Subgraph nodes", []string{"apoc.path.subgraphNodes(start, config) => nodes"})
	register("apoc.path.subgraphAll", "path", fns.path.SubgraphAll, registry.OpaqueReads, "All subgraph", []string{"apoc.path.subgraphAll(start, config) => {nodes,rels}"})
	register("apoc.path.expandConfig", "path", fns.path.ExpandConfig, registry.OpaqueReads, "Expand with config", []string{"apoc.path.expandConfig(start, config) => paths"})
	register("apoc.path.spanningTree", "path", fns.path.SpanningTree, registry.OpaqueReads, "Spanning tree", []string{"apoc.path.spanningTree(start, config) => paths"})
	register("apoc.path.shortestPath", "path", fns.path.ShortestPath, registry.OpaqueReads, "Shortest path", []string{"apoc.path.shortestPath(start, end) => path"})
	register("apoc.path.allShortestPaths", "path", fns.path.AllShortestPaths, registry.OpaqueReads, "All shortest paths", []string{"apoc.path.allShortestPaths(start, end) => paths"})
	register("apoc.path.combine", "path", path.Combine, registry.CanonicalReads, "Combine paths", []string{"apoc.path.combine(path1, path2) => path"})
	register("apoc.path.elements", "path", path.Elements, registry.CanonicalReads, "Path elements", []string{"apoc.path.elements(path) => elements"})
	register("apoc.path.slice", "path", path.Slice, registry.CanonicalReads, "Slice path", []string{"apoc.path.slice(path, 0, 3) => subpath"})

	// ========================================
	// Paths Functions (apoc.paths.*)
	// ========================================
	register("apoc.paths.all", "paths", fns.paths.All, registry.OpaqueReads, "All paths", []string{"apoc.paths.all(start, end, maxLength) => paths"})
	register("apoc.paths.shortest", "paths", fns.paths.Shortest, registry.OpaqueReads, "Shortest path", []string{"apoc.paths.shortest(start, end) => path"})
	register("apoc.paths.longest", "paths", fns.paths.Longest, registry.OpaqueReads, "Longest path", []string{"apoc.paths.longest(start, end, maxLength) => path"})
	register("apoc.paths.simple", "paths", fns.paths.Simple, registry.OpaqueReads, "Simple paths", []string{"apoc.paths.simple(start, end) => paths"})
	register("apoc.paths.cycles", "paths", fns.paths.Cycles, registry.OpaqueReads, "Find cycles", []string{"apoc.paths.cycles(start) => cycles"})
	register("apoc.paths.kShortest", "paths", fns.paths.KShortest, registry.OpaqueReads, "K shortest paths", []string{"apoc.paths.kShortest(start, end, k) => paths"})
	register("apoc.paths.count", "paths", fns.paths.Count, registry.OpaqueReads, "Count paths", []string{"apoc.paths.count(start, end) => count"})
	register("apoc.paths.exists", "paths", fns.paths.Exists, registry.OpaqueReads, "Path exists", []string{"apoc.paths.exists(start, end) => true"})
	register("apoc.paths.elementary", "paths", fns.paths.Elementary, registry.OpaqueReads, "Elementary paths", []string{"apoc.paths.elementary(start, end) => paths"})
	register("apoc.paths.disjoint", "paths", fns.paths.Disjoint, registry.OpaqueReads, "Disjoint paths", []string{"apoc.paths.disjoint(start, end) => paths"})
	register("apoc.paths.edgeDisjoint", "paths", fns.paths.EdgeDisjoint, registry.OpaqueReads, "Edge disjoint paths", []string{"apoc.paths.edgeDisjoint(start, end) => paths"})
	register("apoc.paths.hamiltonian", "paths", fns.paths.Hamiltonian, registry.OpaqueReads, "Hamiltonian path", []string{"apoc.paths.hamiltonian(nodes) => path"})
	register("apoc.paths.eulerian", "paths", paths.Eulerian, registry.CanonicalReads, "Eulerian path", []string{"apoc.paths.eulerian(nodes) => path"})
	register("apoc.paths.withLength", "paths", fns.paths.WithLength, registry.OpaqueReads, "Paths with length", []string{"apoc.paths.withLength(start, end, length) => paths"})
	register("apoc.paths.withinLength", "paths", fns.paths.WithinLength, registry.OpaqueReads, "Paths within length", []string{"apoc.paths.withinLength(start, end, maxLength) => paths"})
	register("apoc.paths.distance", "paths", fns.paths.Distance, registry.OpaqueReads, "Path distance", []string{"apoc.paths.distance(start, end) => distance"})
	register("apoc.paths.common", "paths", paths.Common, registry.CanonicalReads, "Common paths", []string{"apoc.paths.common(path1, path2) => common"})
	register("apoc.paths.unique", "paths", paths.Unique, registry.CanonicalReads, "Unique paths", []string{"apoc.paths.unique(paths) => unique"})
	register("apoc.paths.merge", "paths", paths.Merge, registry.CanonicalReads, "Merge paths", []string{"apoc.paths.merge(path1, path2) => merged"})
	register("apoc.paths.reverse", "paths", paths.Reverse, registry.CanonicalReads, "Reverse path", []string{"apoc.paths.reverse(path) => reversed"})
	register("apoc.paths.slice", "paths", paths.Slice, registry.CanonicalReads, "Slice path", []string{"apoc.paths.slice(path, start, end) => sliced"})

	// ========================================
	// Periodic Functions (apoc.periodic.*)
	// ========================================
	register("apoc.periodic.iterate", "periodic", periodic.Iterate, registry.CanonicalReads, "Iterate in batches", []string{"apoc.periodic.iterate('MATCH...', 'SET...', {batchSize:1000}) => stats"})
	register("apoc.periodic.commit", "periodic", periodic.Commit, registry.CanonicalReads, "Commit batches", []string{"apoc.periodic.commit('MATCH... SET...', {limit:1000}) => stats"})
	register("apoc.periodic.repeat", "periodic", periodic.Repeat, registry.CanonicalReads, "Repeat query", []string{"apoc.periodic.repeat('job', 'MATCH...', 60) => scheduled"})
	register("apoc.periodic.schedule", "periodic", periodic.Schedule, registry.CanonicalReads, "Schedule query", []string{"apoc.periodic.schedule('job', 'MATCH...', 60) => scheduled"})
	register("apoc.periodic.cancel", "periodic", periodic.Cancel, registry.CanonicalReads, "Cancel job", []string{"apoc.periodic.cancel('job') => cancelled"})
	register("apoc.periodic.list", "periodic", periodic.List, registry.CanonicalReads, "List jobs", []string{"apoc.periodic.list() => jobs"})
	register("apoc.periodic.submit", "periodic", periodic.Submit, registry.CanonicalReads, "Submit job", []string{"apoc.periodic.submit('job', 'MATCH...') => submitted"})
	register("apoc.periodic.rock", "periodic", periodic.Rock, registry.CanonicalReads, "Rock'n'Roll", []string{"apoc.periodic.rock('job', config) => results"})
	register("apoc.periodic.countdown", "periodic", periodic.Countdown, registry.CanonicalReads, "Countdown", []string{"apoc.periodic.countdown('job', count, fn) => results"})
	register("apoc.periodic.truncate", "periodic", periodic.Truncate, registry.CanonicalReads, "Truncate", []string{"apoc.periodic.truncate(config) => results"})

	// ========================================
	// Schema Functions (apoc.schema.*)
	// ========================================
	register("apoc.schema.assert", "schema", schema.Assert, registry.CanonicalReads, "Assert schema", []string{"apoc.schema.assert(indexes, constraints) => result"})
	register("apoc.schema.nodes", "schema", fns.schema.Nodes, registry.OpaqueReads, "Node schema", []string{"apoc.schema.nodes() => schema"})
	register("apoc.schema.relationships", "schema", fns.schema.Relationships, registry.OpaqueReads, "Rel schema", []string{"apoc.schema.relationships() => schema"})
	register("apoc.schema.nodeConstraints", "schema", schema.NodeConstraints, registry.CanonicalReads, "Node constraints", []string{"apoc.schema.nodeConstraints() => constraints"})
	register("apoc.schema.nodeIndexes", "schema", schema.NodeIndexes, registry.CanonicalReads, "Node indexes", []string{"apoc.schema.nodeIndexes() => indexes"})
	register("apoc.schema.properties", "schema", fns.meta.PropertyKeys, registry.OpaqueReads, "Schema properties", []string{"apoc.schema.properties('Person') => props"})
	register("apoc.schema.createIndex", "schema", schema.CreateIndex, registry.CanonicalReads, "Create index", []string{"apoc.schema.createIndex('Person', ['name']) => index"})
	register("apoc.schema.dropIndex", "schema", schema.DropIndex, registry.CanonicalReads, "Drop index", []string{"apoc.schema.dropIndex('Person', ['name']) => dropped"})
	register("apoc.schema.createConstraint", "schema", schema.CreateConstraint, registry.CanonicalReads, "Create constraint", []string{"apoc.schema.createConstraint('Person', ['name']) => constraint"})
	register("apoc.schema.dropConstraint", "schema", schema.DropConstraint, registry.CanonicalReads, "Drop constraint", []string{"apoc.schema.dropConstraint('Person', ['name']) => dropped"})
	register("apoc.schema.relationshipConstraints", "schema", schema.RelationshipConstraints, registry.CanonicalReads, "Rel constraints", []string{"apoc.schema.relationshipConstraints() => constraints"})
	register("apoc.schema.relationshipIndexes", "schema", schema.RelationshipIndexes, registry.CanonicalReads, "Rel indexes", []string{"apoc.schema.relationshipIndexes() => indexes"})
	register("apoc.schema.nodeConstraintExists", "schema", schema.NodeConstraintExists, registry.CanonicalReads, "Constraint exists", []string{"apoc.schema.nodeConstraintExists('Person', ['name']) => true"})
	register("apoc.schema.nodeIndexExists", "schema", schema.NodeIndexExists, registry.CanonicalReads, "Index exists", []string{"apoc.schema.nodeIndexExists('Person', ['name']) => true"})
	register("apoc.schema.propertiesDistinct", "schema", fns.schema.PropertiesDistinct, registry.OpaqueReads, "Distinct properties", []string{"apoc.schema.propertiesDistinct('Person', 'name') => values"})
	register("apoc.schema.labels", "schema", fns.schema.Labels, registry.OpaqueReads, "Get labels", []string{"apoc.schema.labels() => labels"})
	register("apoc.schema.types", "schema", fns.schema.Types, registry.OpaqueReads, "Get rel types", []string{"apoc.schema.types() => types"})
	register("apoc.schema.info", "schema", fns.schema.Info, registry.OpaqueReads, "Schema info", []string{"apoc.schema.info() => info"})
	register("apoc.schema.createUniqueConstraint", "schema", schema.CreateUniqueConstraint, registry.CanonicalReads, "Create unique", []string{"apoc.schema.createUniqueConstraint('Person', ['name']) => constraint"})
	register("apoc.schema.createExistsConstraint", "schema", schema.CreateExistsConstraint, registry.CanonicalReads, "Create exists", []string{"apoc.schema.createExistsConstraint('Person', 'name') => constraint"})
	register("apoc.schema.createNodeKeyConstraint", "schema", schema.CreateNodeKeyConstraint, registry.CanonicalReads, "Create node key", []string{"apoc.schema.createNodeKeyConstraint('Person', ['name']) => constraint"})
	register("apoc.schema.validate", "schema", schema.Validate, registry.CanonicalReads, "Validate schema", []string{"apoc.schema.validate() => valid"})
	register("apoc.schema.compare", "schema", schema.Compare, registry.CanonicalReads, "Compare schemas", []string{"apoc.schema.compare(s1, s2) => diff"})
	register("apoc.schema.export", "schema", fns.schema.Info, registry.OpaqueReads, "Export schema", []string{"apoc.schema.export() => schema"})
	register("apoc.schema.import", "schema", schema.Import, registry.CanonicalReads, "Import schema", []string{"apoc.schema.import(schema) => result"})
	register("apoc.schema.snapshot", "schema", fns.schema.Info, registry.OpaqueReads, "Schema snapshot", []string{"apoc.schema.snapshot() => snapshot"})
	register("apoc.schema.restore", "schema", schema.Import, registry.CanonicalReads, "Restore schema", []string{"apoc.schema.restore(snapshot) => result"})
	register("apoc.schema.stats", "schema", fns.schema.Stats, registry.OpaqueReads, "Schema stats", []string{"apoc.schema.stats() => stats"})
	register("apoc.schema.analyze", "schema", schema.Analyze, registry.CanonicalReads, "Analyze schema", []string{"apoc.schema.analyze() => analysis"})
	register("apoc.schema.optimize", "schema", schema.Optimize, registry.CanonicalReads, "Optimize schema", []string{"apoc.schema.optimize() => result"})

	// ========================================
	// Search Functions (apoc.search.*)
	// ========================================
	register("apoc.search.node", "search", fns.search.Node, registry.OpaqueReads, "Search node", []string{"apoc.search.node('Person', 'name', 'Alice') => nodes"})
	register("apoc.search.nodeAll", "search", fns.search.NodeAll, registry.OpaqueReads, "Search all nodes", []string{"apoc.search.nodeAll('Person', {name:'Alice'}) => nodes"})
	register("apoc.search.nodeReduced", "search", fns.search.NodeReduced, registry.OpaqueReads, "Reduced search", []string{"apoc.search.nodeReduced('Person', props) => nodes"})
	register("apoc.search.parallel", "search", fns.search.Parallel, registry.OpaqueReads, "Parallel search", []string{"apoc.search.parallel(queries) => results"})
	register("apoc.search.fullText", "search", fns.search.FullText, registry.OpaqueReads, "Full text search", []string{"apoc.search.fullText('index', 'query') => results"})
	register("apoc.search.fuzzy", "search", fns.search.Fuzzy, registry.OpaqueReads, "Fuzzy search", []string{"apoc.search.fuzzy('Person', 'name', 'Alise') => nodes"})
	register("apoc.search.regex", "search", fns.search.Regex, registry.OpaqueReads, "Regex search", []string{"apoc.search.regex('Person', 'name', 'A.*') => nodes"})
	register("apoc.search.prefix", "search", fns.search.Prefix, registry.OpaqueReads, "Prefix search", []string{"apoc.search.prefix('Person', 'name', 'Al') => nodes"})
	register("apoc.search.suffix", "search", fns.search.Suffix, registry.OpaqueReads, "Suffix search", []string{"apoc.search.suffix('Person', 'email', '.com') => nodes"})
	register("apoc.search.contains", "search", fns.search.Contains, registry.OpaqueReads, "Contains search", []string{"apoc.search.contains('Person', 'name', 'ice') => nodes"})
	register("apoc.search.range", "search", fns.search.Range, registry.OpaqueReads, "Range search", []string{"apoc.search.range('Person', 'age', 18, 65) => nodes"})
	register("apoc.search.in", "search", fns.search.In, registry.OpaqueReads, "In search", []string{"apoc.search.in('Person', 'name', ['Alice','Bob']) => nodes"})
	register("apoc.search.notIn", "search", fns.search.NotIn, registry.OpaqueReads, "Not in search", []string{"apoc.search.notIn('Person', 'name', ['Alice']) => nodes"})
	register("apoc.search.exists", "search", fns.search.Exists, registry.OpaqueReads, "Property exists", []string{"apoc.search.exists('Person', 'email') => nodes"})
	register("apoc.search.missing", "search", fns.search.Missing, registry.OpaqueReads, "Property missing", []string{"apoc.search.missing('Person', 'email') => nodes"})
	register("apoc.search.null", "search", fns.search.Null, registry.OpaqueReads, "Is null", []string{"apoc.search.null('Person', 'email') => nodes"})
	register("apoc.search.notNull", "search", fns.search.NotNull, registry.OpaqueReads, "Is not null", []string{"apoc.search.notNull('Person', 'email') => nodes"})
	register("apoc.search.match", "search", fns.search.Match, registry.OpaqueReads, "Pattern match", []string{"apoc.search.match('Person', 'name', 'Al*') => nodes"})
	register("apoc.search.score", "search", fns.search.Score, registry.CanonicalReads, "Search score", []string{"apoc.search.score(node, query) => score"})
	register("apoc.search.highlight", "search", search.Highlight, registry.CanonicalReads, "Highlight matches", []string{"apoc.search.highlight(text, query) => highlighted"})
	// One canonical suggestion owner answers both names: Suggest takes an optional
	// limit (default suggestionLimit), so autocomplete needs no Go wrapper.
	register("apoc.search.suggest", "search", fns.search.Suggest, registry.OpaqueReads, "Suggestions", []string{"apoc.search.suggest('Person', 'name', 'Ali') => suggestions"})
	register("apoc.search.autocomplete", "search", fns.search.Suggest, registry.OpaqueReads, "Autocomplete", []string{"apoc.search.autocomplete('Person', 'name', 'Al') => completions"})
	register("apoc.search.didYouMean", "search", fns.search.DidYouMean, registry.OpaqueReads, "Did you mean", []string{"apoc.search.didYouMean('Person', 'name', 'Alise') => 'Alice'"})
	register("apoc.search.index", "search", search.Index, registry.CanonicalReads, "Create index", []string{"apoc.search.index('Person', ['name']) => index"})
	register("apoc.search.dropIndex", "search", search.DropIndex, registry.CanonicalReads, "Drop index", []string{"apoc.search.dropIndex('Person', ['name']) => dropped"})
	register("apoc.search.reindex", "search", search.Reindex, registry.CanonicalReads, "Rebuild index", []string{"apoc.search.reindex('Person') => reindexed"})
	register("apoc.search.nodeAny", "search", fns.search.NodeAny, registry.OpaqueReads, "Search any", []string{"apoc.search.nodeAny('Person', props) => nodes"})
	register("apoc.search.multiSearchAll", "search", fns.search.MultiSearchAll, registry.OpaqueReads, "Multi search all", []string{"apoc.search.multiSearchAll(queries) => results"})
	register("apoc.search.multiSearchAny", "search", fns.search.MultiSearchAny, registry.OpaqueReads, "Multi search any", []string{"apoc.search.multiSearchAny(queries) => results"})

	// ========================================
	// Temporal Functions (apoc.temporal.*)
	// ========================================
	register("apoc.temporal.format", "temporal", temporal.Format, registry.CanonicalReads, "Format temporal", []string{"apoc.temporal.format(datetime, 'yyyy-MM-dd') => '2024-01-15'"})
	register("apoc.temporal.parse", "temporal", temporal.Parse, registry.CanonicalReads, "Parse temporal", []string{"apoc.temporal.parse('2024-01-15', 'yyyy-MM-dd') => datetime"})
	register("apoc.temporal.formatDuration", "temporal", temporal.FormatDuration, registry.CanonicalReads, "Format duration", []string{"apoc.temporal.formatDuration(duration) => 'P1D'"})
	register("apoc.temporal.toEpochMillis", "temporal", temporal.ToEpochMillis, registry.CanonicalReads, "To epoch ms", []string{"apoc.temporal.toEpochMillis(datetime) => 1705276800000"})
	register("apoc.temporal.fromEpochMillis", "temporal", temporal.FromEpochMillis, registry.CanonicalReads, "From epoch ms", []string{"apoc.temporal.fromEpochMillis(1705276800000) => datetime"})
	register("apoc.temporal.add", "temporal", temporal.Add, registry.CanonicalReads, "Add duration", []string{"apoc.temporal.add(datetime, duration) => datetime"})
	register("apoc.temporal.subtract", "temporal", temporal.Subtract, registry.CanonicalReads, "Subtract duration", []string{"apoc.temporal.subtract(datetime, duration) => datetime"})
	register("apoc.temporal.difference", "temporal", temporal.Difference, registry.CanonicalReads, "Difference", []string{"apoc.temporal.difference(dt1, dt2) => duration"})
	register("apoc.temporal.startOf", "temporal", temporal.StartOf, registry.CanonicalReads, "Start of unit", []string{"apoc.temporal.startOf(datetime, 'day') => datetime"})
	register("apoc.temporal.endOf", "temporal", temporal.EndOf, registry.CanonicalReads, "End of unit", []string{"apoc.temporal.endOf(datetime, 'day') => datetime"})
	register("apoc.temporal.isBetween", "temporal", temporal.IsBetween, registry.CanonicalReads, "Is between", []string{"apoc.temporal.isBetween(dt, start, end) => true"})
	register("apoc.temporal.dayOfWeek", "temporal", temporal.DayOfWeek, registry.CanonicalReads, "Day of week", []string{"apoc.temporal.dayOfWeek(datetime) => 1"})
	register("apoc.temporal.quarter", "temporal", temporal.Quarter, registry.CanonicalReads, "Quarter", []string{"apoc.temporal.quarter(datetime) => 1"})
	register("apoc.temporal.age", "temporal", temporal.Age, registry.CanonicalReads, "Calculate age", []string{"apoc.temporal.age(birthdate) => 25"})
	register("apoc.temporal.timezone", "temporal", temporal.Timezone, registry.CanonicalReads, "Get timezone", []string{"apoc.temporal.timezone(datetime) => 'UTC'"})
	register("apoc.temporal.duration", "temporal", temporal.Duration, registry.CanonicalReads, "Create duration", []string{"apoc.temporal.duration(value) => duration"})
	register("apoc.temporal.truncate", "temporal", temporal.Truncate, registry.CanonicalReads, "Truncate temporal", []string{"apoc.temporal.truncate(datetime, 'day') => truncated"})
	register("apoc.temporal.round", "temporal", temporal.Round, registry.CanonicalReads, "Round temporal", []string{"apoc.temporal.round(datetime, 'hour') => rounded"})
	register("apoc.temporal.toUTC", "temporal", temporal.ToUTC, registry.CanonicalReads, "Convert to UTC", []string{"apoc.temporal.toUTC(datetime) => utc"})
	register("apoc.temporal.toLocal", "temporal", temporal.ToLocal, registry.CanonicalReads, "Convert to local", []string{"apoc.temporal.toLocal(datetime) => local"})
	register("apoc.temporal.isWeekend", "temporal", temporal.IsWeekend, registry.CanonicalReads, "Is weekend", []string{"apoc.temporal.isWeekend(datetime) => bool"})
	register("apoc.temporal.isWeekday", "temporal", temporal.IsWeekday, registry.CanonicalReads, "Is weekday", []string{"apoc.temporal.isWeekday(datetime) => bool"})
	register("apoc.temporal.dayOfYear", "temporal", temporal.DayOfYear, registry.CanonicalReads, "Day of year", []string{"apoc.temporal.dayOfYear(datetime) => 15"})
	register("apoc.temporal.weekOfYear", "temporal", temporal.WeekOfYear, registry.CanonicalReads, "Week of year", []string{"apoc.temporal.weekOfYear(datetime) => 3"})
	register("apoc.temporal.isLeapYear", "temporal", temporal.IsLeapYear, registry.CanonicalReads, "Is leap year", []string{"apoc.temporal.isLeapYear(datetime) => bool"})
	register("apoc.temporal.daysInMonth", "temporal", temporal.DaysInMonth, registry.CanonicalReads, "Days in month", []string{"apoc.temporal.daysInMonth(datetime) => 31"})

	// ========================================
	// Trigger Functions (apoc.trigger.*)
	// ========================================
	register("apoc.trigger.add", "trigger", trigger.Add, registry.CanonicalReads, "Add trigger", []string{"apoc.trigger.add('name', 'MATCH...', {phase:'before'}) => trigger"})
	register("apoc.trigger.remove", "trigger", trigger.Remove, registry.CanonicalReads, "Remove trigger", []string{"apoc.trigger.remove('name') => removed"})
	register("apoc.trigger.removeAll", "trigger", trigger.RemoveAll, registry.CanonicalReads, "Remove all triggers", []string{"apoc.trigger.removeAll() => count"})
	register("apoc.trigger.list", "trigger", trigger.List, registry.CanonicalReads, "List triggers", []string{"apoc.trigger.list() => triggers"})
	register("apoc.trigger.pause", "trigger", trigger.Pause, registry.CanonicalReads, "Pause trigger", []string{"apoc.trigger.pause('name') => paused"})
	register("apoc.trigger.resume", "trigger", trigger.Resume, registry.CanonicalReads, "Resume trigger", []string{"apoc.trigger.resume('name') => resumed"})
	register("apoc.trigger.install", "trigger", trigger.Install, registry.CanonicalReads, "Install trigger", []string{"apoc.trigger.install('name', config) => installed"})
	register("apoc.trigger.drop", "trigger", trigger.Drop, registry.CanonicalReads, "Drop trigger", []string{"apoc.trigger.drop('name') => dropped"})
	register("apoc.trigger.enable", "trigger", trigger.Enable, registry.CanonicalReads, "Enable trigger", []string{"apoc.trigger.enable('name') => enabled"})
	register("apoc.trigger.disable", "trigger", trigger.Disable, registry.CanonicalReads, "Disable trigger", []string{"apoc.trigger.disable('name') => disabled"})
	register("apoc.trigger.show", "trigger", trigger.Show, registry.CanonicalReads, "Show trigger", []string{"apoc.trigger.show('name') => trigger"})
	register("apoc.trigger.nodeByLabel", "trigger", trigger.NodeByLabel, registry.CanonicalReads, "Node by label trigger", []string{"apoc.trigger.nodeByLabel('Person', fn) => trigger"})
	register("apoc.trigger.relationshipByType", "trigger", trigger.RelationshipByType, registry.CanonicalReads, "Rel by type trigger", []string{"apoc.trigger.relationshipByType('KNOWS', fn) => trigger"})
	register("apoc.trigger.onCreate", "trigger", trigger.OnCreate, registry.CanonicalReads, "On create trigger", []string{"apoc.trigger.onCreate('name', fn) => trigger"})
	register("apoc.trigger.onUpdate", "trigger", trigger.OnUpdate, registry.CanonicalReads, "On update trigger", []string{"apoc.trigger.onUpdate('name', fn) => trigger"})
	register("apoc.trigger.onDelete", "trigger", trigger.OnDelete, registry.CanonicalReads, "On delete trigger", []string{"apoc.trigger.onDelete('name', fn) => trigger"})
	register("apoc.trigger.before", "trigger", trigger.Before, registry.CanonicalReads, "Before trigger", []string{"apoc.trigger.before('name', fn) => trigger"})
	register("apoc.trigger.after", "trigger", trigger.After, registry.CanonicalReads, "After trigger", []string{"apoc.trigger.after('name', fn) => trigger"})
	register("apoc.trigger.afterAsync", "trigger", trigger.AfterAsync, registry.CanonicalReads, "Async after trigger", []string{"apoc.trigger.afterAsync('name', fn) => trigger"})
	register("apoc.trigger.isEnabled", "trigger", trigger.IsEnabled, registry.CanonicalReads, "Is enabled", []string{"apoc.trigger.isEnabled('name') => bool"})
	register("apoc.trigger.count", "trigger", trigger.Count, registry.CanonicalReads, "Trigger count", []string{"apoc.trigger.count() => count"})
	register("apoc.trigger.stats", "trigger", trigger.Stats, registry.CanonicalReads, "Trigger stats", []string{"apoc.trigger.stats() => stats"})
	register("apoc.trigger.export", "trigger", trigger.Export, registry.CanonicalReads, "Export triggers", []string{"apoc.trigger.export() => triggers"})
	register("apoc.trigger.import", "trigger", trigger.Import, registry.CanonicalReads, "Import triggers", []string{"apoc.trigger.import(triggers) => result"})

	// ========================================
	// Create Functions (apoc.create.*)
	// ========================================
	register("apoc.create.node", "create", create.Node, registry.CanonicalReads, "Create node", []string{"apoc.create.node(['Person'], {name:'Alice'}) => node"})
	register("apoc.create.nodes", "create", create.Nodes, registry.CanonicalReads, "Create nodes", []string{"apoc.create.nodes(['Person'], [{name:'Alice'},{name:'Bob'}]) => nodes"})
	register("apoc.create.relationship", "create", create.Relationship, registry.CanonicalReads, "Create relationship", []string{"apoc.create.relationship(n1, 'KNOWS', n2, {since:2024}) => rel"})
	register("apoc.create.vNode", "create", create.VNode, registry.CanonicalReads, "Create virtual node", []string{"apoc.create.vNode(['Person'], {name:'Alice'}) => vnode"})
	register("apoc.create.vNodes", "create", create.VNodes, registry.CanonicalReads, "Create virtual nodes", []string{"apoc.create.vNodes(['Person'], [{name:'Alice'}]) => vnodes"})
	register("apoc.create.vRelationship", "create", create.VRelationship, registry.CanonicalReads, "Create virtual rel", []string{"apoc.create.vRelationship(n1, 'KNOWS', n2, {}) => vrel"})
	register("apoc.create.vPattern", "create", create.VPattern, registry.CanonicalReads, "Create virtual pattern", []string{"apoc.create.vPattern({}, 'KNOWS', {}) => vpattern"})
	register("apoc.create.addLabels", "create", create.AddLabels, registry.CanonicalReads, "Add labels", []string{"apoc.create.addLabels(node, ['Label']) => node"})
	register("apoc.create.removeLabels", "create", create.RemoveLabels, registry.CanonicalReads, "Remove labels", []string{"apoc.create.removeLabels(node, ['Label']) => node"})
	register("apoc.create.setProperty", "create", create.SetProperty, registry.CanonicalReads, "Set property", []string{"apoc.create.setProperty(node, 'key', 'value') => node"})
	register("apoc.create.setProperties", "create", create.SetProperties, registry.CanonicalReads, "Set properties", []string{"apoc.create.setProperties(node, {key:'value'}) => node"})
	register("apoc.create.removeProperties", "create", create.RemoveProperties, registry.CanonicalReads, "Remove properties", []string{"apoc.create.removeProperties(node, ['key']) => node"})
	register("apoc.create.setRelProperty", "create", create.SetRelProperty, registry.CanonicalReads, "Set rel property", []string{"apoc.create.setRelProperty(rel, 'key', 'value') => rel"})
	register("apoc.create.setRelProperties", "create", create.SetRelProperties, registry.CanonicalReads, "Set rel properties", []string{"apoc.create.setRelProperties(rel, {key:'value'}) => rel"})
	register("apoc.create.removeRelProperties", "create", create.RemoveRelProperties, registry.CanonicalReads, "Remove rel props", []string{"apoc.create.removeRelProperties(rel, ['key']) => rel"})
	register("apoc.create.uuid", "create", create.UUID, registry.CanonicalReads, "Generate UUID", []string{"apoc.create.uuid() => '550e8400-e29b-41d4-a716-446655440000'"})
	register("apoc.create.uuids", "create", create.UUIDs, registry.CanonicalReads, "Generate UUIDs", []string{"apoc.create.uuids(5) => ['uuid1','uuid2',...]"})
	register("apoc.create.clone", "create", create.Clone, registry.CanonicalReads, "Clone node", []string{"apoc.create.clone(node) => clonedNode"})
	register("apoc.create.cloneSubgraph", "create", create.CloneSubgraph, registry.CanonicalReads, "Clone subgraph", []string{"apoc.create.cloneSubgraph(nodes, rels) => {nodes, rels}"})

	// ========================================
	// Export Functions (apoc.export.*)
	// ========================================
	register("apoc.export.json", "export", apocexport.Json, registry.CanonicalReads, "Export JSON", []string{"apoc.export.json(nodes, rels) => json"})
	register("apoc.export.jsonAll", "export", apocexport.JsonAll, registry.CanonicalReads, "Export all JSON", []string{"apoc.export.jsonAll(nodes, rels, '/path/to/file.json') => written"})
	register("apoc.export.jsonData", "export", apocexport.JsonAll, registry.CanonicalReads, "Export JSON data", []string{"apoc.export.jsonData(nodes, rels, '/path/to/file.json') => written"})
	register("apoc.export.csv", "export", apocexport.Csv, registry.CanonicalReads, "Export CSV", []string{"apoc.export.csv(nodes, '/path/to/file.csv') => written"})
	register("apoc.export.csvAll", "export", apocexport.Csv, registry.CanonicalReads, "Export all CSV", []string{"apoc.export.csvAll(nodes, '/path/to/file.csv') => written"})
	register("apoc.export.csvData", "export", apocexport.Csv, registry.CanonicalReads, "Export CSV data", []string{"apoc.export.csvData(nodes, '/path/to/file.csv') => written"})
	register("apoc.export.cypher", "export", apocexport.Cypher, registry.CanonicalReads, "Export Cypher", []string{"apoc.export.cypher(nodes, rels) => cypher"})
	register("apoc.export.cypherAll", "export", apocexport.CypherAll, registry.CanonicalReads, "Export all Cypher", []string{"apoc.export.cypherAll(nodes, rels, '/path/to/file.cypher') => written"})
	register("apoc.export.cypherData", "export", apocexport.CypherAll, registry.CanonicalReads, "Export Cypher data", []string{"apoc.export.cypherData(nodes, rels, '/path/to/file.cypher') => written"})
	register("apoc.export.graphML", "export", apocexport.GraphML, registry.CanonicalReads, "Export GraphML", []string{"apoc.export.graphML(nodes, rels) => graphml"})
	register("apoc.export.graphMLAll", "export", apocexport.GraphMLAll, registry.CanonicalReads, "Export all GraphML", []string{"apoc.export.graphMLAll(nodes, rels, '/path/to/file.graphml') => written"})
	register("apoc.export.graphMLData", "export", apocexport.GraphMLAll, registry.CanonicalReads, "Export GraphML data", []string{"apoc.export.graphMLData(nodes, rels, '/path/to/file.graphml') => written"})
	register("apoc.export.toFile", "export", apocexport.ToFile, registry.CanonicalReads, "Export to file", []string{"apoc.export.toFile(data, '/path/to/file') => result"})
	register("apoc.export.toString", "export", apocexport.ToString, registry.CanonicalReads, "Export to string", []string{"apoc.export.toString(nodes, rels, 'json') => string"})

	// ========================================
	// Load Functions (apoc.load.*)
	// ========================================
	register("apoc.load.json", "load", load.Json, registry.CanonicalReads, "Load JSON", []string{"apoc.load.json('/path/to/file.json') => data"})
	// Both JSON row shapes are registered against the canonical JSONItems factory:
	// apoc.load.jsonStream publishes a document's items (an array, or one item for
	// any other document); apoc.load.jsonArray requires an array. The same
	// DocumentItems handler feeds the native apoc.load.json procedures.
	register("apoc.load.jsonStream", "load", load.JSONItems(load.DocumentItems), registry.CanonicalReads, "Stream JSON", []string{"apoc.load.jsonStream('/path/to/file.json') => stream"})
	register("apoc.load.jsonArray", "load", load.JSONItems(load.RequireArray), registry.CanonicalReads, "Load JSON array", []string{"apoc.load.jsonArray('/path/to/file.json') => array"})
	register("apoc.load.jsonParams", "load", load.JsonParams, registry.CanonicalReads, "Load JSON params", []string{"apoc.load.jsonParams(url, params) => data"})
	register("apoc.load.csv", "load", load.Csv, registry.CanonicalReads, "Load CSV", []string{"apoc.load.csv('/path/to/file.csv') => rows"})
	// apoc.load.csvStream is the same handler as apoc.load.csv; the two Cypher
	// names share one Go entry point rather than a forwarding alias.
	register("apoc.load.csvStream", "load", load.Csv, registry.CanonicalReads, "Stream CSV", []string{"apoc.load.csvStream('/path/to/file.csv') => stream"})
	register("apoc.load.xml", "load", load.Xml, registry.CanonicalReads, "Load XML", []string{"apoc.load.xml('/path/to/file.xml') => data"})
	// apoc.load.xmlSimple is the same handler as apoc.load.xml.
	register("apoc.load.xmlSimple", "load", load.Xml, registry.CanonicalReads, "Load simple XML", []string{"apoc.load.xmlSimple('/path/to/file.xml') => data"})
	register("apoc.load.html", "load", load.Html, registry.CanonicalReads, "Load HTML", []string{"apoc.load.html(url) => document"})
	register("apoc.load.jdbc", "load", load.Jdbc, registry.CanonicalReads, "Load JDBC", []string{"apoc.load.jdbc(url, 'SELECT...') => rows"})
	register("apoc.load.jdbcUpdate", "load", load.JdbcUpdate, registry.CanonicalReads, "JDBC update", []string{"apoc.load.jdbcUpdate(url, 'UPDATE...') => count"})
	register("apoc.load.driver", "load", load.Driver, registry.CanonicalReads, "Load driver", []string{"apoc.load.driver(name) => driver"})
	register("apoc.load.directory", "load", load.Directory, registry.CanonicalReads, "Load directory", []string{"apoc.load.directory('/path/to/dir') => files"})
	register("apoc.load.directoryTree", "load", load.DirectoryTree, registry.CanonicalReads, "Load dir tree", []string{"apoc.load.directoryTree('/path/to/dir') => tree"})
	register("apoc.load.ldap", "load", load.Ldap, registry.CanonicalReads, "Load LDAP", []string{"apoc.load.ldap(url, 'query') => entries"})
	register("apoc.load.jsonSchema", "load", load.JsonSchema, registry.CanonicalReads, "Load JSON schema", []string{"apoc.load.jsonSchema(url) => schema"})
	register("apoc.load.arrow", "load", load.Arrow, registry.CanonicalReads, "Load Arrow", []string{"apoc.load.arrow('/path/to/file.arrow') => data"})
	register("apoc.load.parquet", "load", load.Parquet, registry.CanonicalReads, "Load Parquet", []string{"apoc.load.parquet('/path/to/file.parquet') => data"})
	register("apoc.load.avro", "load", load.Avro, registry.CanonicalReads, "Load Avro", []string{"apoc.load.avro('/path/to/file.avro') => data"})
	register("apoc.load.s3", "load", load.S3, registry.CanonicalReads, "Load S3", []string{"apoc.load.s3('s3://bucket/key') => data"})
	register("apoc.load.gcs", "load", load.Gcs, registry.CanonicalReads, "Load GCS", []string{"apoc.load.gcs('gs://bucket/key') => data"})
	register("apoc.load.azure", "load", load.Azure, registry.CanonicalReads, "Load Azure", []string{"apoc.load.azure('azure://container/blob') => data"})
	register("apoc.load.kafka", "load", load.Kafka, registry.CanonicalReads, "Load Kafka", []string{"apoc.load.kafka(config) => messages"})
	register("apoc.load.redis", "load", load.Redis, registry.CanonicalReads, "Load Redis", []string{"apoc.load.redis(url, key) => data"})
	register("apoc.load.elasticsearch", "load", load.Elasticsearch, registry.CanonicalReads, "Load ES", []string{"apoc.load.elasticsearch(url, query) => hits"})
	register("apoc.load.graphQL", "load", load.GraphQL, registry.CanonicalReads, "Load GraphQL", []string{"apoc.load.graphQL(url, query) => data"})
	register("apoc.load.rest", "load", load.Rest, registry.CanonicalReads, "Load REST", []string{"apoc.load.rest(url) => response"})
	register("apoc.load.binary", "load", load.Binary, registry.CanonicalReads, "Load binary", []string{"apoc.load.binary('/path/to/file') => bytes"})
	// apoc.load.stream registers the same canonical byte loader: a statement
	// retires its context when Execute returns, so a reader handed out as a
	// value could not be read afterwards. Both names accept the source and an
	// optional config map. apoc/load.Stream keeps the Go reader API.
	register("apoc.load.stream", "load", load.Binary, registry.CanonicalReads, "Load stream", []string{"apoc.load.stream('/path/to/file') => bytes"})

	// ========================================
	// Log Functions (apoc.log.*)
	// ========================================
	register("apoc.log.info", "log", apoclog.Info, registry.CanonicalReads, "Log info", []string{"apoc.log.info('message') => logged"})
	register("apoc.log.debug", "log", apoclog.Debug, registry.CanonicalReads, "Log debug", []string{"apoc.log.debug('message') => logged"})
	register("apoc.log.warn", "log", apoclog.Warn, registry.CanonicalReads, "Log warn", []string{"apoc.log.warn('message') => logged"})
	register("apoc.log.error", "log", apoclog.Error, registry.CanonicalReads, "Log error", []string{"apoc.log.error('message') => logged"})
	register("apoc.log.stream", "log", apoclog.Stream, registry.CanonicalReads, "Log stream", []string{"apoc.log.stream() => logs"})
	register("apoc.log.format", "log", apoclog.Format, registry.CanonicalReads, "Format log", []string{"apoc.log.format(msg, args) => formatted"})
	register("apoc.log.setLevel", "log", apoclog.SetLevel, registry.CanonicalReads, "Set level", []string{"apoc.log.setLevel('INFO') => set"})
	register("apoc.log.getLevel", "log", apoclog.GetLevel, registry.CanonicalReads, "Get level", []string{"apoc.log.getLevel() => 'INFO'"})
	register("apoc.log.metrics", "log", apoclog.Metrics, registry.CanonicalReads, "Log metrics", []string{"apoc.log.metrics() => metrics"})
	register("apoc.log.timer", "log", apoclog.Timer, registry.CanonicalReads, "Log timer", []string{"apoc.log.timer('operation') => timer"})
	register("apoc.log.progress", "log", apoclog.Progress, registry.CanonicalReads, "Log progress", []string{"apoc.log.progress(current, total) => logged"})
	register("apoc.log.trace", "log", apoclog.Trace, registry.CanonicalReads, "Log trace", []string{"apoc.log.trace('message') => logged"})
	register("apoc.log.query", "log", apoclog.Query, registry.CanonicalReads, "Log query", []string{"apoc.log.query(cypher) => logged"})
	register("apoc.log.result", "log", apoclog.Result, registry.CanonicalReads, "Log result", []string{"apoc.log.result(result) => logged"})
	register("apoc.log.memory", "log", apoclog.Memory, registry.CanonicalReads, "Log memory", []string{"apoc.log.memory() => stats"})
	register("apoc.log.stats", "log", apoclog.Stats, registry.CanonicalReads, "Log stats", []string{"apoc.log.stats() => stats"})
	register("apoc.log.audit", "log", apoclog.Audit, registry.CanonicalReads, "Audit log", []string{"apoc.log.audit(event) => logged"})
	register("apoc.log.security", "log", apoclog.Security, registry.CanonicalReads, "Security log", []string{"apoc.log.security(event) => logged"})
	register("apoc.log.performance", "log", apoclog.Performance, registry.CanonicalReads, "Perf log", []string{"apoc.log.performance(metric) => logged"})
	register("apoc.log.custom", "log", apoclog.Custom, registry.CanonicalReads, "Custom log", []string{"apoc.log.custom(level, msg) => logged"})
	register("apoc.log.toFile", "log", apoclog.ToFile, registry.CanonicalReads, "Log to file", []string{"apoc.log.toFile('/path/to/log') => configured"})
	register("apoc.log.rotate", "log", apoclog.Rotate, registry.CanonicalReads, "Rotate logs", []string{"apoc.log.rotate() => rotated"})
	register("apoc.log.clear", "log", apoclog.Clear, registry.CanonicalReads, "Clear logs", []string{"apoc.log.clear() => cleared"})
	register("apoc.log.tail", "log", apoclog.Tail, registry.CanonicalReads, "Tail logs", []string{"apoc.log.tail(100) => lines"})
	register("apoc.log.search", "log", apoclog.Search, registry.CanonicalReads, "Search logs", []string{"apoc.log.search('pattern') => matches"})

	// ========================================
	// Neighbors Functions (apoc.neighbors.*)
	// ========================================
	register("apoc.neighbors.atHop", "neighbors", fns.neighbors.AtHop, registry.OpaqueReads, "Neighbors at hop", []string{"apoc.neighbors.atHop(node, 'TYPE', 2) => nodes"})
	register("apoc.neighbors.toHop", "neighbors", fns.neighbors.ToHop, registry.OpaqueReads, "Neighbors to hop", []string{"apoc.neighbors.toHop(node, 'TYPE', 3) => nodes"})
	register("apoc.neighbors.bfs", "neighbors", fns.neighbors.ToHop, registry.OpaqueReads, "BFS neighbors", []string{"apoc.neighbors.bfs(node, 'TYPE', 5) => nodes"})
	register("apoc.neighbors.dfs", "neighbors", fns.neighbors.DFS, registry.OpaqueReads, "DFS neighbors", []string{"apoc.neighbors.dfs(node, 'TYPE') => nodes"})
	register("apoc.neighbors.count", "neighbors", fns.neighbors.Count, registry.OpaqueReads, "Count neighbors", []string{"apoc.neighbors.count(node) => count"})
	register("apoc.neighbors.exists", "neighbors", fns.neighbors.Exists, registry.OpaqueReads, "Has neighbors", []string{"apoc.neighbors.exists(node, 'TYPE') => bool"})

	// ========================================
	// Node Functions (apoc.node.*)
	// ========================================
	register("apoc.node.degree", "node", fns.node.Degree, registry.OpaqueReads, "Node degree", []string{"apoc.node.degree(node) => degree"})
	register("apoc.node.degreeIn", "node", fns.node.DegreeIn, registry.OpaqueReads, "In-degree", []string{"apoc.node.degreeIn(node) => degree"})
	register("apoc.node.degreeOut", "node", fns.node.DegreeOut, registry.OpaqueReads, "Out-degree", []string{"apoc.node.degreeOut(node) => degree"})
	register("apoc.node.id", "node", apocnode.ID, registry.CanonicalReads, "Node ID", []string{"apoc.node.id(node) => id"})
	register("apoc.node.labels", "node", apocnode.Labels, registry.CanonicalReads, "Node labels", []string{"apoc.node.labels(node) => labels"})
	register("apoc.node.properties", "node", apocnode.Properties, registry.CanonicalReads, "Node props", []string{"apoc.node.properties(node) => props"})
	register("apoc.node.property", "node", apocnode.Property, registry.CanonicalReads, "Node property", []string{"apoc.node.property(node, 'key') => value"})
	register("apoc.node.hasLabel", "node", apocnode.HasLabel, registry.CanonicalReads, "Has label", []string{"apoc.node.hasLabel(node, 'Label') => bool"})
	register("apoc.node.hasLabels", "node", apocnode.HasLabels, registry.CanonicalReads, "Has labels", []string{"apoc.node.hasLabels(node, ['L1','L2']) => bool"})
	register("apoc.node.relationshipTypes", "node", fns.node.RelationshipTypes, registry.OpaqueReads, "Rel types", []string{"apoc.node.relationshipTypes(node) => types"})
	register("apoc.node.relationshipTypesIn", "node", fns.node.RelationshipTypesIn, registry.OpaqueReads, "In rel types", []string{"apoc.node.relationshipTypesIn(node) => types"})
	register("apoc.node.relationshipTypesOut", "node", fns.node.RelationshipTypesOut, registry.OpaqueReads, "Out rel types", []string{"apoc.node.relationshipTypesOut(node) => types"})
	register("apoc.node.relationships", "node", fns.node.Relationships, registry.OpaqueReads, "Node rels", []string{"apoc.node.relationships(node) => rels"})
	register("apoc.node.relationshipsIn", "node", fns.node.RelationshipsIn, registry.OpaqueReads, "In rels", []string{"apoc.node.relationshipsIn(node) => rels"})
	register("apoc.node.relationshipsOut", "node", fns.node.RelationshipsOut, registry.OpaqueReads, "Out rels", []string{"apoc.node.relationshipsOut(node) => rels"})
	register("apoc.node.relationshipExists", "node", fns.node.RelationshipExists, registry.OpaqueReads, "Rel exists", []string{"apoc.node.relationshipExists(node, 'TYPE') => bool"})
	register("apoc.node.connected", "node", fns.node.Connected, registry.OpaqueReads, "Connected", []string{"apoc.node.connected(n1, n2) => bool"})
	register("apoc.node.neighbors", "node", fns.node.Neighbors, registry.OpaqueReads, "Neighbors", []string{"apoc.node.neighbors(node) => nodes"})
	register("apoc.node.neighborsIn", "node", fns.node.NeighborsIn, registry.OpaqueReads, "In neighbors", []string{"apoc.node.neighborsIn(node) => nodes"})
	register("apoc.node.neighborsOut", "node", fns.node.NeighborsOut, registry.OpaqueReads, "Out neighbors", []string{"apoc.node.neighborsOut(node) => nodes"})
	register("apoc.node.isDense", "node", fns.node.IsDense, registry.OpaqueReads, "Is dense", []string{"apoc.node.isDense(node) => bool"})
	register("apoc.node.toMap", "node", apocnode.ToMap, registry.CanonicalReads, "To map", []string{"apoc.node.toMap(node) => map"})
	register("apoc.node.fromMap", "node", apocnode.FromMap, registry.CanonicalReads, "From map", []string{"apoc.node.fromMap(map) => node"})
	register("apoc.node.setProperty", "node", apocnode.SetProperty, registry.CanonicalReads, "Set prop", []string{"apoc.node.setProperty(node, 'key', val) => node"})
	register("apoc.node.setProperties", "node", apocnode.SetProperties, registry.CanonicalReads, "Set props", []string{"apoc.node.setProperties(node, map) => node"})
	register("apoc.node.removeProperty", "node", apocnode.RemoveProperty, registry.CanonicalReads, "Remove prop", []string{"apoc.node.removeProperty(node, 'key') => node"})
	register("apoc.node.removeProperties", "node", apocnode.RemoveProperties, registry.CanonicalReads, "Remove props", []string{"apoc.node.removeProperties(node, ['k1']) => node"})
	register("apoc.node.addLabel", "node", apocnode.AddLabel, registry.CanonicalReads, "Add label", []string{"apoc.node.addLabel(node, 'Label') => node"})
	register("apoc.node.addLabels", "node", apocnode.AddLabels, registry.CanonicalReads, "Add labels", []string{"apoc.node.addLabels(node, ['L1','L2']) => node"})
	register("apoc.node.removeLabel", "node", apocnode.RemoveLabel, registry.CanonicalReads, "Remove label", []string{"apoc.node.removeLabel(node, 'Label') => node"})
	register("apoc.node.removeLabels", "node", apocnode.RemoveLabels, registry.CanonicalReads, "Remove labels", []string{"apoc.node.removeLabels(node, ['L1']) => node"})
	register("apoc.node.clone", "node", apocnode.Clone, registry.CanonicalReads, "Clone node", []string{"apoc.node.clone(node) => cloned"})
	register("apoc.node.diff", "node", apocnode.Diff, registry.CanonicalReads, "Diff nodes", []string{"apoc.node.diff(n1, n2) => diff"})
	register("apoc.node.equals", "node", apocnode.Equals, registry.CanonicalReads, "Equals", []string{"apoc.node.equals(n1, n2) => bool"})

	// ========================================
	// Refactor Functions (apoc.refactor.*)
	// ========================================
	register("apoc.refactor.mergeNodes", "refactor", refactor.MergeNodes, registry.CanonicalReads, "Merge nodes", []string{"apoc.refactor.mergeNodes(nodes, config) => merged"})
	register("apoc.refactor.mergeRelationships", "refactor", refactor.MergeRelationships, registry.CanonicalReads, "Merge rels", []string{"apoc.refactor.mergeRelationships(rels, config) => merged"})
	register("apoc.refactor.cloneNodes", "refactor", fns.refactor.CloneNodes, registry.OpaqueReads, "Clone nodes", []string{"apoc.refactor.cloneNodes(nodes) => clones"})
	register("apoc.refactor.cloneSubgraph", "refactor", fns.refactor.CloneSubgraph, registry.OpaqueReads, "Clone subgraph", []string{"apoc.refactor.cloneSubgraph(nodes, rels) => cloned"})
	register("apoc.refactor.collapseNode", "refactor", fns.refactor.CollapseNode, registry.OpaqueReads, "Collapse node", []string{"apoc.refactor.collapseNode(nodes, 'TYPE') => rel"})
	register("apoc.refactor.extractNode", "refactor", fns.refactor.ExtractNode, registry.OpaqueReads, "Extract node", []string{"apoc.refactor.extractNode(rel, labels) => node"})
	register("apoc.refactor.normalizeAsBoolean", "refactor", refactor.NormalizeAsBoolean, registry.CanonicalReads, "Normalize bool", []string{"apoc.refactor.normalizeAsBoolean(node, prop) => node"})
	register("apoc.refactor.categorizeProperty", "refactor", refactor.CategorizeProperty, registry.CanonicalReads, "Categorize prop", []string{"apoc.refactor.categorizeProperty(node, prop, newLabel) => result"})
	register("apoc.refactor.renameLabel", "refactor", fns.refactor.RenameLabel, registry.OpaqueReads, "Rename label", []string{"apoc.refactor.renameLabel('Old', 'New') => count"})
	register("apoc.refactor.renameType", "refactor", fns.refactor.RenameType, registry.OpaqueReads, "Rename type", []string{"apoc.refactor.renameType('OLD', 'NEW') => count"})
	register("apoc.refactor.renameProperty", "refactor", fns.refactor.RenameProperty, registry.OpaqueReads, "Rename prop", []string{"apoc.refactor.renameProperty('old', 'new') => count"})
	register("apoc.refactor.setType", "refactor", refactor.SetType, registry.CanonicalReads, "Set type", []string{"apoc.refactor.setType(rel, 'NEW_TYPE') => rel"})
	register("apoc.refactor.invertRelationship", "refactor", refactor.InvertRelationship, registry.CanonicalReads, "Invert rel", []string{"apoc.refactor.invertRelationship(rel) => rel"})
	register("apoc.refactor.redirectRelationship", "refactor", refactor.RedirectRelationship, registry.CanonicalReads, "Redirect rel", []string{"apoc.refactor.redirectRelationship(rel, newTarget) => rel"})
	register("apoc.refactor.from", "refactor", refactor.From, registry.CanonicalReads, "Change start", []string{"apoc.refactor.from(rel, newStart) => rel"})
	register("apoc.refactor.deleteAndReconnect", "refactor", fns.refactor.DeleteAndReconnect, registry.OpaqueReads, "Delete reconnect", []string{"apoc.refactor.deleteAndReconnect(node, 'TYPE') => count"})
	register("apoc.refactor.cloneSubgraphFromPaths", "refactor", fns.refactor.CloneSubgraphFromPaths, registry.OpaqueReads, "Clone from paths", []string{"apoc.refactor.cloneSubgraphFromPaths(paths) => cloned"})
	register("apoc.refactor.changeType", "refactor", refactor.ChangeType, registry.CanonicalReads, "Change type", []string{"apoc.refactor.changeType(rel, 'NEW') => rel"})
	register("apoc.refactor.normalize", "refactor", fns.refactor.Normalize, registry.OpaqueReads, "Normalize", []string{"apoc.refactor.normalize(data) => normalized"})
	register("apoc.refactor.denormalize", "refactor", fns.refactor.Denormalize, registry.OpaqueReads, "Denormalize", []string{"apoc.refactor.denormalize(data) => denormalized"})

	// ========================================
	// Rel Functions (apoc.rel.*)
	// ========================================
	register("apoc.rel.id", "rel", apocrel.ID, registry.CanonicalReads, "Rel ID", []string{"apoc.rel.id(rel) => id"})
	register("apoc.rel.type", "rel", apocrel.Type, registry.CanonicalReads, "Rel type", []string{"apoc.rel.type(rel) => type"})
	register("apoc.rel.properties", "rel", apocrel.Properties, registry.CanonicalReads, "Rel props", []string{"apoc.rel.properties(rel) => props"})
	register("apoc.rel.property", "rel", apocrel.Property, registry.CanonicalReads, "Rel property", []string{"apoc.rel.property(rel, 'key') => value"})
	register("apoc.rel.startNode", "rel", fns.rel.StartNode, registry.OpaqueReads, "Start node", []string{"apoc.rel.startNode(rel) => node"})
	register("apoc.rel.endNode", "rel", fns.rel.EndNode, registry.OpaqueReads, "End node", []string{"apoc.rel.endNode(rel) => node"})
	register("apoc.rel.nodes", "rel", fns.rel.Nodes, registry.OpaqueReads, "Rel nodes", []string{"apoc.rel.nodes(rel) => [start, end]"})
	register("apoc.rel.setProperty", "rel", apocrel.SetProperty, registry.CanonicalReads, "Set prop", []string{"apoc.rel.setProperty(rel, 'key', val) => rel"})
	register("apoc.rel.setProperties", "rel", apocrel.SetProperties, registry.CanonicalReads, "Set props", []string{"apoc.rel.setProperties(rel, map) => rel"})
	register("apoc.rel.removeProperty", "rel", apocrel.RemoveProperty, registry.CanonicalReads, "Remove prop", []string{"apoc.rel.removeProperty(rel, 'key') => rel"})
	register("apoc.rel.removeProperties", "rel", apocrel.RemoveProperties, registry.CanonicalReads, "Remove props", []string{"apoc.rel.removeProperties(rel, ['k1']) => rel"})
	register("apoc.rel.toMap", "rel", apocrel.ToMap, registry.CanonicalReads, "To map", []string{"apoc.rel.toMap(rel) => map"})
	register("apoc.rel.fromMap", "rel", apocrel.FromMap, registry.CanonicalReads, "From map", []string{"apoc.rel.fromMap(map) => rel"})
	register("apoc.rel.exists", "rel", fns.rel.Exists, registry.OpaqueReads, "Rel exists", []string{"apoc.rel.exists(id) => bool"})
	register("apoc.rel.delete", "rel", fns.rel.Delete, registry.OpaqueReads, "Delete rel", []string{"apoc.rel.delete(rel) => deleted"})
	register("apoc.rel.clone", "rel", fns.rel.Clone, registry.OpaqueReads, "Clone rel", []string{"apoc.rel.clone(rel) => cloned"})
	register("apoc.rel.reverse", "rel", apocrel.Reverse, registry.CanonicalReads, "Reverse rel", []string{"apoc.rel.reverse(rel) => reversed"})
	register("apoc.rel.isType", "rel", apocrel.IsType, registry.CanonicalReads, "Is type", []string{"apoc.rel.isType(rel, 'TYPE') => bool"})
	register("apoc.rel.isAnyType", "rel", apocrel.IsAnyType, registry.CanonicalReads, "Is any type", []string{"apoc.rel.isAnyType(rel, ['T1','T2']) => bool"})
	register("apoc.rel.hasProperty", "rel", apocrel.HasProperty, registry.CanonicalReads, "Has prop", []string{"apoc.rel.hasProperty(rel, 'key') => bool"})
	register("apoc.rel.hasProperties", "rel", apocrel.HasProperties, registry.CanonicalReads, "Has props", []string{"apoc.rel.hasProperties(rel, ['k1']) => bool"})
	register("apoc.rel.equals", "rel", apocrel.Equals, registry.CanonicalReads, "Equals", []string{"apoc.rel.equals(r1, r2) => bool"})
	register("apoc.rel.compare", "rel", apocrel.Compare, registry.CanonicalReads, "Compare", []string{"apoc.rel.compare(r1, r2) => result"})
	register("apoc.rel.weight", "rel", apocrel.Weight, registry.CanonicalReads, "Get weight", []string{"apoc.rel.weight(rel, 'prop') => weight"})
	register("apoc.rel.direction", "rel", apocrel.Direction, registry.CanonicalReads, "Get direction", []string{"apoc.rel.direction(rel, node) => 'OUT'"})
	register("apoc.rel.otherNode", "rel", fns.rel.OtherNode, registry.OpaqueReads, "Other node", []string{"apoc.rel.otherNode(rel, node) => otherNode"})
	register("apoc.rel.isLoop", "rel", apocrel.IsLoop, registry.CanonicalReads, "Is loop", []string{"apoc.rel.isLoop(rel) => bool"})
	register("apoc.rel.isBetween", "rel", apocrel.IsBetween, registry.CanonicalReads, "Is between", []string{"apoc.rel.isBetween(rel, n1, n2) => bool"})
	register("apoc.rel.isDirectedBetween", "rel", apocrel.IsDirectedBetween, registry.CanonicalReads, "Directed between", []string{"apoc.rel.isDirectedBetween(rel, from, to) => bool"})

	// ========================================
	// Scoring Functions (apoc.scoring.*)
	// ========================================
	register("apoc.scoring.existence", "scoring", scoring.Existence, registry.CanonicalReads, "Existence score", []string{"apoc.scoring.existence(actual, expected) => score"})
	register("apoc.scoring.pareto", "scoring", scoring.Pareto, registry.CanonicalReads, "Pareto score", []string{"apoc.scoring.pareto(score, min, max) => normalized"})
	register("apoc.scoring.cosine", "scoring", scoring.Cosine, registry.CanonicalReads, "Cosine similarity", []string{"apoc.scoring.cosine(v1, v2) => score"})
	register("apoc.scoring.euclidean", "scoring", scoring.Euclidean, registry.CanonicalReads, "Euclidean dist", []string{"apoc.scoring.euclidean(v1, v2) => dist"})
	register("apoc.scoring.manhattan", "scoring", scoring.Manhattan, registry.CanonicalReads, "Manhattan dist", []string{"apoc.scoring.manhattan(v1, v2) => dist"})
	register("apoc.scoring.jaccard", "scoring", scoring.Jaccard, registry.CanonicalReads, "Jaccard score", []string{"apoc.scoring.jaccard(set1, set2) => score"})
	register("apoc.scoring.overlap", "scoring", scoring.Overlap, registry.CanonicalReads, "Overlap coef", []string{"apoc.scoring.overlap(set1, set2) => score"})
	register("apoc.scoring.dice", "scoring", scoring.Dice, registry.CanonicalReads, "Dice coef", []string{"apoc.scoring.dice(set1, set2) => score"})
	register("apoc.scoring.pearson", "scoring", scoring.Pearson, registry.CanonicalReads, "Pearson corr", []string{"apoc.scoring.pearson(v1, v2) => score"})
	register("apoc.scoring.tf", "scoring", scoring.TF, registry.CanonicalReads, "Term frequency", []string{"apoc.scoring.tf(term, doc) => score"})
	register("apoc.scoring.idf", "scoring", scoring.IDF, registry.CanonicalReads, "Inv doc freq", []string{"apoc.scoring.idf(term, docs) => score"})
	register("apoc.scoring.tfidf", "scoring", scoring.TFIDF, registry.CanonicalReads, "TF-IDF", []string{"apoc.scoring.tfidf(term, doc, docs) => score"})
	register("apoc.scoring.bm25", "scoring", scoring.BM25, registry.CanonicalReads, "BM25 score", []string{"apoc.scoring.bm25(term, doc, docs) => score"})
	register("apoc.scoring.pageRank", "scoring", scoring.PageRank, registry.CanonicalReads, "PageRank", []string{"apoc.scoring.pageRank(nodes, rels) => scores"})
	register("apoc.scoring.normalize", "scoring", scoring.Normalize, registry.CanonicalReads, "Normalize scores", []string{"apoc.scoring.normalize(scores) => normalized"})
	register("apoc.scoring.rank", "scoring", scoring.Rank, registry.CanonicalReads, "Rank items", []string{"apoc.scoring.rank(items, scores) => ranked"})
	register("apoc.scoring.topK", "scoring", scoring.TopK, registry.CanonicalReads, "Top K", []string{"apoc.scoring.topK(items, scores, k) => top"})
	register("apoc.scoring.percentile", "scoring", scoring.Percentile, registry.CanonicalReads, "Percentile", []string{"apoc.scoring.percentile(scores, p) => value"})
	register("apoc.scoring.zScore", "scoring", scoring.ZScore, registry.CanonicalReads, "Z-score", []string{"apoc.scoring.zScore(value, values) => zscore"})
	register("apoc.scoring.minMax", "scoring", scoring.MinMax, registry.CanonicalReads, "Min-max norm", []string{"apoc.scoring.minMax(value, min, max) => normalized"})
	register("apoc.scoring.sigmoid", "scoring", scoring.Sigmoid, registry.CanonicalReads, "Sigmoid", []string{"apoc.scoring.sigmoid(value) => score"})
	register("apoc.scoring.softmax", "scoring", scoring.Softmax, registry.CanonicalReads, "Softmax", []string{"apoc.scoring.softmax(values) => probs"})

	// ========================================
	// Spatial Functions (apoc.spatial.*)
	// ========================================
	register("apoc.spatial.distance", "spatial", spatial.Distance, registry.CanonicalReads, "Distance", []string{"apoc.spatial.distance(p1, p2) => meters"})
	register("apoc.spatial.haversineDistance", "spatial", spatial.HaversineDistance, registry.CanonicalReads, "Haversine dist", []string{"apoc.spatial.haversineDistance(lat1, lon1, lat2, lon2) => meters"})
	register("apoc.spatial.vincentyDistance", "spatial", spatial.VincentyDistance, registry.CanonicalReads, "Vincenty dist", []string{"apoc.spatial.vincentyDistance(lat1, lon1, lat2, lon2) => meters"})
	register("apoc.spatial.bearing", "spatial", spatial.Bearing, registry.CanonicalReads, "Bearing", []string{"apoc.spatial.bearing(lat1, lon1, lat2, lon2) => degrees"})
	register("apoc.spatial.destination", "spatial", spatial.Destination, registry.CanonicalReads, "Destination", []string{"apoc.spatial.destination(lat, lon, bearing, dist) => {lat, lon}"})
	register("apoc.spatial.midpoint", "spatial", spatial.Midpoint, registry.CanonicalReads, "Midpoint", []string{"apoc.spatial.midpoint(lat1, lon1, lat2, lon2) => {lat, lon}"})
	register("apoc.spatial.boundingBox", "spatial", spatial.BoundingBox, registry.CanonicalReads, "Bounding box", []string{"apoc.spatial.boundingBox(lat, lon, dist) => box"})
	register("apoc.spatial.within", "spatial", spatial.Within, registry.CanonicalReads, "Within", []string{"apoc.spatial.within(point, polygon) => bool"})
	register("apoc.spatial.area", "spatial", spatial.Area, registry.CanonicalReads, "Area", []string{"apoc.spatial.area(polygon) => sqMeters"})
	register("apoc.spatial.centroid", "spatial", spatial.Centroid, registry.CanonicalReads, "Centroid", []string{"apoc.spatial.centroid(polygon) => {lat, lon}"})
	register("apoc.spatial.nearest", "spatial", spatial.Nearest, registry.CanonicalReads, "Nearest", []string{"apoc.spatial.nearest(point, points) => nearest"})
	register("apoc.spatial.kNearest", "spatial", spatial.KNearest, registry.CanonicalReads, "K nearest", []string{"apoc.spatial.kNearest(point, points, k) => nearest"})
	register("apoc.spatial.withinDistance", "spatial", spatial.WithinDistance, registry.CanonicalReads, "Within dist", []string{"apoc.spatial.withinDistance(point, points, dist) => nearby"})
	register("apoc.spatial.intersects", "spatial", spatial.Intersects, registry.CanonicalReads, "Intersects", []string{"apoc.spatial.intersects(geom1, geom2) => bool"})
	register("apoc.spatial.contains", "spatial", spatial.Contains, registry.CanonicalReads, "Contains", []string{"apoc.spatial.contains(geom1, geom2) => bool"})
	register("apoc.spatial.toGeoJSON", "spatial", spatial.ToGeoJSON, registry.CanonicalReads, "To GeoJSON", []string{"apoc.spatial.toGeoJSON(geom) => json"})
	register("apoc.spatial.fromGeoJSON", "spatial", spatial.FromGeoJSON, registry.CanonicalReads, "From GeoJSON", []string{"apoc.spatial.fromGeoJSON(json) => geom"})
	register("apoc.spatial.decodeGeohash", "spatial", spatial.DecodeGeohash, registry.CanonicalReads, "Decode geohash", []string{"apoc.spatial.decodeGeohash('u4pruydqqvj') => {lat, lon}"})
	register("apoc.spatial.encodeGeohash", "spatial", spatial.EncodeGeohash, registry.CanonicalReads, "Encode geohash", []string{"apoc.spatial.encodeGeohash(lat, lon) => hash"})

	// ========================================
	// Stats Functions (apoc.stats.*)
	// ========================================
	register("apoc.stats.degrees", "stats", fns.stats.Degrees, registry.OpaqueReads, "Degree stats", []string{"apoc.stats.degrees(nodes) => stats"})
	register("apoc.stats.mean", "stats", stats.Mean, registry.CanonicalReads, "Mean", []string{"apoc.stats.mean(values) => mean"})
	register("apoc.stats.median", "stats", stats.Median, registry.CanonicalReads, "Median", []string{"apoc.stats.median(values) => median"})
	register("apoc.stats.mode", "stats", stats.Mode, registry.CanonicalReads, "Mode", []string{"apoc.stats.mode(values) => mode"})
	register("apoc.stats.stdDev", "stats", stats.StdDev, registry.CanonicalReads, "Std deviation", []string{"apoc.stats.stdDev(values) => stdev"})
	register("apoc.stats.variance", "stats", stats.Variance, registry.CanonicalReads, "Variance", []string{"apoc.stats.variance(values) => variance"})
	register("apoc.stats.percentile", "stats", stats.Percentile, registry.CanonicalReads, "Percentile", []string{"apoc.stats.percentile(values, p) => value"})
	register("apoc.stats.quartiles", "stats", stats.Quartiles, registry.CanonicalReads, "Quartiles", []string{"apoc.stats.quartiles(values) => [q1,q2,q3]"})
	register("apoc.stats.iqr", "stats", stats.IQR, registry.CanonicalReads, "IQR", []string{"apoc.stats.iqr(values) => iqr"})
	register("apoc.stats.min", "stats", stats.Min, registry.CanonicalReads, "Min", []string{"apoc.stats.min(values) => min"})
	register("apoc.stats.max", "stats", stats.Max, registry.CanonicalReads, "Max", []string{"apoc.stats.max(values) => max"})
	register("apoc.stats.range", "stats", stats.Range, registry.CanonicalReads, "Range", []string{"apoc.stats.range(values) => range"})
	register("apoc.stats.sum", "stats", stats.Sum, registry.CanonicalReads, "Sum", []string{"apoc.stats.sum(values) => sum"})
	register("apoc.stats.count", "stats", stats.Count, registry.CanonicalReads, "Count", []string{"apoc.stats.count(values) => count"})
	register("apoc.stats.skewness", "stats", stats.Skewness, registry.CanonicalReads, "Skewness", []string{"apoc.stats.skewness(values) => skewness"})
	register("apoc.stats.kurtosis", "stats", stats.Kurtosis, registry.CanonicalReads, "Kurtosis", []string{"apoc.stats.kurtosis(values) => kurtosis"})
	register("apoc.stats.correlation", "stats", stats.Correlation, registry.CanonicalReads, "Correlation", []string{"apoc.stats.correlation(v1, v2) => corr"})
	register("apoc.stats.covariance", "stats", stats.Covariance, registry.CanonicalReads, "Covariance", []string{"apoc.stats.covariance(v1, v2) => cov"})
	register("apoc.stats.zScore", "stats", stats.ZScore, registry.CanonicalReads, "Z-score", []string{"apoc.stats.zScore(value, values) => zscore"})
	register("apoc.stats.normalize", "stats", stats.Normalize, registry.CanonicalReads, "Normalize", []string{"apoc.stats.normalize(values) => normalized"})
	register("apoc.stats.histogram", "stats", stats.Histogram, registry.CanonicalReads, "Histogram", []string{"apoc.stats.histogram(values, bins) => hist"})
	register("apoc.stats.outliers", "stats", stats.Outliers, registry.CanonicalReads, "Outliers", []string{"apoc.stats.outliers(values) => outliers"})
	register("apoc.stats.summary", "stats", stats.Summary, registry.CanonicalReads, "Summary", []string{"apoc.stats.summary(values) => {mean, median, ...}"})

	// ========================================
	// Warmup Functions (apoc.warmup.*)
	// ========================================
	register("apoc.warmup.run", "warmup", fns.warmup.Run, registry.OpaqueReads, "Run warmup", []string{"apoc.warmup.run() => stats"})
	register("apoc.warmup.runWithParams", "warmup", fns.warmup.RunWithParams, registry.OpaqueReads, "Run with params", []string{"apoc.warmup.runWithParams(config) => stats"})
	register("apoc.warmup.nodes", "warmup", fns.warmup.Nodes, registry.OpaqueReads, "Warmup nodes", []string{"apoc.warmup.nodes() => count"})
	register("apoc.warmup.relationships", "warmup", fns.warmup.Relationships, registry.OpaqueReads, "Warmup rels", []string{"apoc.warmup.relationships() => count"})
	register("apoc.warmup.indexes", "warmup", fns.warmup.Indexes, registry.OpaqueReads, "Warmup indexes", []string{"apoc.warmup.indexes() => count"})
	register("apoc.warmup.properties", "warmup", fns.warmup.Properties, registry.OpaqueReads, "Warmup props", []string{"apoc.warmup.properties() => count"})
	register("apoc.warmup.subgraph", "warmup", fns.warmup.Subgraph, registry.OpaqueReads, "Warmup subgraph", []string{"apoc.warmup.subgraph(config) => stats"})
	register("apoc.warmup.path", "warmup", fns.warmup.Path, registry.CanonicalReads, "Warmup path", []string{"apoc.warmup.path(pattern) => stats"})
	register("apoc.warmup.cache", "warmup", fns.warmup.Cache, registry.CanonicalReads, "Cache stats", []string{"apoc.warmup.cache() => stats"})
	register("apoc.warmup.stats", "warmup", fns.warmup.Stats, registry.CanonicalReads, "Warmup stats", []string{"apoc.warmup.stats() => stats"})
	register("apoc.warmup.clear", "warmup", fns.warmup.Clear, registry.CanonicalReads, "Clear cache", []string{"apoc.warmup.clear() => cleared"})
	register("apoc.warmup.optimize", "warmup", fns.warmup.Optimize, registry.OpaqueReads, "Optimize cache", []string{"apoc.warmup.optimize() => optimized"})
	register("apoc.warmup.schedule", "warmup", fns.warmup.Schedule, registry.CanonicalReads, "Schedule warmup", []string{"apoc.warmup.schedule(cron) => scheduled"})
	register("apoc.warmup.status", "warmup", fns.warmup.Status, registry.CanonicalReads, "Warmup status", []string{"apoc.warmup.status() => status"})
	register("apoc.warmup.progress", "warmup", fns.warmup.Progress, registry.OpaqueReads, "Warmup progress", []string{"apoc.warmup.progress() => progress"})

	// ========================================
	// XML Functions (apoc.xml.*)
	// ========================================
	register("apoc.xml.parse", "xml", xml.Parse, registry.CanonicalReads, "Parse XML", []string{"apoc.xml.parse('<root/>') => doc"})
	register("apoc.xml.toString", "xml", xml.ToString, registry.CanonicalReads, "XML to string", []string{"apoc.xml.toString(doc) => xml"})
	register("apoc.xml.toMap", "xml", xml.ToMap, registry.CanonicalReads, "XML to map", []string{"apoc.xml.toMap(doc) => map"})
	register("apoc.xml.fromMap", "xml", xml.FromMap, registry.CanonicalReads, "Map to XML", []string{"apoc.xml.fromMap(map) => doc"})
	register("apoc.xml.query", "xml", xml.Query, registry.CanonicalReads, "XPath query", []string{"apoc.xml.query(doc, '//node') => nodes"})
	register("apoc.xml.getAttribute", "xml", xml.GetAttribute, registry.CanonicalReads, "Get attribute", []string{"apoc.xml.getAttribute(node, 'attr') => value"})
	register("apoc.xml.setAttribute", "xml", xml.SetAttribute, registry.CanonicalReads, "Set attribute", []string{"apoc.xml.setAttribute(node, 'attr', val) => node"})
	register("apoc.xml.getText", "xml", xml.GetText, registry.CanonicalReads, "Get text", []string{"apoc.xml.getText(node) => text"})
	register("apoc.xml.setText", "xml", xml.SetText, registry.CanonicalReads, "Set text", []string{"apoc.xml.setText(node, 'text') => node"})
	register("apoc.xml.addChild", "xml", xml.AddChild, registry.CanonicalReads, "Add child", []string{"apoc.xml.addChild(parent, child) => parent"})
	register("apoc.xml.removeChild", "xml", xml.RemoveChild, registry.CanonicalReads, "Remove child", []string{"apoc.xml.removeChild(parent, child) => parent"})
	register("apoc.xml.create", "xml", xml.Create, registry.CanonicalReads, "Create element", []string{"apoc.xml.create('name') => element"})
	register("apoc.xml.clone", "xml", xml.Clone, registry.CanonicalReads, "Clone element", []string{"apoc.xml.clone(element) => cloned"})
	register("apoc.xml.validate", "xml", xml.Validate, registry.CanonicalReads, "Validate XML", []string{"apoc.xml.validate(doc, schema) => valid"})
	register("apoc.xml.transform", "xml", xml.Transform, registry.CanonicalReads, "XSLT transform", []string{"apoc.xml.transform(doc, xslt) => result"})
	register("apoc.xml.prettify", "xml", xml.Prettify, registry.CanonicalReads, "Prettify XML", []string{"apoc.xml.prettify(xml) => pretty"})
	register("apoc.xml.minify", "xml", xml.Minify, registry.CanonicalReads, "Minify XML", []string{"apoc.xml.minify(xml) => minified"})
	register("apoc.xml.toJson", "xml", xml.ToJson, registry.CanonicalReads, "XML to JSON", []string{"apoc.xml.toJson(doc) => json"})
	register("apoc.xml.fromJson", "xml", xml.FromJson, registry.CanonicalReads, "JSON to XML", []string{"apoc.xml.fromJson(json) => doc"})
	register("apoc.xml.escape", "xml", xml.Escape, registry.CanonicalReads, "Escape XML", []string{"apoc.xml.escape('<>') => '&lt;&gt;'"})
	register("apoc.xml.unescape", "xml", xml.Unescape, registry.CanonicalReads, "Unescape XML", []string{"apoc.xml.unescape('&lt;') => '<'"})
	register("apoc.xml.namespace", "xml", xml.Namespace, registry.CanonicalReads, "Get namespace", []string{"apoc.xml.namespace(node) => ns"})
	register("apoc.xml.getNamespace", "xml", xml.GetNamespace, registry.CanonicalReads, "Get namespace", []string{"apoc.xml.getNamespace(node) => ns"})

	return registrationError
}
