// NOR-175: exported apoc.Call generic dispatch over registered callable
// signatures.
//
// The exported apoc.Call(name, args) is the process's external Go boundary into
// the published APOC registry: docs/features/apoc-functions.md documents it as
// the Go entry for registered names, and no production package in this
// repository calls it. External consumers are unknown. It answers
// through the same *registry.Callable instance the typed callable routes hold
// (apoc.GetInfo(name).Handler, compiled once by buildRegistry), with the root
// catalog's APOCArguments policy.
//
// The original report: the exported Call rejects the registered
// apoc.import.cypherData and apoc.cypher.runMany callables with the registry's
// "unsupported function signature" rejection instead of running them. The
// affected population is the registered callable signatures, not two spellings:
// eight registered import names share apoc.import.cypherData's declared
// signature and two registered cypher names share apoc.cypher.runMany's, and
// the tests below extract that population from the published registry instead
// of restating it from the report.
//
// These are repro/regression tests only: every assertion goes through the real
// registered *registry.Callable of a deterministic in-memory APOC backend. The
// handlers here produce their real outcomes from their arguments alone (no
// external network, no file reads beyond t.TempDir(), no arbitrary model or
// query execution), so "same outcome as the typed route" is the same actual
// handler's outcome and not an invented stub contract.
package apoc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"sync"
	"testing"

	apoccypher "github.com/orneryd/nornicdb/apoc/cypher"
	apocimport "github.com/orneryd/nornicdb/apoc/import"
	"github.com/orneryd/nornicdb/apoc/registry"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

// apocCallDispatchAffectedSignatures are the two registered callable signatures
// the report names, taken from the registered implementations themselves. A
// signature is the unit of the affected population: every registered name that
// declares one of these signatures is reachable only through the same dispatch
// decision.
var apocCallDispatchAffectedSignatures = map[string]reflect.Type{
	"importResult": reflect.TypeOf(apocimport.CypherData),
	"runMany":      reflect.TypeOf(apoccypher.RunMany),
}

// apocCallDispatchExpectedPopulation is the inventory of registered names whose
// declared signature is one of the affected signatures. It is pinned so a new
// registration that shares the signature fails this test until it is driven
// through both routes as well; the population is still extracted from the
// published registry below, never taken from this literal alone.
var apocCallDispatchExpectedPopulation = map[string][]string{
	"importResult": {
		"apoc.import.csv",
		"apoc.import.csvData",
		"apoc.import.cypher",
		"apoc.import.cypherData",
		"apoc.import.file",
		"apoc.import.graphML",
		"apoc.import.graphMLData",
		"apoc.import.url",
	},
	"runMany": {
		"apoc.cypher.runFile",
		"apoc.cypher.runMany",
	},
}

// apocCallDispatchEntry is one registered callable of the affected population.
type apocCallDispatchEntry struct {
	name    string
	handler *registry.Callable
}

// apocCallDispatchPopulation extracts every enabled registered callable whose
// declared signature equals one of the affected signatures. It reads the same
// published table apoc.Call reads (List + GetInfo), so the inventory covers the
// whole external surface of the affected signatures rather than two names.
func apocCallDispatchPopulation(t *testing.T) map[reflect.Type][]apocCallDispatchEntry {
	t.Helper()

	population := make(map[reflect.Type][]apocCallDispatchEntry, len(apocCallDispatchAffectedSignatures))
	for _, signature := range apocCallDispatchAffectedSignatures {
		population[signature] = nil
	}
	for _, name := range List() {
		info, found := GetInfo(name)
		require.True(t, found, "%s is listed but must also answer GetInfo", name)
		require.NotNil(t, info.Handler, "%s must publish an admitted callable", name)
		signature := info.Handler.Signature()
		if _, affected := population[signature]; !affected {
			continue
		}
		population[signature] = append(population[signature], apocCallDispatchEntry{name: name, handler: info.Handler})
	}
	for signature, entries := range population {
		sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
		population[signature] = entries
	}
	return population
}

// apocCallDispatchNames flattens and sorts a population for stable reporting.
func apocCallDispatchNames(population map[reflect.Type][]apocCallDispatchEntry) []string {
	names := make([]string, 0, 16)
	for _, entries := range population {
		for _, entry := range entries {
			names = append(names, entry.name)
		}
	}
	sort.Strings(names)
	return names
}

// apocCallDispatchValidArguments binds one registered name's declared
// (string, map[string]interface{}) parameters with the values that member's
// real implementation consumes. Every value stays inside the handler's own
// argument domain: no file is created outside t.TempDir(), no URL is fetched,
// and no statement is executed anywhere.
func apocCallDispatchValidArguments(t *testing.T, name string) []interface{} {
	t.Helper()

	switch name {
	case "apoc.import.csv":
		return []interface{}{t.TempDir() + "/nor175-data.csv", map[string]interface{}{"delimiter": ","}}
	case "apoc.import.csvData":
		return []interface{}{"id,name\n1,Alice\n", map[string]interface{}{}}
	case "apoc.import.cypher":
		return []interface{}{t.TempDir() + "/nor175-statements.cypher", map[string]interface{}{}}
	case "apoc.import.cypherData":
		return []interface{}{"CREATE (:A)", map[string]interface{}{}}
	case "apoc.import.file":
		return []interface{}{t.TempDir() + "/nor175-empty.json", map[string]interface{}{}}
	case "apoc.import.graphML":
		return []interface{}{t.TempDir() + "/nor175-graph.graphml", map[string]interface{}{}}
	case "apoc.import.graphMLData":
		return []interface{}{"<graphml></graphml>", map[string]interface{}{}}
	case "apoc.import.url":
		return []interface{}{"https://example.invalid/nor175.json", map[string]interface{}{}}
	case "apoc.cypher.runFile":
		return []interface{}{t.TempDir() + "/nor175-queries.cypher", map[string]interface{}{}}
	case "apoc.cypher.runMany":
		return []interface{}{"CREATE (:A); CREATE (:B)", map[string]interface{}{}}
	}
	require.FailNow(t, "the affected population gained a registered name without valid arguments", name)
	return nil
}

// apocCallDispatchTypedOutcome invokes one already admitted callable the way the
// typed callable route inside the query executor invokes a context-free handler:
// the same *registry.Callable through CallWithPolicy with the query argument
// policy and no bound prefix. It is the reference outcome for the generic
// boundary, not a second implementation.
func apocCallDispatchTypedOutcome(handler *registry.Callable, args []interface{}) (interface{}, error) {
	return handler.CallWithPolicy(args, nil, registry.CypherArguments)
}

// apocCallDispatchInstallStorage publishes a registry bound to backend and
// restores the registry the test found afterwards, so every helper the registry
// admits for the duration of the test answers from backend.
func apocCallDispatchInstallStorage(t *testing.T, backend apocstorage.Storage) {
	t.Helper()
	previous := GetStorage()
	require.NotNil(t, previous, "the default registry must be published first")
	require.NoError(t, SetStorage(backend))
	t.Cleanup(func() { require.NoError(t, SetStorage(previous)) })
}

// TestAPOCCallDispatchesRegisteredCypherData reproduces the original call: the
// exported boundary is given the registered apoc.import.cypherData handler's
// declared arguments, and the real handler's outcome must come back.
func TestAPOCCallDispatchesRegisteredCypherData(t *testing.T) {
	args := []interface{}{"CREATE (:A)", map[string]interface{}{}}

	value, err := Call("apoc.import.cypherData", args)
	require.False(t, errors.Is(err, registry.ErrUnsupportedSignature),
		"the exported Call rejected the registered handler's signature: %v", err)
	require.NoError(t, err, "apoc.Call must run the registered apoc.import.cypherData handler")
	require.True(t, registry.InvocationStarted(err), "the handler must have been entered")

	result, ok := value.(*apocimport.ImportResult)
	require.True(t, ok, "apoc.import.cypherData returns its real *import.ImportResult, got %T", value)
	require.Equal(t, 1, result.Nodes, "the handler's own statement count is its outcome")
	require.Zero(t, result.Relationships)

	// The generic boundary must answer from the same admitted callable the typed
	// route holds, with the same outcome.
	handler := apocRegisteredHandler(t, "apoc.import.cypherData")
	typed, typedErr := apocCallDispatchTypedOutcome(handler, args)
	require.NoError(t, typedErr)
	require.Equal(t, typed, value, "both routes answer from the same registered callable")
}

// TestAPOCCallDispatchesRegisteredRunMany reproduces the second reported call
// and the real handler result count: one registered handler call answers exactly one
// result per statement the handler frames itself.
func TestAPOCCallDispatchesRegisteredRunMany(t *testing.T) {
	args := []interface{}{"CREATE (:A); CREATE (:B)", map[string]interface{}{}}

	value, err := Call("apoc.cypher.runMany", args)
	require.False(t, errors.Is(err, registry.ErrUnsupportedSignature),
		"the exported Call rejected the registered handler's signature: %v", err)
	require.NoError(t, err, "apoc.Call must run the registered apoc.cypher.runMany handler")

	results, ok := value.([]*apoccypher.Result)
	require.True(t, ok, "apoc.cypher.runMany returns its real []*cypher.Result, got %T", value)
	require.Len(t, results, 2, "one handler execution answers one result per statement")

	handler := apocRegisteredHandler(t, "apoc.cypher.runMany")
	typed, typedErr := apocCallDispatchTypedOutcome(handler, args)
	require.NoError(t, typedErr)
	require.Equal(t, typed, value, "both routes answer from the same registered callable")
	typedResults, ok := typed.([]*apoccypher.Result)
	require.True(t, ok)
	require.Len(t, typedResults, 2, "the reference route makes the same real execution count")
}

// TestAPOCCallDispatchCoversEveryAffectedRegisteredSignature is the inventory:
// every registered name whose declared signature is one of the affected
// signatures is driven through the exported boundary and through the typed
// callable route, and both must return the same actual handler outcome.
func TestAPOCCallDispatchCoversEveryAffectedRegisteredSignature(t *testing.T) {
	population := apocCallDispatchPopulation(t)

	var expected []string
	for _, names := range apocCallDispatchExpectedPopulation {
		expected = append(expected, names...)
	}
	sort.Strings(expected)
	require.Equal(t, expected, apocCallDispatchNames(population),
		"the affected signature population changed: every member must be driven by this test")

	for label, signature := range apocCallDispatchAffectedSignatures {
		entries := population[signature]
		require.NotEmpty(t, entries, "%s has no registered name", label)
		for _, entry := range entries {
			entry := entry
			t.Run(entry.name, func(t *testing.T) {
				require.Equal(t, signature, entry.handler.Signature(),
					"the registered callable's declared signature is the affected one")

				args := apocCallDispatchValidArguments(t, entry.name)

				typed, typedErr := apocCallDispatchTypedOutcome(entry.handler, args)
				require.NoError(t, typedErr, "the typed callable route runs the real handler")
				require.NotNil(t, typed, "the real handler answered")

				value, err := Call(entry.name, args)
				require.False(t, errors.Is(err, registry.ErrUnsupportedSignature),
					"the exported Call rejected the registered handler's signature: %v", err)
				require.NoError(t, err, "apoc.Call must run the registered handler")
				require.Equal(t, typed, value,
					"the exported boundary and the typed callable route answer from the same registered handler")

				// The generic boundary answers from the callable this name was
				// admitted as; it never rebuilds a second handler for the name.
				info, found := GetInfo(entry.name)
				require.True(t, found)
				require.Same(t, entry.handler, info.Handler,
					"the exported boundary and the typed route hold the same admitted callable")
			})
		}
	}
}

// TestAPOCCallAffectedDispatchKeepsCallableCompatibility pins the compatibility
// half of the change: the affected registrations keep their exact declared Go
// signatures, each name keeps answering from the one callable instance it was
// admitted as, and the root catalog's existing argument policy for a supported
// scalar is unchanged.
func TestAPOCCallAffectedDispatchKeepsCallableCompatibility(t *testing.T) {
	declared := map[string]reflect.Type{
		"apoc.import.csv":         reflect.TypeOf(apocimport.Csv),
		"apoc.import.csvData":     reflect.TypeOf(apocimport.CsvData),
		"apoc.import.cypher":      reflect.TypeOf(apocimport.Cypher),
		"apoc.import.cypherData":  reflect.TypeOf(apocimport.CypherData),
		"apoc.import.file":        reflect.TypeOf(apocimport.File),
		"apoc.import.graphML":     reflect.TypeOf(apocimport.GraphML),
		"apoc.import.graphMLData": reflect.TypeOf(apocimport.GraphMLData),
		"apoc.import.url":         reflect.TypeOf(apocimport.Url),
		"apoc.cypher.runFile":     reflect.TypeOf(apoccypher.RunFile),
		"apoc.cypher.runMany":     reflect.TypeOf(apoccypher.RunMany),
	}

	population := apocCallDispatchPopulation(t)
	seen := map[string]bool{}
	for _, entries := range population {
		for _, entry := range entries {
			seen[entry.name] = true
			signature, declaredHere := declared[entry.name]
			require.True(t, declaredHere, "%s must declare the signature the affected population pinned", entry.name)
			require.Equal(t, signature, entry.handler.Signature(),
				"%s keeps its registered callable signature", entry.name)

			info, found := GetInfo(entry.name)
			require.True(t, found)
			require.Same(t, entry.handler, info.Handler,
				"%s keeps answering from the callable it was admitted as", entry.name)
			admittedAgain, err := registry.Compile(entry.handler, entry.handler.ReadObservation())
			require.NoError(t, err)
			require.Same(t, entry.handler, admittedAgain,
				"an admitted callable is compiled once and keeps its identity")
		}
	}
	require.Equal(t, len(declared), len(seen), "every pinned declared signature is part of the affected population")

	// Existing supported scalar binding is untouched by the generic dispatch.
	_, err := Call("apoc.math.gcd", []interface{}{"12", "8"})
	require.ErrorContains(t, err, "cannot be converted",
		"a declared scalar still binds its registered argument types")
}

// TestAPOCCallAffectedDispatchRejectsBadCallsWithoutPanic pins the error half:
// arity, argument types, unknown names and real handler failures all report an
// error and cannot panic, and a handler failure is the handler's own error
// rather than a pre-entry rejection.
func TestAPOCCallAffectedDispatchRejectsBadCallsWithoutPanic(t *testing.T) {
	t.Run("arity", func(t *testing.T) {
		for _, test := range []struct {
			name string
			args []interface{}
		}{
			{name: "apoc.import.cypherData", args: []interface{}{"CREATE (:A)"}},
			{name: "apoc.import.cypherData", args: []interface{}{"CREATE (:A)", map[string]interface{}{}, map[string]interface{}{}}},
			{name: "apoc.cypher.runMany", args: []interface{}{"CREATE (:A)", map[string]interface{}{}, map[string]interface{}{}}},
			{name: "apoc.cypher.runMany", args: []interface{}{}},
		} {
			test := test
			t.Run(fmt.Sprintf("%s/%d", test.name, len(test.args)), func(t *testing.T) {
				var value interface{}
				var err error
				require.NotPanics(t, func() { value, err = Call(test.name, test.args) },
					"an unusable argument count must not panic")
				require.Nil(t, value, "a refused call publishes no value")
				require.Error(t, err, "an unusable argument count is an error")
				require.False(t, registry.InvocationStarted(err),
					"the handler must not be entered for an argument count it does not accept")
			})
		}
	})

	t.Run("argument types", func(t *testing.T) {
		for _, test := range []struct {
			name string
			args []interface{}
		}{
			{name: "apoc.import.cypherData", args: []interface{}{int64(1), map[string]interface{}{}}},
			{name: "apoc.import.cypherData", args: []interface{}{"CREATE (:A)", []interface{}{"not", "a", "map"}}},
			{name: "apoc.cypher.runMany", args: []interface{}{"CREATE (:A)", "not a map"}},
		} {
			test := test
			t.Run(test.name, func(t *testing.T) {
				var value interface{}
				var err error
				require.NotPanics(t, func() { value, err = Call(test.name, test.args) },
					"an unbound argument must not panic")
				require.Nil(t, value)
				require.Error(t, err, "an argument outside the declared Go type is an error")
				require.False(t, registry.InvocationStarted(err),
					"the handler must not be entered with an unbound argument")
			})
		}
	})

	t.Run("unknown names", func(t *testing.T) {
		for _, name := range []string{"apoc.import.notRegistered", "apoc.cypher.runMany.notRegistered", ""} {
			var value interface{}
			var err error
			require.NotPanics(t, func() { value, err = Call(name, nil) }, "%q must not panic", name)
			require.Nil(t, value)
			require.Error(t, err, "%q is not registered", name)
		}
	})

	t.Run("handler failure", func(t *testing.T) {
		// apoc.import.csvData's real implementation rejects an input with no CSV
		// header row with the reader's own error. That error must reach the
		// caller unchanged, and the call must count as entered.
		var value interface{}
		var err error
		require.NotPanics(t, func() {
			value, err = Call("apoc.import.csvData", []interface{}{"", map[string]interface{}{}})
		}, "a failing handler must not panic")
		require.Nil(t, value, "a failed import publishes no value")
		require.ErrorIs(t, err, io.EOF,
			"the registered handler's own error must surface through the exported boundary")
		require.NotErrorIs(t, err, registry.ErrNotInvoked,
			"a handler failure is not a pre-entry rejection")
		require.True(t, registry.InvocationStarted(err),
			"the registered handler ran and returned this error")
	})
}

// errAPOCCallDispatchSentinel marks any storage operation the affected handlers
// are not expected to make. Its exact spelling is part of the recording so a
// test failure names the operation that reached storage.
var errAPOCCallDispatchSentinel = errors.New("nor175: unexpected APOC storage operation")

// apocCallDispatchSentinelStorage is a mock APOC storage: it records every
// operation with its exact arguments and answers each with the sentinel error.
// A handler that reads or writes the bound backend therefore cannot pass
// silently, and no query is executed anywhere.
type apocCallDispatchSentinelStorage struct {
	mu      sync.Mutex
	records []string
}

func (s *apocCallDispatchSentinelStorage) record(operation string) error {
	s.mu.Lock()
	s.records = append(s.records, operation)
	s.mu.Unlock()
	return errAPOCCallDispatchSentinel
}

func (s *apocCallDispatchSentinelStorage) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.records...)
}

func (s *apocCallDispatchSentinelStorage) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = nil
}

func (s *apocCallDispatchSentinelStorage) GetNode(_ context.Context, id int64) (*apocstorage.Node, error) {
	return nil, s.record(fmt.Sprintf("GetNode(%d)", id))
}

func (s *apocCallDispatchSentinelStorage) CreateNode(_ context.Context, labels []string, properties map[string]interface{}) (*apocstorage.Node, error) {
	return nil, s.record(fmt.Sprintf("CreateNode(labels=%v,properties=%v)", labels, properties))
}

func (s *apocCallDispatchSentinelStorage) UpdateNode(_ context.Context, id int64, update apocstorage.PropertyUpdate) error {
	return s.record(fmt.Sprintf("UpdateNode(%d,%+v)", id, update))
}

func (s *apocCallDispatchSentinelStorage) DeleteNode(_ context.Context, id int64) error {
	return s.record(fmt.Sprintf("DeleteNode(%d)", id))
}

func (s *apocCallDispatchSentinelStorage) AddLabels(_ context.Context, id int64, labels []string) error {
	return s.record(fmt.Sprintf("AddLabels(%d,%v)", id, labels))
}

func (s *apocCallDispatchSentinelStorage) RemoveLabels(_ context.Context, id int64, labels []string) error {
	return s.record(fmt.Sprintf("RemoveLabels(%d,%v)", id, labels))
}

func (s *apocCallDispatchSentinelStorage) AllNodes(context.Context) ([]*apocstorage.Node, error) {
	return nil, s.record("AllNodes()")
}

func (s *apocCallDispatchSentinelStorage) UpdateNodeLabels(_ context.Context, id int64, labels []string) error {
	return s.record(fmt.Sprintf("UpdateNodeLabels(%d,%v)", id, labels))
}

func (s *apocCallDispatchSentinelStorage) GetRelationship(_ context.Context, id int64) (*apocstorage.Relationship, error) {
	return nil, s.record(fmt.Sprintf("GetRelationship(%d)", id))
}

func (s *apocCallDispatchSentinelStorage) CreateRelationship(_ context.Context, startID, endID int64, relType string, properties map[string]interface{}) (*apocstorage.Relationship, error) {
	return nil, s.record(fmt.Sprintf("CreateRelationship(%d,%d,%q,properties=%v)", startID, endID, relType, properties))
}

func (s *apocCallDispatchSentinelStorage) UpdateRelationship(_ context.Context, id int64, update apocstorage.PropertyUpdate) error {
	return s.record(fmt.Sprintf("UpdateRelationship(%d,%+v)", id, update))
}

func (s *apocCallDispatchSentinelStorage) DeleteRelationship(_ context.Context, id int64) error {
	return s.record(fmt.Sprintf("DeleteRelationship(%d)", id))
}

func (s *apocCallDispatchSentinelStorage) UpdateRelationshipType(_ context.Context, id int64, newType string) error {
	return s.record(fmt.Sprintf("UpdateRelationshipType(%d,%q)", id, newType))
}

func (s *apocCallDispatchSentinelStorage) AllRelationships(context.Context) ([]*apocstorage.Relationship, error) {
	return nil, s.record("AllRelationships()")
}

func (s *apocCallDispatchSentinelStorage) GetNodeRelationships(_ context.Context, nodeID int64, relType string, direction apocstorage.Direction) ([]*apocstorage.Relationship, error) {
	return nil, s.record(fmt.Sprintf("GetNodeRelationships(%d,%q,%d)", nodeID, relType, direction))
}

func (s *apocCallDispatchSentinelStorage) GetNodeNeighbors(_ context.Context, nodeID int64, relType string, direction apocstorage.Direction) ([]*apocstorage.Node, error) {
	return nil, s.record(fmt.Sprintf("GetNodeNeighbors(%d,%q,%d)", nodeID, relType, direction))
}

func (s *apocCallDispatchSentinelStorage) GetNodeDegree(_ context.Context, nodeID int64, relType string, direction apocstorage.Direction) (int, error) {
	return 0, s.record(fmt.Sprintf("GetNodeDegree(%d,%q,%d)", nodeID, relType, direction))
}

func (s *apocCallDispatchSentinelStorage) FindShortestPath(_ context.Context, startID, endID int64, relType string, maxHops int) (*apocstorage.Path, error) {
	return nil, s.record(fmt.Sprintf("FindShortestPath(%d,%d,%q,%d)", startID, endID, relType, maxHops))
}

func (s *apocCallDispatchSentinelStorage) FindAllPaths(_ context.Context, startID, endID int64, relType string, maxHops int) ([]*apocstorage.Path, error) {
	return nil, s.record(fmt.Sprintf("FindAllPaths(%d,%d,%q,%d)", startID, endID, relType, maxHops))
}

func (s *apocCallDispatchSentinelStorage) BFS(_ context.Context, startID int64, relType string, maxDepth int, _ func(*apocstorage.Node) bool) error {
	return s.record(fmt.Sprintf("BFS(%d,%q,%d)", startID, relType, maxDepth))
}

func (s *apocCallDispatchSentinelStorage) DFS(_ context.Context, startID int64, relType string, maxDepth int, _ func(*apocstorage.Node) bool) error {
	return s.record(fmt.Sprintf("DFS(%d,%q,%d)", startID, relType, maxDepth))
}

// TestAPOCCallAffectedHandlersExecuteNothingElse proves the outcome equivalence
// is the real handler's answer and nothing else: with a backend that records
// every operation and fails it, the affected handlers still answer their normal
// outcomes and the recorder stays empty on both routes.
func TestAPOCCallAffectedHandlersExecuteNothingElse(t *testing.T) {
	recorder := &apocCallDispatchSentinelStorage{}
	apocCallDispatchInstallStorage(t, recorder)

	// The harness must detect an operation: a real storage call records its
	// exact arguments and returns the sentinel.
	_, err := recorder.GetNode(context.Background(), int64(7))
	require.ErrorIs(t, err, errAPOCCallDispatchSentinel)
	require.Equal(t, []string{"GetNode(7)"}, recorder.recorded())
	recorder.reset()

	for _, name := range apocCallDispatchNames(apocCallDispatchPopulation(t)) {
		name := name
		t.Run(name, func(t *testing.T) {
			args := apocCallDispatchValidArguments(t, name)

			typed, typedErr := apocCallDispatchTypedOutcome(apocRegisteredHandler(t, name), args)
			require.NoError(t, typedErr, "the typed route answers without touching the bound backend")

			value, err := Call(name, args)
			require.NoError(t, err, "the exported boundary answers without touching the bound backend")
			require.Equal(t, typed, value)
			require.Empty(t, recorder.recorded(),
				"the affected handler must not execute an operation against the bound backend")
		})
	}
}

// TestAPOCCallAffectedHandlersKeepSingleRealStorageNode is the small real-storage
// case: with one node in the backend the registry is bound to, the affected
// handlers answer their own outcome (the handler's statement count) and leave
// that backend's graph exactly as it was, because neither handler executes a
// query there.
func TestAPOCCallAffectedHandlersKeepSingleRealStorageNode(t *testing.T) {
	backend := apocBindingBackend(t, "Alice")
	apocCallDispatchInstallStorage(t, backend)

	value, err := Call("apoc.import.cypherData", []interface{}{"CREATE (:A)", map[string]interface{}{}})
	require.False(t, errors.Is(err, registry.ErrUnsupportedSignature),
		"the exported Call rejected the registered handler's signature: %v", err)
	require.NoError(t, err)
	result, ok := value.(*apocimport.ImportResult)
	require.True(t, ok, "got %T", value)
	require.Equal(t, 1, result.Nodes, "the handler's own single-node statement count")

	value, err = Call("apoc.cypher.runMany", []interface{}{"CREATE (:A)", map[string]interface{}{}})
	require.NoError(t, err)
	results, ok := value.([]*apoccypher.Result)
	require.True(t, ok, "got %T", value)
	require.Len(t, results, 1, "the handler's own single-statement result count")

	nodes, err := backend.AllNodes(context.Background())
	require.NoError(t, err)
	require.Len(t, nodes, 1, "the bound backend keeps its one node; no query ran through the handler")
	require.Equal(t, "Alice", nodes[0].Properties["name"])
}
