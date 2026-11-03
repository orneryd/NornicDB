// One canonical owner for the retired APOC Go forwarding aliases.
//
// Ten Go helpers were one-line forwards of another helper in the same package:
// neighbors.BFS, meta.Export, meta.Snapshot, schema.Export, schema.Snapshot,
// meta.CypherType, meta.CypherTypes, meta.Diff, meta.Restore and
// schema.Restore. Each Cypher name keeps its exact spelling, category,
// description and example metadata, and is registered directly against the
// canonical owner; no forwarding wrapper remains.
//
// Every assertion goes through the registered handler of the initialized
// registry, which TestMain binds to apocInitializedBackend, so the binding the
// query adapter reaches is the one under test. Nothing here claims a name is
// procedure-classified, and a Go-value-only helper is not presented as a
// validated wire function: the subject is the registry binding, the admitted
// symbol and the arguments/results the handler answers with.
package apoc

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// apocAliasContextType is the caller lifetime the adapter binds for a
// context-first handler.
var apocAliasContextType = reflect.TypeOf((*context.Context)(nil)).Elem()

// apocAliasBinding is one Cypher name whose retired Go forwarding alias must now
// answer from the canonical owner: the same admitted symbol and Go signature as
// the canonical Cypher name, with the registration metadata unchanged.
type apocAliasBinding struct {
	name          string
	canonicalName string
	canonicalSym  string
	retiredSym    string
	category      string
	description   string
	example       string
	signature     reflect.Type
	contextFirst  bool
}

// apocAliasBindings is the complete population: one entry per retired Go alias.
var apocAliasBindings = []apocAliasBinding{
	{
		name:          "apoc.neighbors.bfs",
		canonicalName: "apoc.neighbors.toHop",
		canonicalSym:  "/apoc/neighbors.(*Functions).ToHop",
		retiredSym:    "/apoc/neighbors.(*Functions).BFS",
		category:      "neighbors",
		description:   "BFS neighbors",
		example:       "apoc.neighbors.bfs(node, 'TYPE', 5) => nodes",
		signature:     reflect.TypeOf((func(context.Context, *apocstorage.Node, string, int) ([]*apocstorage.Node, error))(nil)),
		contextFirst:  true,
	},
	{
		name:          "apoc.meta.export",
		canonicalName: "apoc.meta.schema",
		canonicalSym:  "/apoc/meta.(*BoundFunctions).Schema",
		retiredSym:    "/apoc/meta.(*BoundFunctions).Export",
		category:      "meta",
		description:   "Export schema",
		example:       "apoc.meta.export() => schema",
		signature:     reflect.TypeOf((func(context.Context) (map[string]interface{}, error))(nil)),
		contextFirst:  true,
	},
	{
		name:          "apoc.meta.snapshot",
		canonicalName: "apoc.meta.schema",
		canonicalSym:  "/apoc/meta.(*BoundFunctions).Schema",
		retiredSym:    "/apoc/meta.(*BoundFunctions).Snapshot",
		category:      "meta",
		description:   "Schema snapshot",
		example:       "apoc.meta.snapshot() => snapshot",
		signature:     reflect.TypeOf((func(context.Context) (map[string]interface{}, error))(nil)),
		contextFirst:  true,
	},
	{
		name:          "apoc.schema.export",
		canonicalName: "apoc.schema.info",
		canonicalSym:  "/apoc/schema.(*Functions).Info",
		retiredSym:    "/apoc/schema.(*Functions).Export",
		category:      "schema",
		description:   "Export schema",
		example:       "apoc.schema.export() => schema",
		signature:     reflect.TypeOf((func(context.Context) (map[string]interface{}, error))(nil)),
		contextFirst:  true,
	},
	{
		name:          "apoc.schema.snapshot",
		canonicalName: "apoc.schema.info",
		canonicalSym:  "/apoc/schema.(*Functions).Info",
		retiredSym:    "/apoc/schema.(*Functions).Snapshot",
		category:      "schema",
		description:   "Schema snapshot",
		example:       "apoc.schema.snapshot() => snapshot",
		signature:     reflect.TypeOf((func(context.Context) (map[string]interface{}, error))(nil)),
		contextFirst:  true,
	},
	{
		name:          "apoc.meta.cypherType",
		canonicalName: "apoc.meta.type",
		canonicalSym:  "/apoc/meta.Type",
		retiredSym:    "/apoc/meta.CypherType",
		category:      "meta",
		description:   "Cypher type",
		example:       "apoc.meta.cypherType(value) => 'STRING'",
		signature:     reflect.TypeOf((func(interface{}) string)(nil)),
	},
	{
		name:          "apoc.meta.cypherTypes",
		canonicalName: "apoc.meta.types",
		canonicalSym:  "/apoc/meta.Types",
		retiredSym:    "/apoc/meta.CypherTypes",
		category:      "meta",
		description:   "Cypher types",
		example:       "apoc.meta.cypherTypes(map) => types",
		signature:     reflect.TypeOf((func(*apocstorage.Node) map[string]string)(nil)),
	},
	{
		name:          "apoc.meta.diff",
		canonicalName: "apoc.meta.compare",
		canonicalSym:  "/apoc/meta.Compare",
		retiredSym:    "/apoc/meta.Diff",
		category:      "meta",
		description:   "Diff schemas",
		example:       "apoc.meta.diff(s1, s2) => diff",
		signature:     reflect.TypeOf((func(map[string]interface{}, map[string]interface{}) map[string]interface{})(nil)),
	},
	{
		name:          "apoc.meta.restore",
		canonicalName: "apoc.meta.import",
		canonicalSym:  "/apoc/meta.Import",
		retiredSym:    "/apoc/meta.Restore",
		category:      "meta",
		description:   "Restore schema",
		example:       "apoc.meta.restore(snapshot) => result",
		signature:     reflect.TypeOf((func(map[string]interface{}) error)(nil)),
	},
	{
		name:          "apoc.schema.restore",
		canonicalName: "apoc.schema.import",
		canonicalSym:  "/apoc/schema.Import",
		retiredSym:    "/apoc/schema.Restore",
		category:      "schema",
		description:   "Restore schema",
		example:       "apoc.schema.restore(snapshot) => result",
		signature:     reflect.TypeOf((func(map[string]interface{}) error)(nil)),
	},
}

// apocCallAliasHandler invokes a registered handler with arguments only, the way
// the query adapter binds a helper that declares no caller context first.
func apocCallAliasHandler(t *testing.T, name string, args ...interface{}) (interface{}, error) {
	t.Helper()
	return apocRegisteredHandler(t, name).CallWithPolicy(args, nil, registry.CypherArguments)
}

// TestRetiredAliasesBindTheirOneCanonicalOwner pins every retired alias to the
// one owner its canonical Cypher name registers: the admitted symbol is the
// canonical one, the retired Go symbol is gone, the Go signature is unchanged,
// and the name, category, description and example metadata are preserved.
func TestRetiredAliasesBindTheirOneCanonicalOwner(t *testing.T) {
	registered := map[string]bool{}
	for _, name := range List() {
		registered[name] = true
	}

	for _, binding := range apocAliasBindings {
		t.Run(binding.name, func(t *testing.T) {
			require.True(t, registered[binding.name],
				"%s keeps its registered spelling", binding.name)
			if lowered := strings.ToLower(binding.name); lowered != binding.name {
				require.False(t, registered[lowered],
					"%s must not be re-registered under a different case", binding.name)
			}

			info, found := GetInfo(binding.name)
			require.True(t, found, binding.name)
			require.Equal(t, binding.category, info.Category, binding.name)
			require.Equal(t, binding.description, info.Description, binding.name)
			require.Equal(t, []string{binding.example}, info.Examples, binding.name)

			handler := apocRegisteredHandler(t, binding.name)
			canonical := apocRegisteredHandler(t, binding.canonicalName)
			require.Equal(t, canonical.Symbol(), handler.Symbol(),
				"%s must register the owner %s already registers",
				binding.name, binding.canonicalName)
			require.Contains(t, handler.Symbol(), binding.canonicalSym,
				"%s must name the canonical owner", binding.name)
			require.NotContains(t, handler.Symbol(), binding.retiredSym,
				"%s must not answer from the retired forwarding alias", binding.name)
			require.Equal(t, canonical.Signature(), handler.Signature(),
				"%s keeps the canonical owner's Go signature", binding.name)
			require.Equal(t, binding.signature, handler.Signature(),
				"%s keeps its declared Go signature", binding.name)

			contextFirst := handler.Signature().NumIn() > 0 &&
				handler.Signature().In(0) == apocAliasContextType
			require.Equal(t, binding.contextFirst, contextFirst,
				"%s must keep declaring the caller context first", binding.name)
		})
	}
}

// TestRetiredAliasNamesAnswerLikeTheirCanonicalOwner pins unchanged answers
// through the retired Cypher names against the initialized backend: the shared
// schema owners, the free value helpers and the argument errors a caller sees.
func TestRetiredAliasNamesAnswerLikeTheirCanonicalOwner(t *testing.T) {
	ctx := context.Background()

	schema, err := apocCallStatementHandler(t, "apoc.meta.schema", ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Person"}, schema.(map[string]interface{})["labels"])
	require.ElementsMatch(t, []string{"name"}, schema.(map[string]interface{})["propertyKeys"])
	for _, name := range []string{"apoc.meta.export", "apoc.meta.snapshot"} {
		value, callErr := apocCallStatementHandler(t, name, ctx)
		require.NoError(t, callErr, name)
		require.Equal(t, schema, value, "%s answers with the schema owner's value", name)
	}

	info, err := apocCallStatementHandler(t, "apoc.schema.info", ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Person"}, info.(map[string]interface{})["labels"])
	for _, name := range []string{"apoc.schema.export", "apoc.schema.snapshot"} {
		value, callErr := apocCallStatementHandler(t, name, ctx)
		require.NoError(t, callErr, name)
		require.Equal(t, info, value, "%s answers with the schema info owner's value", name)
	}

	for _, sample := range []struct {
		value interface{}
		want  string
	}{
		{int64(3), "INTEGER"},
		{"Alice", "STRING"},
		{true, "BOOLEAN"},
		{[]interface{}{"a", "b"}, "LIST"},
		{&apocstorage.Node{ID: 1}, "NODE"},
	} {
		canonicalValue, callErr := apocCallAliasHandler(t, "apoc.meta.type", sample.value)
		require.NoError(t, callErr)
		require.Equal(t, sample.want, canonicalValue)
		aliasValue, callErr := apocCallAliasHandler(t, "apoc.meta.cypherType", sample.value)
		require.NoError(t, callErr)
		require.Equal(t, canonicalValue, aliasValue, "apoc.meta.cypherType(%v)", sample.value)
	}

	node := &apocstorage.Node{ID: 7, Properties: map[string]interface{}{
		"name": "Alice",
		"age":  int64(30),
	}}
	wantTypes := map[string]string{"name": "STRING", "age": "INTEGER"}
	canonicalTypes, err := apocCallAliasHandler(t, "apoc.meta.types", node)
	require.NoError(t, err)
	require.Equal(t, wantTypes, canonicalTypes)
	aliasTypes, err := apocCallAliasHandler(t, "apoc.meta.cypherTypes", node)
	require.NoError(t, err)
	require.Equal(t, canonicalTypes, aliasTypes)

	before := map[string]interface{}{"labels": []string{"Person"}}
	after := map[string]interface{}{"labels": []string{"Person", "Company"}}
	wantDiff := map[string]interface{}{
		"added":   []string{},
		"removed": []string{},
		"changed": []string{},
	}
	canonicalDiff, err := apocCallAliasHandler(t, "apoc.meta.compare", before, after)
	require.NoError(t, err)
	require.Equal(t, wantDiff, canonicalDiff)
	aliasDiff, err := apocCallAliasHandler(t, "apoc.meta.diff", before, after)
	require.NoError(t, err)
	require.Equal(t, canonicalDiff, aliasDiff)

	for _, pair := range [][2]string{
		{"apoc.meta.import", "apoc.meta.restore"},
		{"apoc.schema.import", "apoc.schema.restore"},
	} {
		canonicalValue, callErr := apocCallAliasHandler(t, pair[0], before)
		require.NoError(t, callErr, pair[0])
		require.Nil(t, canonicalValue, pair[0])
		aliasValue, callErr := apocCallAliasHandler(t, pair[1], before)
		require.NoError(t, callErr, pair[1])
		require.Nil(t, aliasValue, pair[1])
	}

	// The neighbor helper keeps the canonical owner's argument protocol: the
	// arity and conversion errors a caller sees are the owner's own.
	neighbor := &apocstorage.Node{ID: 1}
	_, toHopArity := apocCallStatementHandler(t, "apoc.neighbors.toHop", ctx, neighbor, "KNOWS")
	_, bfsArity := apocCallStatementHandler(t, "apoc.neighbors.bfs", ctx, neighbor, "KNOWS")
	require.ErrorIs(t, bfsArity, registry.ErrUnsupportedSignature)
	require.Contains(t, bfsArity.Error(), "expected 4 arguments, got 3")
	require.EqualError(t, bfsArity, toHopArity.Error())

	_, toHopConversion := apocCallStatementHandler(t, "apoc.neighbors.toHop", ctx, neighbor, "KNOWS", "not-a-number")
	_, bfsConversion := apocCallStatementHandler(t, "apoc.neighbors.bfs", ctx, neighbor, "KNOWS", "not-a-number")
	require.ErrorIs(t, bfsConversion, registry.ErrUnsupportedSignature)
	require.Equal(t, toHopConversion.Error(), bfsConversion.Error())

	neighbors, err := apocInitializedBackend.AllNodes(ctx)
	require.NoError(t, err)
	require.Len(t, neighbors, 1)
	toHopValue, err := apocCallStatementHandler(t, "apoc.neighbors.toHop", ctx, neighbors[0], "", 5)
	require.NoError(t, err)
	bfsValue, err := apocCallStatementHandler(t, "apoc.neighbors.bfs", ctx, neighbors[0], "", 5)
	require.NoError(t, err)
	require.Equal(t, toHopValue, bfsValue)
	require.Len(t, bfsValue.([]*apocstorage.Node), 1,
		"the one isolated node is its own whole BFS answer")
}

// TestRetainedAliasHandlerKeepsItsBackend pins the backend lifetime through a
// retired Cypher name: a handler admitted before a backend swap answers from the
// backend it was built for, while the name's new registration answers from the
// new backend.
func TestRetainedAliasHandlerKeepsItsBackend(t *testing.T) {
	ctx := context.Background()

	second := apocstorage.NewInMemoryStorage()
	_, err := second.CreateNode(t.Context(), []string{"Person"}, map[string]interface{}{"name": "Bill"})
	require.NoError(t, err)
	_, err = second.CreateNode(t.Context(), []string{"Project"}, map[string]interface{}{"name": "Apollo"})
	require.NoError(t, err)

	retained := apocRegisteredHandler(t, "apoc.meta.export")
	previous := GetStorage()
	require.NotNil(t, previous)
	require.NoError(t, SetStorage(second))
	t.Cleanup(func() { require.NoError(t, SetStorage(previous)) })

	retainedValue, err := retained.CallWithPolicy(nil,
		[]reflect.Value{reflect.ValueOf(ctx)}, registry.CypherArguments)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Person"},
		retainedValue.(map[string]interface{})["labels"].([]string),
		"the retained handler still answers from its own backend")

	freshValue, err := apocCallStatementHandler(t, "apoc.meta.export", ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Person", "Project"},
		freshValue.(map[string]interface{})["labels"].([]string),
		"the new registration answers from the new backend")
}

// TestRetiredAliasHandlersHandOverTheCallerContext pins the lifetime contract
// through a retired Cypher name: the handler the registry publishes hands the
// caller's own context to its whole-graph read, exactly as the canonical owner
// does today.
func TestRetiredAliasHandlersHandOverTheCallerContext(t *testing.T) {
	require.NoError(t, Initialize(apocstorage.NewInMemoryStorage(), nil))
	recorder := &recordingAPOCStorage{}
	installRecordingStorage(t, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, name := range []string{
		"apoc.meta.export",
		"apoc.meta.snapshot",
		"apoc.schema.export",
		"apoc.schema.snapshot",
	} {
		recorder.reset()
		_, err := apocCallStatementHandler(t, name, ctx)
		require.NoError(t, err, name)
		observed := fixtureowner.Snapshot(&recorder.mu, &recorder.observations)
		require.NotEmpty(t, observed, "%s must read the whole graph", name)
		for _, read := range observed {
			require.True(t, read.ctx == ctx,
				"%s handed its %s read a replacement lifetime", name, read.kind)
		}
	}
}
