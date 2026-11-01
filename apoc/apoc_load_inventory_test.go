package apoc

// The bundled scalar plugin exports real I/O loaders as well as placeholder
// stubs. This inventory pins which apoc.load.* exports can reach the network or
// the filesystem, so policy regressions in pkg/cypher and pkg/apocio cover the
// real I/O population and no check claims more than it covers.
//
// The extractor is structural: the shared APOC I/O owner takes the caller's
// statement context, so every real I/O loader is registered with a
// context-first handler and every placeholder stub is not. Nothing here claims
// Neo4j APOC semantics for a stub.

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

// apocHandlerContextType is the caller-context parameter an APOC handler declares.
var apocHandlerContextType = reflect.TypeOf((*context.Context)(nil)).Elem()

// apocLoadIOLoaders are the bundled plugin's apoc.load.* exports that perform
// real I/O through pkg/apocio.
var apocLoadIOLoaders = []string{
	"apoc.load.binary",
	"apoc.load.csvStream",
	"apoc.load.graphQL",
	"apoc.load.jsonStream",
	"apoc.load.rest",
	"apoc.load.stream",
	"apoc.load.xmlSimple",
}

// apocLoadStubs are the bundled plugin's apoc.load.* exports that return fixed
// data and perform no I/O. Names the plugin does not export as scalar
// functions at all (apoc.load.jdbc, apoc.load.html, apoc.load.ldap and the
// other procedure-only entries) are outside this population; see
// TestPluginFunctionNames for that exclusion.
var apocLoadStubs = []string{
	"apoc.load.arrow",
	"apoc.load.avro",
	"apoc.load.azure",
	"apoc.load.directoryTree",
	"apoc.load.elasticsearch",
	"apoc.load.gcs",
	"apoc.load.jsonSchema",
	"apoc.load.kafka",
	"apoc.load.redis",
	"apoc.load.s3",
}

// TestPluginLoadExportInventory extracts the bundled plugin's apoc.load.*
// exports and pins the real I/O loaders apart from the placeholders.
func TestPluginLoadExportInventory(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))

	var ioLoaders, stubs []string
	for _, name := range PluginFunctionNames() {
		if !strings.HasPrefix(name, "apoc.load.") {
			continue
		}
		info, ok := GetInfo(name)
		require.True(t, ok, "exported plugin name %s must be registered", name)
		require.NotNil(t, info.Handler, name)
		signature := info.Handler.Signature()
		require.NotNil(t, signature, name)
		if apocHandlerTakesCallerContext(signature) {
			ioLoaders = append(ioLoaders, name)
			continue
		}
		stubs = append(stubs, name)
	}
	sort.Strings(ioLoaders)
	sort.Strings(stubs)

	require.Equal(t, apocLoadIOLoaders, ioLoaders,
		"the real I/O loaders are the apoc.load.* plugin exports that take the caller's context")
	require.Equal(t, apocLoadStubs, stubs,
		"the remaining apoc.load.* plugin exports are placeholders that perform no I/O")
	t.Logf("apoc.load plugin exports: %d real I/O, %d placeholders", len(ioLoaders), len(stubs))
}

// TestPluginLoadIOExportSignatures pins the transports each real I/O loader
// declares: every one takes the caller context first and a source string next.
func TestPluginLoadIOExportSignatures(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))

	for _, name := range apocLoadIOLoaders {
		info, ok := GetInfo(name)
		require.True(t, ok, name)
		signature := info.Handler.Signature()
		require.True(t, apocHandlerTakesCallerContext(signature), name)
		require.Equal(t, reflect.TypeOf(""), signature.In(1), "%s takes its source as a string", name)
		require.True(t, signature.NumOut() >= 1, name)
		last := signature.Out(signature.NumOut() - 1)
		require.Equal(t, "error", last.Name(), "%s reports its failure as the last result", name)
	}
}

// TestLoadStreamNamesShareCanonicalHandler proves the plugin's stream name is
// registered against the same Go handler as its non-stream counterpart rather
// than a forwarding alias.
func TestLoadStreamNamesShareCanonicalHandler(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))

	for canonical, stream := range map[string]string{
		"apoc.load.csv":    "apoc.load.csvStream",
		"apoc.load.xml":    "apoc.load.xmlSimple",
		"apoc.load.binary": "apoc.load.stream",
	} {
		canonicalInfo, ok := GetInfo(canonical)
		require.True(t, ok, canonical)
		streamInfo, ok := GetInfo(stream)
		require.True(t, ok, stream)
		require.Equal(t, canonicalInfo.Handler.Symbol(), streamInfo.Handler.Symbol(),
			"%s must register the canonical %s handler", stream, canonical)
	}
}

// apocHandlerTakesCallerContext reports whether signature declares the caller
// context as its first parameter.
func apocHandlerTakesCallerContext(signature reflect.Type) bool {
	if signature == nil || signature.Kind() != reflect.Func || signature.NumIn() == 0 {
		return false
	}
	return signature.In(0) == apocHandlerContextType
}
