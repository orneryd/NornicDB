package nornicdb

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/internal/plugintest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBundledAPOCPluginLeavesCollectionBuiltIns loads the bundled APOC
// plugin as the images do (apoc.so built from apoc/plugin-src/apoc, loaded
// from the plugins directory a database opens with) and checks that the
// apoc.coll functions NornicDB implements are answered by the built-ins,
// which compare items as APOC 5.26.0's AnyValues (1 and 1.0 are one item;
// a null equals nothing in the lookups). A plugin function overrides a
// built-in of the same name, and the plugin's own copies compare printed
// text, so this needs the plugin to leave the built-in names alone: it
// passes once the plugin's export exclusion lists these names (#1422 /
// #1494 on develop, #1476 on release/v1.4.4).
func TestBundledAPOCPluginLeavesCollectionBuiltIns(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the APOC plugin")
	}
	source, err := filepath.Abs(filepath.Join("..", "..", "apoc", "plugin-src", "apoc"))
	require.NoError(t, err)
	plugins := t.TempDir()
	args := append([]string{"build"}, plugintest.BuildFlags()...)
	args = append(args, "-buildmode=plugin", "-o", filepath.Join(plugins, "apoc.so"), "apoc_plugin.go")
	build := exec.Command("go", args...)
	build.Dir = source
	build.Env = os.Environ()
	out, err := build.CombinedOutput()
	require.NoError(t, err, "go build apoc plugin: %s", out)

	resetPluginTestState()
	lookup, list := cypher.PluginFunctionLookup, cypher.PluginFunctionList
	t.Cleanup(func() {
		cypher.PluginFunctionLookup, cypher.PluginFunctionList = lookup, list
		resetPluginTestState()
	})
	config := DefaultConfig()
	config.Server.PluginsDir = plugins
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	// The plugin is loaded: a function only it has answers.
	result, err := db.ExecuteCypher(ctx, "RETURN apoc.text.camelCase('hello world') AS v", nil)
	require.NoError(t, err)
	require.Equal(t, "helloWorld", result.Rows[0][0])

	list1 := func(items ...interface{}) []interface{} { return items }
	for query, want := range map[string]interface{}{
		"RETURN apoc.coll.toSet([1, 1.0, 2, 2.0]) AS v":               list1(int64(1), int64(2)),
		"RETURN apoc.coll.toSet([[1], ['1'], [1]]) AS v":              list1(list1(int64(1)), list1("1")),
		"RETURN apoc.coll.union([1, 2], [2.0, 3]) AS v":               list1(int64(1), int64(2), int64(3)),
		"RETURN apoc.coll.intersection([1, 2.0, '2'], [2, 1.0]) AS v": list1(int64(1), float64(2)),
		"RETURN apoc.coll.subtract([1, 1, 2, '2'], [2.0]) AS v":       list1(int64(1), "2"),
		"RETURN apoc.coll.contains([1, 2], 2.0) AS v":                 true,
		"RETURN apoc.coll.contains(['1'], 1) AS v":                    false,
		"RETURN apoc.coll.containsAll([1, 2], [2.0, 1]) AS v":         true,
		"RETURN apoc.coll.containsAny([1, 2], [3, 2.0]) AS v":         true,
		"RETURN apoc.coll.indexOf([1, 2, 3], 2.0) AS v":               int64(1),
		"RETURN apoc.coll.occurrences([1, 1.0, '1', 2], 1) AS v":      int64(2),
		"RETURN apoc.coll.frequencies([1, 1.0, '1']) AS v": list1(
			map[string]interface{}{"item": int64(1), "count": int64(2)},
			map[string]interface{}{"item": "1", "count": int64(1)}),
	} {
		result, err := db.ExecuteCypher(ctx, query, nil)
		require.NoError(t, err, query)
		assert.Equal(t, want, result.Rows[0][0], query)
	}
}
