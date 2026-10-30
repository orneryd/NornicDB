// Author: Codex NOR-184 lane. Real .so loading and database execution, lab only.
package nornicdb

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/testing/apocintegers"
	"github.com/stretchr/testify/require"
)

func TestAPOCPluginInvocationContractNOR184(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go buildmode=plugin is unsupported on Windows")
	}
	pluginsMu.Lock()
	oldLoaded, oldFunctions, oldProcedures, oldInitialized := loadedPlugins, pluginFunctions, pluginProcedures, pluginsInitialized
	loadedPlugins = make(map[string]*LoadedPlugin)
	pluginFunctions = make(map[string]PluginFunction)
	pluginProcedures = make(map[string]PluginProcedure)
	pluginsInitialized = false
	pluginsMu.Unlock()
	oldLookup, oldList := cypher.PluginFunctionLookup, cypher.PluginFunctionList
	t.Cleanup(func() {
		pluginsMu.Lock()
		loadedPlugins, pluginFunctions, pluginProcedures, pluginsInitialized = oldLoaded, oldFunctions, oldProcedures, oldInitialized
		pluginsMu.Unlock()
		cypher.PluginFunctionLookup, cypher.PluginFunctionList = oldLookup, oldList
	})
	directory := t.TempDir()
	// Build the actual bundled exporter, not a copied implementation. Its raw
	// Go function/opaque descriptor transport differs between release and head.
	source, err := os.ReadFile(filepath.Join("..", "..", "apoc", "plugin-src", "apoc", "apoc_plugin.go"))
	require.NoError(t, err)
	buildTestPluginSO(t, directory, "nor184_bundled", string(source))
	buildTestPluginSO(t, directory, "nor184_transports", `package main
import (
 m "github.com/orneryd/nornicdb/apoc/math"
 n "github.com/orneryd/nornicdb/apoc/number"
)
type Descriptor struct{Handler interface{}}
type CheckedPlugin struct{}
func (CheckedPlugin) Type()string{return "function"}
func (CheckedPlugin) Name()string{return "nor184_transports"}
func (CheckedPlugin) Version()string{return "1"}
func (CheckedPlugin) Functions()map[string]interface{}{return map[string]interface{}{
 "apoc.nor184.raw_math_gcd":m.Gcd,
 "apoc.nor184.wrapped_math_lcm":Descriptor{m.Lcm},
 "apoc.nor184.raw_number_gcd":n.GCD,
 "apoc.nor184.wrapped_number_lcm":Descriptor{n.LCM},
}}
var Plugin CheckedPlugin
`)
	require.NoError(t, LoadPluginsFromDir(directory, nil))
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingProvider = "none"
	db, err := Open(filepath.Join(t.TempDir(), "db"), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	entries := []struct{ name, operation string }{
		{"apoc.math.gcd", "gcd"}, {"apoc.math.lcm", "lcm"}, {"apoc.number.gcd", "gcd"}, {"apoc.number.lcm", "lcm"},
		{"apoc.nor184.raw_math_gcd", "gcd"}, {"apoc.nor184.wrapped_math_lcm", "lcm"},
		{"apoc.nor184.raw_number_gcd", "gcd"}, {"apoc.nor184.wrapped_number_lcm", "lcm"},
	}
	for _, entry := range entries {
		published, found := GetPluginFunction(entry.name)
		require.True(t, found, entry.name)
		for _, pair := range [][2]int64{{0, 0}, {0, math.MinInt64}, {12, -18}, {-12, -18}, {math.MinInt64, 0}, {math.MinInt64, math.MinInt64}, {math.MaxInt64, 2}, {4_000_000_000, 6_000_000_000}} {
			want, fits := apocintegers.Expected(entry.operation, pair[0], pair[1])
			t.Run(fmt.Sprintf("%s/descriptor/%d/%d", entry.name, pair[0], pair[1]), func(t *testing.T) {
				var got interface{}
				var err error
				require.NotPanics(t, func() { got, err = apocintegers.DescriptorCall(published.Handler, []interface{}{pair[0], pair[1]}) })
				if !fits {
					require.Error(t, err)
					require.Nil(t, got)
					return
				}
				require.NoError(t, err)
				require.Equal(t, want, got)
			})
			for route, query := range []string{fmt.Sprintf("RETURN %s(%d,%d) AS v", entry.name, pair[0], pair[1]), fmt.Sprintf("UNWIND [$a] AS a WITH a,$b AS b RETURN %s(a,b) AS v", entry.name)} {
				t.Run(fmt.Sprintf("%s/public/%d/%d/route=%d", entry.name, pair[0], pair[1], route), func(t *testing.T) {
					var err error
					require.NotPanics(t, func() {
						result, callErr := db.ExecuteCypher(context.Background(), query, map[string]interface{}{"a": pair[0], "b": pair[1]})
						err = callErr
						if fits && err == nil {
							require.Equal(t, [][]interface{}{{want}}, result.Rows)
						}
					})
					if fits {
						require.NoError(t, err)
						return
					}
					require.Error(t, err)
					if err != nil {
						require.NotContains(t, strings.ToLower(err.Error()), "panic:")
						require.NotContains(t, err.Error(), "nil handler")
						require.NotContains(t, err.Error(), "unsupported function signature")
					}
				})
			}
		}
	}
	// Provider failures and unhandled result protocols must fail public loading
	// before a callable is published; neither may become a successful load.
	for _, bad := range []struct{ name, body string }{
		{"provider_error", `func (BadPlugin) Functions()(map[string]interface{},error){return nil,fmt.Errorf("provider refused registration")}`},
		{"unchecked_result", `func (BadPlugin) Functions()map[string]interface{}{return map[string]interface{}{"apoc.nor184.unchecked":func()(error,int64){return fmt.Errorf("unchecked"),0}}}`},
	} {
		t.Run(bad.name, func(t *testing.T) {
			dir := t.TempDir()
			buildTestPluginSO(t, dir, "nor184_"+bad.name, `package main
import "fmt"
type BadPlugin struct{}
func (BadPlugin) Type()string{return "function"}
func (BadPlugin) Name()string{return "nor184_`+bad.name+`"}
func (BadPlugin) Version()string{return "1"}
`+bad.body+`
var Plugin BadPlugin
`)
			require.Error(t, LoadPluginsFromDir(dir, nil))
			_, published := GetPluginFunction("apoc.nor184.unchecked")
			require.False(t, published)
		})
	}
}
