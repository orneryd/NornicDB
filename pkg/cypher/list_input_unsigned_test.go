package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func TestListInputUnsignedBoundaries(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	handlers := map[string]interface{}{}
	for _, name := range apoc.List() {
		collection := strings.HasPrefix(name, "apoc.coll.") && name != "apoc.coll.partition" && name != "apoc.coll.split"
		conversion := strings.HasPrefix(name, "apoc.convert.to") && (strings.HasSuffix(name, "List") || name == "apoc.convert.toSet" || name == "apoc.convert.toInteger" || name == "apoc.convert.toFloat" || name == "apoc.convert.toBoolean" || name == "apoc.convert.toString")
		if collection || conversion {
			info, ok := apoc.GetInfo(name)
			require.True(t, ok)
			handlers[name] = info.Handler
		}
	}
	withPluginFunctions(t, handlers)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "unsigned_inputs")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	read := func(t *testing.T, query string, params map[string]interface{}) interface{} {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1)
		return result.Rows[0][0]
	}
	encoded := func(t *testing.T, value interface{}) string {
		t.Helper()
		data, err := json.Marshal(value)
		require.NoError(t, err)
		return string(data) // Retain exact 64-bit boundary values; no float oracle.
	}
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range listinputs.UnsignedInputs() {
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, input.Name), func(t *testing.T) {
				list := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(input.Value)), 1, 1)
				list.Index(0).Set(reflect.ValueOf(input.Value))
				boxed := []interface{}{input.Value}
				if !disabled {
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"UnsignedInput"}, Properties: map[string]interface{}{"name": input.Name, "v": input.Value, "xs": boxed}})
					require.NoError(t, err)
				}
				params := map[string]interface{}{"n": input.Value, "xs": list.Interface(), "boxed": boxed, "name": input.Name}
				for _, form := range []struct{ prefix, number, list string }{
					{"", "$n", "$xs"},
					{"WITH $n AS n,$xs AS xs ", "n", "xs"},
					{"MATCH (n:UnsignedInput {name:$name}) ", "n.v", "n.xs"},
				} {
					t.Run(form.prefix, func(t *testing.T) {
						query := func(expression string) string { return form.prefix + "RETURN " + expression + " AS v" }
						require.Equal(t, input.Integer, read(t, query("apoc.convert.toInteger("+form.number+")"), params))
						require.Equal(t, encoded(t, []int64{input.Integer}), encoded(t, read(t, query("apoc.convert.toIntList("+form.list+")"), params)))
						require.Equal(t, input.Integer, read(t, query("apoc.coll.sumLongs("+form.list+")"), params))
						u := reflect.ValueOf(input.Value).Uint()
						require.Equal(t, float64(u), read(t, query("apoc.convert.toFloat("+form.number+")"), params))
						require.Equal(t, u != 0, read(t, query("apoc.convert.toBoolean("+form.number+")"), params))
						require.Equal(t, strconv.FormatUint(u, 10), read(t, query("apoc.convert.toString("+form.number+")"), params))
						for _, template := range listInputPluginQueries() {
							t.Run(template, func(t *testing.T) {
								want := encoded(t, read(t, listinputs.Query(template, "$boxed"), params))
								got := encoded(t, read(t, form.prefix+listinputs.Query(template, form.list), params))
									require.Equal(t, want, got, "typed/boxed parity for every registered list consumer")
							})
						}
					})
				}
				if !input.Valid {
					params["mixed"] = []interface{}{input.Value, uint64(1)}
					require.Equal(t, int64(1), read(t, "RETURN apoc.coll.sumLongs($mixed) AS v", params), "skip the unconvertible value without corrupting valid neighbors")
				}
			})
		}
	}
}
