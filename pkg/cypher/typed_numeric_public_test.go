package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func typedNumericExecutor(t *testing.T) (*StorageExecutor, storage.Engine) {
	t.Helper()
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	handlers := map[string]interface{}{}
	for _, name := range apoc.List() {
		info, ok := apoc.GetInfo(name)
		require.True(t, ok)
		handlers[name] = info.Handler
	}
	withPluginFunctions(t, handlers)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "numeric_family")
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	return NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0}), store
}

func typedNumericRead(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}) interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	require.Len(t, result.Rows, 1, query)
	require.Len(t, result.Rows[0], 1, query)
	return result.Rows[0][0]
}
func typedNumericJSON(t *testing.T, value interface{}) string {
	t.Helper()
	data, err := json.Marshal(typedNumericJSONValue(value))
	require.NoError(t, err)
	return string(data)
}

func typedNumericJSONValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Float32, reflect.Float64:
		number := reflected.Float()
		if math.IsNaN(number) {
			return "numeric-test:NaN"
		}
		if math.IsInf(number, 1) {
			return "numeric-test:+Inf"
		}
		if math.IsInf(number, -1) {
			return "numeric-test:-Inf"
		}
	case reflect.Slice, reflect.Array:
		result := make([]interface{}, reflected.Len())
		for i := range result {
			result[i] = typedNumericJSONValue(reflected.Index(i).Interface())
		}
		return result
	case reflect.Map:
		result := map[string]interface{}{}
		iterator := reflected.MapRange()
		for iterator.Next() {
			result[fmt.Sprint(iterator.Key().Interface())] = typedNumericJSONValue(iterator.Value().Interface())
		}
		return result
	}
	return value
}

// Width/sign/boundary cases run through public conversions, including the
// context evaluator inside a plugin and persisted typed/boxed property reads.
func TestTypedNumericPublicBoundaries(t *testing.T) {
	exec, store := typedNumericExecutor(t)
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range listinputs.NumericInputs() {
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, input.Name), func(t *testing.T) {
				if !disabled {
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"NumericInput"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.Property}})
					require.NoError(t, err)
				}
				params := map[string]interface{}{"xs": input.List, "boxed": []interface{}{input.Scalar}, "name": input.Name}
				for _, form := range []struct{ name, prefix, list string }{
					{"parameter", "", "$xs"}, {"boxed", "", "$boxed"}, {"with", "WITH $xs AS xs ", "xs"},
					{"stored", "MATCH (n:NumericInput {name:$name}) ", "n.xs"},
					{"nested", "WITH {items:[$xs]} AS box ", "head(box.items)"},
				} {
					checks := []struct {
						expression string
						want       interface{}
					}{
						{"toInteger(head(%s))", input.Core}, {"head(toIntegerList(%s))", input.Core},
						{"toIntegerOrNull(head(%s))", input.Core},
						{"apoc.convert.toInteger(head(%s))", input.APOC}, {"head(apoc.convert.toIntList(%s))", input.APOC},
						{"apoc.coll.sumLongs(%s)", input.Sum},
						{"apoc.map.setKey({},'v',toInteger(coalesce(%s,[])[0])).v", input.Core},
						{"toInteger(%s[0])", input.Core},
					}
					reflected := reflect.ValueOf(input.Scalar)
					if kind := reflected.Kind(); kind >= reflect.Int && kind <= reflect.Uint64 {
						floating := reflected.Convert(reflect.TypeOf(float64(0))).Float()
						checks = append(checks, struct {
							expression string
							want       interface{}
						}{"toFloat(head(%s))", floating},
							struct {
								expression string
								want       interface{}
							}{"toBoolean(head(%s))", floating != 0},
							struct {
								expression string
								want       interface{}
							}{"toString(head(%s))", fmt.Sprint(input.Scalar)})
					}
					for _, tc := range checks {
						t.Run(form.name+"/"+tc.expression, func(t *testing.T) {
							query := form.prefix + listinputs.Query(tc.expression, form.list)
							require.Equal(t, tc.want, typedNumericRead(t, exec, query, params), query)
						})
					}
				}
			})
		}
	}
}

// Representation parity uses public literal controls for ordinary safe values,
// independently of the production converter. Boundary expectations above are
// explicit, so saturation cannot hide behind an equally broken literal path.
func TestTypedNumericPublicWidths(t *testing.T) {
	exec, store := typedNumericExecutor(t)
	queries := append(listInputPluginQueries(), listinputs.Queries()...)
	queries = append(queries,
		"vector.similarity.cosine(%s,%s)", "vector.similarity.euclidean(%s,%s)",
		"vector_dimension_count(vector(%s,3,FLOAT32))", "vector_dimension_count(vector(%s,3,INTEGER64))",
		"%s[0]", "%s[-1]", "%s[1..3]", "%s[..-1]",
		"apoc.map.setKey({},'v',coalesce(%s,[])[0])",
		"apoc.map.setKey({},'v',coalesce(%s,[])[1..3])",
		"abs(head(%s))", "sqrt(head(%s))", "head(%s) < last(%s)",
		"apoc.meta.type(%s)", "apoc.util.typeOf(%s)",
		"apoc.number.format(head(%s),'#,##0.00')",
	)
	for i, template := range queries {
		switch template {
		case "apoc.coll.intersection(%s,%s)":
			// The registered library returns a set in map iteration order.
			queries[i] = "coll.sort(" + template + ")"
		case "apoc.map.clean({a:3,b:1,c:9},[],%s)":
			// The bundled map library owns Go-kind-sensitive value equality.
			// Supply both map and exclusion values from the same coordinates so
			// discarding a typed list still fails without changing that policy.
			queries[i] = "apoc.map.clean({a:head(%s),b:last(%s),c:9},[],%s)"
		}
	}
	// Mechanically enumerate the registered APOC numeric/list ABI family too.
	// Small coordinates avoid running factorial/fibonacci/prime algorithms at
	// huge representable limits; their target-width boundary is tested separately.
	for _, name := range apoc.List() {
		if strings.HasPrefix(name, "apoc.util.") {
			info, _ := apoc.GetInfo(name)
			signature := reflect.TypeOf(info.Handler)
			if signature.Kind() == reflect.Func && signature.NumIn() == 1 && signature.In(0).Kind() == reflect.Interface {
				queries = append(queries, name+"(%s)", name+"(head(%s))")
			}
		}
		if !strings.HasPrefix(name, "apoc.math.") && !strings.HasPrefix(name, "apoc.ml.") && !strings.HasPrefix(name, "apoc.agg.") && !strings.HasPrefix(name, "apoc.stats.") {
			continue
		}
		info, _ := apoc.GetInfo(name)
		signature := reflect.TypeOf(info.Handler)
		if signature.Kind() != reflect.Func || signature.NumIn() == 0 {
			continue
		}
		args := []string{}
		supported := true
		for i := 0; i < signature.NumIn(); i++ {
			target := signature.In(i)
			if signature.IsVariadic() && i == signature.NumIn()-1 {
				target = target.Elem()
			}
			kind := target.Kind()
			switch kind {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
				args = append(args, "head(%s)")
			case reflect.Slice:
				args = append(args, "%s")
			default:
				supported = false
			}
		}
		if supported {
			expression := name + "(" + strings.Join(args, ",") + ")"
			if name == "apoc.agg.histogram" {
				expression = "apoc.coll.sortMaps(" + expression + ",'bucket')"
			}
			queries = append(queries, expression)
		}
	}
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range listinputs.Inputs() {
			switch input.Name {
			case "bool", "string", "maps", "nested", "nested-string", "nested-array", "mixed", "string-array", "named-strings":
				continue
			}
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, input.Name), func(t *testing.T) {
				if !disabled {
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"NumericWidth"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.PropertyValue()}})
					require.NoError(t, err)
				}
				params := map[string]interface{}{"xs": input.Value, "name": input.Name}
				for _, template := range queries {
					t.Run(template, func(t *testing.T) {
						// Compare public codes rather than Go-kind-dependent wording.
						// fromPairs has a reference-backed static property-type error
						// distinct from the scalar literal's runtime coercion error.
						control, controlErr := exec.Execute(context.Background(), listinputs.Query(template, input.Literal), nil)
						if template == "apoc.map.fromPairs(%s)" {
							requireStatusCode(t, controlErr, listinputs.FromPairsExpectation(input, "literal").Code)
						}
						for _, form := range []struct{ name, prefix, value string }{
							{"parameter", "", "$xs"}, {"with", "WITH $xs AS xs ", "xs"}, {"stored", "MATCH (n:NumericWidth {name:$name}) ", "n.xs"},
							{"nested-map", "WITH {items:$xs} AS box ", "box.items"},
						} {
							// The rebased APOC hash owner preserves byte[] as binary,
							// whose Java text includes brackets, unlike LIST<INTEGER>.
							// Keep testing it, using an independent text input oracle.
							if input.Name == "uint8" && strings.HasSuffix(template, "(%s)") &&
								(strings.HasPrefix(template, "apoc.util.md5") || strings.HasPrefix(template, "apoc.util.sha")) {
								control, controlErr = exec.Execute(context.Background(), listinputs.Query(template, "['[3, 1, 3]']"), nil)
							}
							if input.Name == "uint8" && form.value == "$xs" && (strings.HasPrefix(template, "reduce(") || strings.HasPrefix(template, "[x IN")) {
								continue
							} // Existing binary-parameter grammar contract has separate tests.
							query := form.prefix + listinputs.Query(template, form.value)
							got, err := exec.Execute(context.Background(), query, params)
							if template == "apoc.map.fromPairs(%s)" {
								requireStatusCode(t, err, listinputs.FromPairsExpectation(input, form.name).Code)
								continue
							}
							if controlErr != nil {
								require.Error(t, err, query)
								require.Equal(t, errorStatusCode(controlErr), errorStatusCode(err), query)
								continue
							}
							require.NoError(t, err, query)
							require.Equal(t, typedNumericJSON(t, control.Rows), typedNumericJSON(t, got.Rows), query)
						}
					})
				}
				for _, aggregate := range []string{"sum", "avg", "min", "max", "stDev", "stDevP", "collect"} {
					query := "UNWIND $xs AS x RETURN " + aggregate + "(x) AS v"
					literal := "UNWIND " + input.Literal + " AS x RETURN " + aggregate + "(x) AS v"
					require.Equal(t, typedNumericJSON(t, typedNumericRead(t, exec, literal, nil)), typedNumericJSON(t, typedNumericRead(t, exec, query, params)), query)
				}
				query := "UNWIND $xs AS x RETURN x ORDER BY x"
				got, err := exec.Execute(context.Background(), query, params)
				require.NoError(t, err)
				control, err := exec.Execute(context.Background(), "UNWIND "+input.Literal+" AS x RETURN x ORDER BY x", nil)
				require.NoError(t, err)
				require.Equal(t, typedNumericJSON(t, control.Rows), typedNumericJSON(t, got.Rows))
			})
		}
	}
}

func TestTypedNumericSubscriptBounds(t *testing.T) {
	exec, _ := typedNumericExecutor(t)
	for _, value := range []interface{}{uint64(math.MaxUint64), uint64(math.MaxInt64) + 1} {
		params := map[string]interface{}{"index": value, "xs": []int16{3, 1, 3}}
		for _, disabled := range []bool{false, true} {
			rowValueProgramsDisabled.Store(disabled)
			require.Nil(t, typedNumericRead(t, exec, "RETURN $xs[$index] AS v", params))
			require.Nil(t, typedNumericRead(t, exec, "RETURN apoc.map.setKey({},'v',coalesce($xs,[])[$index]).v AS v", params))
			if reflect.TypeOf(value).Kind() == reflect.Uint64 {
				require.Equal(t, "[]", typedNumericJSON(t, typedNumericRead(t, exec, "RETURN $xs[$index..] AS v", params)))
			}
		}
	}
	for _, index := range []interface{}{float64(1), float32(1), float64(0x1p63), float32(0x1p63), 1.5, math.Inf(1), math.NaN(), true} {
		_, err := exec.Execute(context.Background(), "RETURN $xs[$index] AS v", map[string]interface{}{"xs": []int8{3, 1, 3}, "index": index})
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}

// A small fail-first public suite is copied unchanged to the current release
// base. The larger generated matrices exercise the complete input population.
func TestTypedNumericReviewRepros(t *testing.T) {
	exec, store := typedNumericExecutor(t)
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		t.Run(fmt.Sprintf("programsDisabled=%t", disabled), func(t *testing.T) {
			for _, tc := range []struct {
				query       string
				value, want interface{}
			}{
				{"RETURN head(toIntegerList($xs)) AS v", []float64{1e20}, int64(math.MaxInt64)},
				{"RETURN head(toIntegerList($xs)) AS v", []string{"1e20"}, nil},
				{"RETURN apoc.convert.toInteger(head($xs)) AS v", []float64{0x1p63}, int64(0)},
				{"RETURN apoc.coll.sumLongs($xs) AS v", []float32{0x1p63}, int64(0)},
			} {
				t.Run(tc.query+fmt.Sprintf("/%T", tc.value), func(t *testing.T) {
					require.Equal(t, tc.want, typedNumericRead(t, exec, tc.query, map[string]interface{}{"xs": tc.value}))
				})
			}
			for _, input := range listinputs.Inputs() {
				switch input.Name {
				case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "array", "named-ints":
				default:
					continue
				}
				if !disabled {
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"NumericReview"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.PropertyValue()}})
					require.NoError(t, err)
				}
				params := map[string]interface{}{"xs": input.Value, "name": input.Name}
				for _, metric := range []string{"cosine", "euclidean"} {
					t.Run(input.Name+"/vector/"+metric, func(t *testing.T) {
						require.Equal(t, float64(1), typedNumericRead(t, exec, "RETURN vector.similarity."+metric+"($xs,$xs) AS v", params))
					})
				}
				for _, form := range []struct{ prefix, value string }{
					{"", "coalesce($xs,[])[0]"},
					{"WITH $xs AS xs ", "coalesce(xs,[])[0]"},
					{"MATCH (n:NumericReview {name:$name}) ", "coalesce(n.xs,[])[0]"},
					{"MATCH (n:NumericReview {name:$name}) ", "n.xs[0]"},
					{"", "coalesce([$xs],[])[0][0]"},
				} {
					t.Run(input.Name+"/context/"+form.prefix+form.value, func(t *testing.T) {
						control := typedNumericRead(t, exec, listinputs.Query("apoc.map.setKey({},'v',head(%s))", input.Literal), nil)
						got := typedNumericRead(t, exec, form.prefix+"RETURN apoc.map.setKey({},'v',"+form.value+") AS v", params)
						require.Equal(t, typedNumericJSON(t, control), typedNumericJSON(t, got))
					})
				}
			}
		})
	}
}

// Record the inherited scalar-string/map policies on base and candidate through
// public Cypher before deciding whether a branch-specific adapter is needed.
// This probe is evidence, not a green assertion of any changed contract.
func TestTypedNumericSubscriptPolicyProbe(t *testing.T) {
	exec, store := typedNumericExecutor(t)
	_, err := store.CreateNode(&storage.Node{ID: "policy", Labels: []string{"NumericPolicy"}, Properties: map[string]interface{}{"text": "a\u00e9\U0001f642", "xs": []int64{3, 1, 3}}})
	require.NoError(t, err)
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, query := range []string{
			"RETURN 'a\u00e9\U0001f642'[1] AS v",
			"RETURN apoc.map.setKey({},'v',coalesce($text,'')[1]) AS v",
			"RETURN apoc.map.setKey({},'v',coalesce($text,'')[-1]) AS v",
			"MATCH (n:NumericPolicy) RETURN apoc.map.setKey({},'v',coalesce(n.text,'')[1]) AS v",
			"RETURN apoc.map.setKey({},'v',{a:7}['a']) AS v",
			"RETURN apoc.map.setKey({},'v',{a:7}[0]) AS v",
			"RETURN apoc.map.setKey({},'v',coalesce($xs,[])[1.0]) AS v",
			"RETURN apoc.map.setKey({},'v',coalesce($xs,[])['1']) AS v",
		} {
			result, err := exec.Execute(context.Background(), query, map[string]interface{}{"text": "a\u00e9\U0001f642", "xs": []int64{3, 1, 3}})
			if err != nil {
				t.Logf("programsDisabled=%t query=%s code=%s error=%s", disabled, query, errorStatusCode(err), err)
				continue
			}
			t.Logf("programsDisabled=%t query=%s result=%s", disabled, query, typedNumericJSON(t, result.Rows))
		}
	}
}

func TestTypedNumericMathNarrowing(t *testing.T) {
	exec, _ := typedNumericExecutor(t)
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range listinputs.NumericInputs() {
			if input.Valid || strings.HasPrefix(input.Name, "string/") {
				continue
			}
			params := map[string]interface{}{"xs": input.List}
			for _, expression := range []string{
				"apoc.math.percentile([1.0,2.0,3.0],head($xs))",
				"apoc.stats.percentile([1.0,2.0,3.0],head($xs))",
				"apoc.agg.percentile([1.0,2.0,3.0],head($xs))",
			} {
				require.Equal(t, float64(0), typedNumericRead(t, exec, "RETURN "+expression+" AS v", params), input.Name)
			}
		}
		require.Equal(t, "18,446,744,073,709,551,616.00", typedNumericRead(t, exec, "RETURN apoc.number.format(head($xs),'#,##0.00') AS v", map[string]interface{}{"xs": []uint64{math.MaxUint64}}))
		require.Equal(t, "[3,0]", typedNumericJSON(t, typedNumericRead(t, exec, "RETURN apoc.stats.histogram($xs,2) AS v", map[string]interface{}{"xs": []uint16{3, 3, 3}})))
		got := typedNumericRead(t, exec, "RETURN apoc.agg.histogram($xs,1.0) AS v", map[string]interface{}{"xs": []float64{1e20}})
		require.Equal(t, `[{"bucket":100000000000000000000,"count":1}]`, typedNumericJSON(t, got))
	}
	// Every real registered signed-count/numeric handler must reject unsigned
	// overflow during binding, before potentially unbounded algorithms run.
	for _, name := range apoc.List() {
		if !strings.HasPrefix(name, "apoc.math.") && !strings.HasPrefix(name, "apoc.number.") && !strings.HasPrefix(name, "apoc.stats.") && !strings.HasPrefix(name, "apoc.agg.") && !strings.HasPrefix(name, "apoc.coll.") {
			continue
		}
		info, _ := apoc.GetInfo(name)
		signature := reflect.TypeOf(info.Handler)
		if signature.Kind() != reflect.Func || signature.IsVariadic() {
			continue
		}
		signed, supported := false, true
		args := []string{}
		for i := 0; i < signature.NumIn(); i++ {
			switch signature.In(i).Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				signed = true
				args = append(args, "head($xs)")
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
				args = append(args, "1")
			case reflect.String:
				args = append(args, "''")
			case reflect.Slice:
				args = append(args, "[1,2,3]")
			case reflect.Interface:
				args = append(args, "1")
			default:
				supported = false
			}
		}
		if !signed || !supported {
			continue
		}
		t.Run(name, func(t *testing.T) {
			// Bind the actual registered handler through an alias: a few native
			// Cypher functions (notably split by separator) have a different
			// signature from the bundled library's split-by-chunk-size ABI.
			withPluginFunctions(t, map[string]interface{}{"zz.numeric.checked": info.Handler})
			for _, disabled := range []bool{false, true} {
				rowValueProgramsDisabled.Store(disabled)
				_, err := exec.Execute(context.Background(), "RETURN zz.numeric.checked("+strings.Join(args, ",")+") AS v", map[string]interface{}{"xs": []uint64{math.MaxUint64}})
				require.Error(t, err)
			}
		})
	}
}
