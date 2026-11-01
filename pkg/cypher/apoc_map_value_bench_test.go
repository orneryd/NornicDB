package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// These benchmarks cover the two convergences they sit next to: the map
// constructors' and removers' shared argument and copy work, and the one case
// mapping owner behind upper/lower and toUpper/toLower. They measure the
// registered functions through the registry the statement evaluators call.

func BenchmarkApocMapRemoveKeysCopy(b *testing.B) {
	for _, size := range []struct {
		name  string
		count int
	}{
		{"small", 8},
		{"large", 2048},
	} {
		b.Run(size.name, func(b *testing.B) {
			entries := apocCancellationMap(size.count)
			scope := apocRemovalScope(entries)
			args := []string{"m", "keys", "config"}
			workCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				value, found, err := cypherfn.EvaluateFunction(workCtx, "apoc.map.removekeys", args, scope)
				if err != nil || !found || value == nil {
					b.Fatalf("removeKeys: found=%v err=%v value=%v", found, err, value)
				}
			}
		})
	}
}

func BenchmarkApocMapRemoveKeysShallow(b *testing.B) {
	for _, size := range []struct {
		name  string
		count int
	}{
		{"small", 8},
		{"large", 2048},
	} {
		b.Run(size.name, func(b *testing.B) {
			entries := make(map[string]interface{}, size.count)
			for index := 0; index < size.count; index++ {
				entries[fmt.Sprintf("k%04d", index)] = int64(index)
			}
			scope := cypherfn.Context{Eval: func(expression string) (interface{}, error) {
				switch expression {
				case "m":
					return entries, nil
				case "keys":
					return []interface{}{"k0000"}, nil
				}
				return map[string]interface{}{}, nil
			}}
			args := []string{"m", "keys", "config"}
			workCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := cypherfn.EvaluateFunction(workCtx, "apoc.map.removekeys", args, scope); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkApocMapConstructors(b *testing.B) {
	const count = 512
	entries := apocCancellationMap(count)
	pairs := make([]interface{}, 0, count)
	for index := 0; index < count; index++ {
		pairs = append(pairs, []interface{}{fmt.Sprintf("p%04d", index), int64(index)})
	}
	for _, tc := range []struct {
		name     string
		function string
		args     []string
		values   map[string]interface{}
	}{
		{"merge", "apoc.map.merge", []string{"left", "right"}, map[string]interface{}{"left": entries, "right": map[string]interface{}{"extra": int64(1)}}},
		{"clean", "apoc.map.clean", []string{"left", "keys", "items"}, map[string]interface{}{"left": entries, "keys": []interface{}{"absent"}, "items": []interface{}{}}},
		{"frompairs", "apoc.map.frompairs", []string{"pairs"}, map[string]interface{}{"pairs": pairs}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			scope := neo5FunctionContext(tc.values)
			workCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, found, err := cypherfn.EvaluateFunction(workCtx, tc.function, tc.args, scope); err != nil || !found {
					b.Fatalf("%s: found=%v err=%v", tc.function, found, err)
				}
			}
		})
	}
}

func BenchmarkCaseFunctions(b *testing.B) {
	for _, text := range []struct {
		name  string
		value string
	}{
		{"small", "Straße ǅ İstanbul"},
		{"large", strings.Repeat("Straße ǅ İstanbul ﬁ ", 256)},
	} {
		for _, name := range []string{"upper", "lower", "toupper", "tolower"} {
			b.Run(text.name+"/"+name, func(b *testing.B) {
				scope := neo5FunctionContext(map[string]interface{}{"v": text.value})
				workCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, found, err := cypherfn.EvaluateFunction(workCtx, name, []string{"v"}, scope); err != nil || !found {
						b.Fatalf("%s: found=%v err=%v", name, found, err)
					}
				}
			})
		}
	}
}

// BenchmarkApocMapCopyObservationCost measures the one part of the copy the
// convergence added: apocCopyInto observes the caller's statement context
// every apocCopyCancellationStride entries. Both sides run in this binary,
// alternating, so host load affects them alike; the plain loop is the copy
// the functions made before.
func BenchmarkApocMapCopyObservationCost(b *testing.B) {
	for _, size := range []struct {
		name  string
		count int
	}{
		{"small", 8},
		{"large", 2048},
	} {
		source := make(map[string]interface{}, size.count)
		for index := 0; index < size.count; index++ {
			source[fmt.Sprintf("k%04d", index)] = int64(index)
		}
		workCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		b.Run(size.name+"/plain", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := make(map[string]interface{}, len(source))
				for key, value := range source {
					out[key] = value
				}
			}
		})
		b.Run(size.name+"/observed", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := make(map[string]interface{}, len(source))
				if err := apocCopyInto(workCtx, out, source); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
