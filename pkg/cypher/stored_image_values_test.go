package cypher

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Storage now publishes every written body as the codec image (the Go API
// behaviour change). For every value kind, as a literal or as an embedded
// caller's Go parameter, a MATCH right after the write returns exactly what a
// MATCH after a restart returns, and the same value the CREATE returned from
// the body it wrote: only a list's Go container type (the typed slice storage
// decodes) and a custom zone's name (its offset kept) may differ in-process,
// and neither reaches Bolt or HTTP (TestProtocolValuesUnchangedByStoredImage).
func TestCypherValuesUnchangedByStoredImage(t *testing.T) {
	zone := time.FixedZone("plus-one", 3600)
	params := map[string]any{
		"int": 30, "int32": int32(30), "int64": int64(30), "uint8": uint8(30), "float32": float32(0.5), "float64": 0.5,
		"string": "x", "bool": true, "go_ints": []any{1, 2}, "int64s": []any{int64(1), int64(2)}, "strings": []string{"a", "b"},
		"mixed": []any{int64(1), 2.5}, "floats": []any{0.5, 1.5}, "typed_ints": []int64{1, 2}, "time": time.Date(2024, 1, 2, 3, 4, 5, 6, zone),
	}
	literals := map[string]string{
		"l_int": "30", "l_float": "0.5", "l_strings": "['a', 'b']", "l_ints": "[1, 2]", "l_mixed": "[1, 2.5]",
		"l_floats": "[0.5, 1.5]", "l_bools": "[true, false]", "l_empty": "[]", "l_date": "date('2024-01-02')",
		"l_datetime": "datetime('2024-01-02T03:04:05.000000006+01:00')", "l_duration": "duration('P1DT2H')",
		"l_point": "point({x: 1.5, y: 2.5})",
	}
	var names []string
	set := ""
	for name := range params {
		names = append(names, name)
		set += fmt.Sprintf(", %s: $%s", name, name)
	}
	for name, literal := range literals {
		names = append(names, name)
		set += fmt.Sprintf(", %s: %s", name, literal)
	}
	returns := ""
	for i, name := range names {
		if i > 0 {
			returns += ", "
		}
		returns += "n." + name
	}
	previousParser := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previousParser) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, buffered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/write-behind=%v", parser, buffered), func(t *testing.T) {
				config.SetParserType(parser)
				dir := t.TempDir()
				open := func() (*storage.BadgerEngine, *StorageExecutor) {
					base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour})
					require.NoError(t, err)
					return base, NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
				}
				base, exec := open()
				ctx := context.Background()
				created, err := exec.Execute(ctx, "CREATE (n:K {id: 'n'"+set+"}) RETURN "+returns, params)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, "MATCH (a:K {id: 'n'}) CREATE (a)-[:LINK {id: 'r'"+set+"}]->(:E)", params)
				require.NoError(t, err)
				relReturns := ""
				for i, name := range names {
					if i > 0 {
						relReturns += ", "
					}
					relReturns += "r." + name
				}
				matchNode := "MATCH (n:K {id: 'n'}) RETURN " + returns
				matchRel := "MATCH ()-[r:LINK {id: 'r'}]->() RETURN " + relReturns
				afterNode, err := exec.Execute(ctx, matchNode, nil)
				require.NoError(t, err)
				afterRel, err := exec.Execute(ctx, matchRel, nil)
				require.NoError(t, err)
				require.NoError(t, base.Close())

				base, exec = open()
				t.Cleanup(func() { _ = base.Close() })
				restartNode, err := exec.Execute(ctx, matchNode, nil)
				require.NoError(t, err)
				restartRel, err := exec.Execute(ctx, matchRel, nil)
				require.NoError(t, err)
				require.Len(t, created.Rows, 1)
				for i, name := range names {
					require.Equal(t, restartNode.Rows[0][i], afterNode.Rows[0][i], "node %s: MATCH after write vs after restart", name)
					require.Equal(t, restartRel.Rows[0][i], afterRel.Rows[0][i], "relationship %s: MATCH after write vs after restart", name)
					require.True(t, sameCypherValue(created.Rows[0][i], afterNode.Rows[0][i]), "node %s: CREATE %#v vs MATCH %#v", name, created.Rows[0][i], afterNode.Rows[0][i])
					require.True(t, sameCypherValue(created.Rows[0][i], afterRel.Rows[0][i]), "relationship %s: CREATE %#v vs MATCH %#v", name, created.Rows[0][i], afterRel.Rows[0][i])
				}
			})
		}
	}
}

// sameCypherValue reports whether a and b are the same Cypher value: equal
// numbers of the same family (integer or float), lists element by element
// whatever their Go container type, maps key by key, and times at the same
// instant and offset.
func sameCypherValue(a, b any) bool {
	if ta, ok := a.(time.Time); ok {
		tb, ok := b.(time.Time)
		_, offsetA := ta.Zone()
		_, offsetB := tb.Zone()
		return ok && ta.Equal(tb) && offsetA == offsetB
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if !va.IsValid() || !vb.IsValid() {
		return va.IsValid() == vb.IsValid()
	}
	switch {
	case va.CanInt() || va.CanUint():
		if !vb.CanInt() && !vb.CanUint() {
			return false
		}
		return fmt.Sprint(a) == fmt.Sprint(b)
	case va.CanFloat():
		return vb.CanFloat() && va.Float() == vb.Float()
	case va.Kind() == reflect.Slice && vb.Kind() == reflect.Slice:
		if va.Len() != vb.Len() {
			return false
		}
		for i := 0; i < va.Len(); i++ {
			if !sameCypherValue(va.Index(i).Interface(), vb.Index(i).Interface()) {
				return false
			}
		}
		return true
	case va.Kind() == reflect.Map && vb.Kind() == reflect.Map:
		if va.Len() != vb.Len() {
			return false
		}
		for _, key := range va.MapKeys() {
			other := vb.MapIndex(key)
			if !other.IsValid() || !sameCypherValue(va.MapIndex(key).Interface(), other.Interface()) {
				return false
			}
		}
		return true
	case va.Kind() == reflect.Struct && va.Type() == vb.Type():
		for i := 0; i < va.NumField(); i++ {
			if !va.Type().Field(i).IsExported() {
				return reflect.DeepEqual(a, b)
			}
			if !sameCypherValue(va.Field(i).Interface(), vb.Field(i).Interface()) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}
