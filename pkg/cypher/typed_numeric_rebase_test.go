package cypher

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

// These consumers were added by the release base during the rebase. Their
// public contracts retain the upstream null, byte-array and GDS policies.
func TestTypedNumericRebasedMapConfig(t *testing.T) {
	exec, store := typedNumericExecutor(t)
	for _, input := range listinputs.NumericInputs() {
		t.Run(input.Name, func(t *testing.T) {
			value := reflect.ValueOf(input.Scalar)
			recursive := true
			switch value.Kind() {
			case reflect.String:
				recursive = value.String() != "" && value.String() != "0"
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				recursive = value.Int() != 0
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				recursive = value.Uint() != 0
			case reflect.Float32, reflect.Float64:
				n := value.Float()
				recursive = !math.IsNaN(n) && (n <= -1 || n >= 1)
			}
			want := map[string]interface{}{"outer": map[string]interface{}{"drop": int64(1), "keep": int64(2)}}
			if recursive {
				delete(want["outer"].(map[string]interface{}), "drop")
			}
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"RebasedNumeric"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.Property}})
			require.NoError(t, err)
			params := map[string]interface{}{"xs": input.List, "boxed": []interface{}{input.Scalar}, "name": input.Name}
			for _, form := range []struct{ prefix, list string }{
				{"", "$xs"}, {"", "$boxed"}, {"WITH $xs AS xs ", "xs"},
				{"MATCH (n:RebasedNumeric {name:$name}) ", "n.xs"},
			} {
				query := form.prefix + "RETURN apoc.map.removeKeys({outer:{drop:1,keep:2}},['drop'],{recursive:head(" + form.list + ")})"
				require.Equal(t, want, typedNumericRead(t, exec, query, params), query)
			}
		})
	}
}

func TestTypedNumericRebasedDigestBoundaries(t *testing.T) {
	exec, store := typedNumericExecutor(t)
	for i, tc := range []struct {
		value interface{}
		text  string
	}{
		{int64(math.MinInt64), "-9223372036854775808"},
		{int64(math.MaxInt64), "9223372036854775807"},
		{uint64(math.MaxInt64 + 1), "9223372036854775808"},
		{uint64(math.MaxUint64), "18446744073709551615"},
		{float32(1.5), "1.5"}, {math.NaN(), "NaN"},
		{math.Inf(1), "Infinity"}, {math.Inf(-1), "-Infinity"},
		{math.SmallestNonzeroFloat64, "4.9E-324"},
	} {
		t.Run(fmt.Sprintf("%T/%d", tc.value, i), func(t *testing.T) {
			list := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(tc.value)), 1, 1)
			list.Index(0).Set(reflect.ValueOf(tc.value))
			name := fmt.Sprint(i)
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(name), Labels: []string{"RebasedDigest"}, Properties: map[string]interface{}{"name": name, "xs": []interface{}{tc.value}}})
			require.NoError(t, err)
			for _, nameAndHash := range []struct{ name, hash string }{
				{"md5", func() string { sum := md5.Sum([]byte(tc.text)); return hex.EncodeToString(sum[:]) }()},
				{"sha1", func() string { sum := sha1.Sum([]byte(tc.text)); return hex.EncodeToString(sum[:]) }()},
				{"sha256", func() string { sum := sha256.Sum256([]byte(tc.text)); return hex.EncodeToString(sum[:]) }()},
			} {
				params := map[string]interface{}{"xs": list.Interface(), "boxed": []interface{}{tc.value}, "name": name}
				for _, form := range []struct{ prefix, list string }{
					{"", "$xs"}, {"", "$boxed"}, {"WITH $xs AS xs ", "xs"},
					{"MATCH (n:RebasedDigest {name:$name}) ", "n.xs"},
					{"WITH {items:$xs} AS box ", "box.items"},
				} {
					query := form.prefix + "RETURN apoc.util." + nameAndHash.name + "(" + form.list + ")"
					require.Equal(t, nameAndHash.hash, typedNumericRead(t, exec, query, params), query)
				}
			}
		})
	}
}

func TestTypedNumericRuntimeMapRetainsTemporalAdmission(t *testing.T) {
	exec, _ := typedNumericExecutor(t)
	instant := time.Date(2026, time.October, 20, 1, 2, 3, 0, time.UTC)
	for _, value := range []interface{}{instant, &instant} {
		got := typedNumericRead(t, exec, "WITH {v:$value} AS box RETURN box.v", map[string]interface{}{"value": value})
		require.Equal(t, dateTimeFromGoTime(instant), got)
	}
}
