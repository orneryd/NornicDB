package bolt

import (
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Stored numbers compare exactly in Cypher, integers beyond 2^53 and floats
// included. An index must return exactly what the same query returns without
// it: equality seeks, strict and inclusive bounds and ORDER BY ... LIMIT
// around 2^53 and 2^63, with integer and float values and bounds.
func TestNumericPropertyIndexExactAroundFloatPrecision(t *testing.T) {
	values := []any{
		int64(9007199254740991), int64(9007199254740992), int64(9007199254740993),
		int64(9007199254740994), int64(9007199254740995), int64(-9007199254740993),
		int64(-9007199254740992), int64(9223372036854775806), int64(9223372036854775807),
		float64(9007199254740996), float64(9223372036854775808), float64(-9007199254740994), 1.5,
	}
	bounds := []any{
		int64(9007199254740992), int64(9007199254740993), float64(9007199254740992),
		int64(-9007199254740993), int64(9223372036854775807), float64(9223372036854775808),
	}
	for _, index := range []struct{ name, ddl string }{
		{"no_index", ""},
		{"range_index", "CREATE RANGE INDEX precise_id FOR (n:PreciseNumber) ON (n.id)"},
		{"index", "CREATE INDEX precise_id FOR (n:PreciseNumber) ON (n.id)"},
		{"unique", "CREATE CONSTRAINT precise_id FOR (n:PreciseNumber) REQUIRE n.id IS UNIQUE"},
	} {
		t.Run(index.name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "test"))
			conn := openBoltTestConn(t, port)
			defer conn.Close()
			if index.ddl != "" {
				runBoltQueryAndCollectRecords(t, conn, index.ddl)
			}
			for k, value := range values {
				runBoltQueryWithParams(t, conn, "CREATE (:PreciseNumber {k:$k, id:$id})", map[string]any{"k": int64(k), "id": value})
			}
			for _, bound := range bounds {
				for _, op := range []string{"=", ">", ">=", "<", "<="} {
					query := fmt.Sprintf("MATCH (n:PreciseNumber) WHERE n.id %s $b RETURN n.k", op)
					got := sortedKeys(runBoltQueryWithParams(t, conn, query, map[string]any{"b": bound}))
					var want []int64
					for k, value := range values {
						if exactNumberOp(value, op, bound) {
							want = append(want, int64(k))
						}
					}
					if !reflect.DeepEqual(got, want) {
						t.Errorf("n.id %s %v (%T): got k=%v, want %v", op, bound, bound, got, want)
					}
				}
			}
			lo := int64(-9007199254740993)
			var ascending []int64
			for k, value := range values {
				if exactNumberOp(value, ">=", lo) {
					ascending = append(ascending, int64(k))
				}
			}
			sort.Slice(ascending, func(i, j int) bool {
				return exactNumberCompare(values[ascending[i]], values[ascending[j]]) < 0
			})
			for _, direction := range []string{"ASC", "DESC"} {
				query := "MATCH (n:PreciseNumber) WHERE n.id >= $lo RETURN n.k ORDER BY n.id " + direction + " LIMIT 4"
				var got []int64
				for _, record := range runBoltQueryWithParams(t, conn, query, map[string]any{"lo": lo}) {
					got = append(got, record[0].(int64))
				}
				want := append([]int64(nil), ascending[:4]...)
				if direction == "DESC" {
					want = want[:0]
					for i := len(ascending) - 1; i >= len(ascending)-4; i-- {
						want = append(want, ascending[i])
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("ORDER BY n.id %s LIMIT 4: got k=%v, want %v", direction, got, want)
				}
			}
			// A pending node in an explicit transaction is placed by the same
			// exact key: strictly above the float 2^53+4 bucket's integer neighbour.
			requireNoError(t, SendBegin(t, conn, nil))
			requireNoError(t, ReadSuccess(t, conn))
			pendingValue, pendingBound := int64(9007199254740997), int64(9007199254740996)
			runBoltQueryWithParams(t, conn, "CREATE (:PreciseNumber {k:50, id:$id})", map[string]any{"id": pendingValue})
			got := sortedKeys(runBoltQueryWithParams(t, conn, "MATCH (n:PreciseNumber) WHERE n.id > $b RETURN n.k", map[string]any{"b": pendingBound}))
			var want []int64
			for k, value := range values {
				if exactNumberOp(value, ">", pendingBound) {
					want = append(want, int64(k))
				}
			}
			want = append(want, 50)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("in-transaction n.id > %d: got k=%v, want %v", pendingBound, got, want)
			}
			requireNoError(t, SendRollback(t, conn))
			requireNoError(t, ReadSuccess(t, conn))
			if index.name != "unique" {
				return
			}
			// UNIQUE keys the adjacent integers apart: MERGE matches 2^53+1
			// itself, and only a true duplicate is rejected.
			odd := map[string]any{"id": int64(9007199254740993)}
			if records := runBoltQueryWithParams(t, conn, "MERGE (n:PreciseNumber {id:$id}) RETURN n.k", odd); len(records) != 1 || records[0][0] != int64(2) {
				t.Errorf("MERGE of 2^53+1 must match k=2, got %v", records)
			}
			duplicate := openBoltTestConn(t, port)
			defer duplicate.Close()
			requireNoError(t, SendRun(t, duplicate, "CREATE (:PreciseNumber {k:99, id:$id})", odd, nil))
			if _, _, err := AssertFailure(t, duplicate); err != nil {
				t.Errorf("duplicate 2^53+1 must be rejected: %v", err)
			}
		})
	}
}

func sortedKeys(records [][]any) []int64 {
	var keys []int64
	for _, record := range records {
		keys = append(keys, record[0].(int64))
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func exactNumber(value any) *big.Float {
	switch n := value.(type) {
	case int64:
		return new(big.Float).SetInt64(n)
	case float64:
		return big.NewFloat(n)
	}
	panic(fmt.Sprintf("unexpected %T", value))
}

func exactNumberCompare(a, b any) int { return exactNumber(a).Cmp(exactNumber(b)) }

func exactNumberOp(value any, op string, bound any) bool {
	c := exactNumberCompare(value, bound)
	switch op {
	case "=":
		return c == 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	case "<":
		return c < 0
	default:
		return c <= 0
	}
}
