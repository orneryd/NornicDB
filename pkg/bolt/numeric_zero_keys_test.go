package bolt

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// -0.0, +0.0 and integer 0 are one value and NaN equals nothing: every index
// kind answers equality and range bounds as the scan does.
func TestNumericZeroAndNaNIndexKeys(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	values := []any{negativeZero, 0.0, int64(0), math.NaN(), int64(1)}
	zeros := []any{negativeZero, 0.0, int64(0)}
	for _, index := range []struct{ name, ddl, equality string }{
		{"no_index", "", "MATCH (n:ZeroKey) WHERE n.v = $b RETURN n.k"},
		{"range_index", "CREATE RANGE INDEX zero_v FOR (n:ZeroKey) ON (n.v)", "MATCH (n:ZeroKey) WHERE n.v = $b RETURN n.k"},
		{"index", "CREATE INDEX zero_v FOR (n:ZeroKey) ON (n.v)", "MATCH (n:ZeroKey) WHERE n.v = $b RETURN n.k"},
		{"composite_index", "CREATE INDEX zero_tv FOR (n:ZeroKey) ON (n.tenant, n.v)", "MATCH (n:ZeroKey {tenant:7, v:$b}) RETURN n.k"},
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
				runBoltQueryWithParams(t, conn, "CREATE (:ZeroKey {k:$k, tenant:7, v:$v})", map[string]any{"k": int64(k), "v": value})
			}
			for _, bound := range zeros {
				if got := sortedKeys(runBoltQueryWithParams(t, conn, index.equality, map[string]any{"b": bound})); !reflect.DeepEqual(got, []int64{0, 1, 2}) {
					t.Errorf("equality with %v (%T, signbit %v): got k=%v, want [0 1 2]", bound, bound, math.Signbit(toFloat(bound)), got)
				}
				for _, tc := range []struct {
					op   string
					want []int64
				}{{">", []int64{4}}, {">=", []int64{0, 1, 2, 4}}, {"<=", []int64{0, 1, 2}}, {"<", nil}} {
					query := fmt.Sprintf("MATCH (n:ZeroKey) WHERE n.v %s $b RETURN n.k", tc.op)
					if got := sortedKeys(runBoltQueryWithParams(t, conn, query, map[string]any{"b": bound})); !reflect.DeepEqual(got, tc.want) {
						t.Errorf("n.v %s %v (%T): got k=%v, want %v", tc.op, bound, bound, got, tc.want)
					}
				}
			}
			if got := sortedKeys(runBoltQueryWithParams(t, conn, index.equality, map[string]any{"b": math.NaN()})); len(got) != 0 {
				t.Errorf("NaN equals nothing, got k=%v", got)
			}
		})
	}
}

// A composite UNIQUE / NODE KEY tuple stored with -0.0 is the tuple looked up,
// merged or duplicated with integer 0 or +0.0, including MERGE admission of a
// transaction that read before the tuple's commit.
func TestCompositeKeyZeroSigns(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	large := int64(9007199254740993)
	for _, kind := range []string{"UNIQUE", "NODE KEY"} {
		t.Run(kind, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "test"))
			peer := openBoltTestConn(t, port)
			defer peer.Close()
			runBoltQueryAndCollectRecords(t, peer, "CREATE CONSTRAINT zero_tuple FOR (n:ZeroTuple) REQUIRE (n.tenant, n.v) IS "+kind)
			loser := openBoltTestConn(t, port)
			defer loser.Close()
			requireNoError(t, SendBegin(t, loser, nil))
			requireNoError(t, ReadSuccess(t, loser))
			runBoltQueryAndCollectRecords(t, loser, "CREATE (:AdmissionAudit {id:'loser'})")
			runBoltQueryWithParams(t, peer, "CREATE (:ZeroTuple {tenant:$a, v:$v, tag:7})", map[string]any{"a": negativeZero, "v": large})
			merge := "MERGE (n:ZeroTuple {tenant:$a, v:$v}) RETURN n.tag"
			requireNoError(t, SendRun(t, loser, merge, map[string]any{"a": int64(0), "v": large}, nil))
			code, message, err := AssertFailure(t, loser)
			requireNoError(t, err)
			if code != "Neo.TransientError.Transaction.Outdated" {
				t.Errorf("MERGE with integer 0 behind a -0.0 tuple: got %s (%s), want retryable Outdated", code, message)
			}
			for _, tenant := range []any{int64(0), 0.0, negativeZero} {
				params := map[string]any{"a": tenant, "v": large}
				if records := runBoltQueryWithParams(t, peer, "MATCH (n:ZeroTuple {tenant:$a, v:$v}) RETURN n.tag", params); len(records) != 1 || records[0][0] != int64(7) {
					t.Errorf("lookup with tenant %v (%T): got %v, want [[7]]", tenant, tenant, records)
				}
				if records := runBoltQueryWithParams(t, peer, merge, params); len(records) != 1 || records[0][0] != int64(7) {
					t.Errorf("MERGE with tenant %v (%T): got %v, want the stored tag 7", tenant, tenant, records)
				}
			}
			if records := runBoltQueryAndCollectRecords(t, peer, "MATCH (n:ZeroTuple) RETURN count(n)"); len(records) != 1 || records[0][0] != int64(1) {
				t.Errorf("MERGE must not create a second zero tuple, got %v", records)
			}
			duplicate := openBoltTestConn(t, port)
			defer duplicate.Close()
			requireNoError(t, SendRun(t, duplicate, "CREATE (:ZeroTuple {tenant:$a, v:$v, tag:8})", map[string]any{"a": int64(0), "v": large}, nil))
			if _, _, err := AssertFailure(t, duplicate); err != nil {
				t.Errorf("integer 0 duplicates the -0.0 tuple: %v", err)
			}
		})
	}
}

func toFloat(value any) float64 {
	switch n := value.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	}
	return math.NaN()
}
