package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Constraint contracts order integers above 2^53 exactly, consistently with
// exact constraint equality: inclusive and strict bounds accept the adjacent
// valid value and reject the adjacent invalid one, for node and relationship
// contracts, on write and on backfill.
func TestConstraintContractOrderingIntegerPrecision(t *testing.T) {
	const even, odd = int64(9007199254740992), int64(9007199254740993)
	for _, relationship := range []bool{false, true} {
		for _, backfill := range []bool{false, true} {
			for _, tc := range []struct {
				op        string
				threshold int64
				value     int64
				valid     bool
			}{
				{">=", even, odd, true},
				{"<=", odd, even, true},
				{">", even, odd, true},
				{"<", odd, even, true},
				{">=", odd, even, false},
				{"<=", even, odd, false},
				{">", odd, odd, false},
				{"<", even, even, false},
			} {
				t.Run(fmt.Sprintf("relationship=%v/backfill=%v/%d%s%d", relationship, backfill, tc.value, tc.op, tc.threshold), func(t *testing.T) {
					base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
					t.Cleanup(func() { _ = base.Close() })
					_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "test"))
					conn := openBoltTestConn(t, port)
					defer conn.Close()
					pattern, create, variable := "(n:ContractOrdered)", "CREATE (:ContractOrdered {id:$id})", "n"
					if relationship {
						pattern, create, variable = "()-[r:CONTRACT_ORDERED]-()", "CREATE ()-[:CONTRACT_ORDERED {id:$id}]->()", "r"
					}
					ddl := fmt.Sprintf("CREATE CONSTRAINT contract_order FOR %s REQUIRE { %s.id %s %d }", pattern, variable, tc.op, tc.threshold)
					params := map[string]any{"id": tc.value}
					first, second, secondParams := create, ddl, map[string]any(nil)
					firstParams := params
					if !backfill {
						first, second, firstParams, secondParams = ddl, create, nil, params
					}
					runBoltQueryWithParams(t, conn, first, firstParams)
					if tc.valid {
						runBoltQueryWithParams(t, conn, second, secondParams)
						return
					}
					requireNoError(t, SendRun(t, conn, second, secondParams, nil))
					if _, _, err := AssertFailure(t, conn); err != nil {
						t.Errorf("%d %s %d must be rejected: %v", tc.value, tc.op, tc.threshold, err)
					}
				})
			}
		}
	}
}
