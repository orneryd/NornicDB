package cypher

// BenchmarkStoredLinkLookup times the stored-link shape C on both parsers
// with one fresh explicit transaction per iteration. Fixture construction,
// BEGIN and ROLLBACK stay outside the timer; only the query's full result
// consumption is timed, and every iteration asserts the exact six-column row.
// ReportAllocs is on; CPU labels include timer bookkeeping, so attribution
// must inspect actual executor/storage frames. The sub-benchmark names and the profile labels
// record the fixture history (sequential CreateNode/CreateEdge) and
// configuration (in-memory native Badger behind personal_documents). The
// measured read lifecycle is benchmarkStatementRead.
//
// Run only this manual benchmark with fixed iteration counts, for example:
//
//   go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' -bench '^BenchmarkStoredLinkLookup$' -benchmem -benchtime=100x -count=3 -timeout=10m
//
// Parser and degree are slash-separated selectors. A fixture is rebuilt for
// each parser/degree/calibration run; testing.B runs its cleanup after runN.

import (
	"fmt"
	"runtime/pprof"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkStoredLinkLookup(b *testing.B) {
	for _, parser := range relationshipEqualityParsers {
		b.Run("history=sequential-create-node-create-edge/configuration=badger-in-memory+personal_documents/parser="+parser.name, func(b *testing.B) {
			b.Cleanup(parser.setup())
			for _, degree := range []int{1000, 16000} {
				b.Run(fmt.Sprintf("degree=%d/explicit=true", degree), func(b *testing.B) {
					fixture := newStoredLinkFixture(b, degree)
					params := []map[string]interface{}{storedLinkParams()}
					expected := storedLinkExpectedRows()
					benchmarkStatementRead(b, fixture.exec, storedLinkStatement, params, true,
						pprof.Labels("phase", "stored-link-query",
							"history", "sequential-create-node-create-edge",
							"configuration", "badger-in-memory+personal_documents",
							"parser", parser.name, "degree", fmt.Sprintf("%d", degree)),
						func(result *ExecuteResult) { require.Equal(b, expected, result.Rows) })
				})
			}
		})
	}
}
