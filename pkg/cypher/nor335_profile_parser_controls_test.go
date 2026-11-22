package cypher

// NOR-335 both-parser controls for the PROFILE/EXPLAIN contracts that already
// have single-executor owners.
//
// The comment-derived scope of NOR-335 states that the PROFILE counters and the
// EXPLAIN index applicability decision were qualified on one parser setting.
// `profile_explain_metrics_public_test.go` owns both contracts through the
// public StorageExecutor.Execute path, so those owners stay unchanged; this file
// only closes the coverage gap by pinning the same public contract on the
// default and the ANTLR parser setting and requiring the two settings to agree.
//
// No new contract, column set, metric or plan shape is asserted here, and no
// production code is touched. Every case goes through StorageExecutor.Execute,
// the entry point the HTTP and Bolt routes use.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor335ParserExecutor returns a fresh executor over an isolated namespace, so
// each parser setting starts from the same empty schema and graph.
func nor335ParserExecutor(t *testing.T, namespace string) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace))
}

// nor335PlanSignature renders the measured facts both parsers must share: the
// operator sequence plus the metrics the contract states.
func nor335PlanSignature(plan *ExecutionPlan) string {
	return fmt.Sprintf("operators=%s|rows=%d|rootRows=%d|hits=%d|rootHits=%d",
		strings.Join(collectOperatorTypes(plan.Root), ">"),
		plan.TotalRows, plan.Root.ActualRows, plan.TotalDBHits, plan.Root.DBHits)
}

// TestNOR335ProfileConstantCountersHoldOnBothParsers pins the reported
// constant-projection contract on both parser settings: `PROFILE RETURN 1`
// produces one row and performs no storage read, so every hit counter is zero
// and the rendered plan says so.
func TestNOR335ProfileConstantCountersHoldOnBothParsers(t *testing.T) {
	signatures := map[string]string{}
	forEachParser(t, func(t *testing.T) {
		exec := nor335ParserExecutor(t, "nor335_profile_constant")

		// The namespace holds no node, relationship, index or property, so no
		// positive hit count could be a measurement.
		result, err := exec.Execute(t.Context(), `PROFILE RETURN 1`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		plan := executionPlanFromResult(t, result)
		require.Equal(t, ModeProfile, plan.Mode)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(1), plan.TotalRows, "the statement produces exactly one row")
		require.Equal(t, int64(1), plan.Root.ActualRows)
		require.Equal(t, int64(0), plan.TotalDBHits,
			"a constant projection performs no storage read, so total hits are a measured zero")
		require.Equal(t, int64(0), plan.Root.DBHits)
		require.Contains(t, planStringFromResult(t, result), "Total DB Hits: 0")

		signatures[config.GetParserType()] = nor335PlanSignature(plan)
	})

	require.Len(t, signatures, 2)
	require.Equal(t, signatures[config.ParserTypeNornic], signatures[config.ParserTypeANTLR],
		"both parser settings must report the same measured constant profile")
}

// TestNOR335ProfileScanCountersHoldOnBothParsers pins the reported
// per-operator row-count contract on both parser settings: a label scan over
// five nodes that a WHERE clause reduces to one row must report five rows on
// the scan, one row on the filter, and at least one storage read per scanned
// node — not the statement's final row count copied onto every operator.
func TestNOR335ProfileScanCountersHoldOnBothParsers(t *testing.T) {
	signatures := map[string]string{}
	forEachParser(t, func(t *testing.T) {
		exec := nor335ParserExecutor(t, "nor335_profile_scan")
		for _, age := range []int{30, 31, 32, 33, 40} {
			_, err := exec.Execute(t.Context(), `CREATE (:Person {age: $age})`,
				map[string]interface{}{"age": age})
			require.NoError(t, err)
		}

		result, err := exec.Execute(t.Context(),
			`PROFILE MATCH (n:Person) WHERE n.age > 34 RETURN n`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		plan := executionPlanFromResult(t, result)
		require.Equal(t, ModeProfile, plan.Mode)

		scan := planOperatorOfType(plan.Root, "NodeByLabelScan")
		require.NotNil(t, scan, "the plan contains the label scan it executed")
		filter := planOperatorOfType(plan.Root, "Filter")
		require.NotNil(t, filter, "the plan contains the WHERE filter it executed")

		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(1), plan.TotalRows)
		require.Equal(t, int64(1), plan.Root.ActualRows)
		require.Equal(t, int64(5), scan.ActualRows,
			"the label scan emits one row per :Person node, not the final row count")
		require.Equal(t, int64(1), filter.ActualRows,
			"the filter emits the single row that satisfies WHERE")
		require.GreaterOrEqual(t, scan.DBHits, int64(5),
			"reading five nodes is at least five storage reads on the scan")
		require.GreaterOrEqual(t, plan.TotalDBHits, int64(5),
			"a five-node label scan must not report a zero or final-row hit count")

		signatures[config.GetParserType()] = nor335PlanSignature(plan)
	})

	require.Len(t, signatures, 2)
	require.Equal(t, signatures[config.ParserTypeNornic], signatures[config.ParserTypeANTLR],
		"both parser settings must report the same measured scan and filter rows")
}

// nor335ExplainCase is one index-applicability scene: the schema state, the
// explained statement, and whether an applicable index exists.
type nor335ExplainCase struct {
	name  string
	setup []string
	query string
	// seek is the label of the index an applicable seek must report, or "" when
	// no index can serve the statement and the plan must scan instead.
	seek string
}

// TestNOR335ExplainIndexApplicabilityHoldsOnBothParsers pins the reported
// EXPLAIN applicability decision on both parser settings. The three scenes are
// the contract's own: no index for the label, an index for a different
// property, and an applicable index reached through a property map or a WHERE
// equality. The positive scenes guard against satisfying the negative ones by
// never reporting a seek.
func TestNOR335ExplainIndexApplicabilityHoldsOnBothParsers(t *testing.T) {
	cases := []nor335ExplainCase{
		{
			name:  "no index for the label",
			query: `EXPLAIN MATCH (n:Person {name: 'Ada'}) RETURN n`,
		},
		{
			name:  "index on a different property",
			setup: []string{`CREATE INDEX nor335_misfit FOR (n:Person) ON (n.name)`},
			query: `EXPLAIN MATCH (n:Person {age: 30}) RETURN n`,
		},
		{
			name:  "applicable index for a property map",
			setup: []string{`CREATE INDEX nor335_map FOR (n:Person) ON (n.name)`},
			query: `EXPLAIN MATCH (n:Person {name: 'Ada'}) RETURN n`,
			seek:  "Person",
		},
		{
			name:  "applicable index for a WHERE equality",
			setup: []string{`CREATE INDEX nor335_where FOR (n:Person) ON (n.name)`},
			query: `EXPLAIN MATCH (n:Person) WHERE n.name = 'Ada' RETURN n`,
			seek:  "Person",
		},
	}

	signatures := map[string]map[string]string{}
	forEachParser(t, func(t *testing.T) {
		parser := config.GetParserType()
		signatures[parser] = map[string]string{}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				exec := nor335ParserExecutor(t, "nor335_explain_applicability")
				for _, statement := range tc.setup {
					_, err := exec.Execute(t.Context(), statement, nil)
					require.NoError(t, err)
				}

				result, err := exec.Execute(t.Context(), tc.query, nil)
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Empty(t, result.Rows, "EXPLAIN runs no statement")

				plan := executionPlanFromResult(t, result)
				require.Equal(t, ModeExplain, plan.Mode)

				operatorTypes := strings.Join(collectOperatorTypes(plan.Root), ">")
				seek := planOperatorOfType(plan.Root, "NodeIndexSeek")
				if tc.seek == "" {
					require.NotContains(t, collectOperatorTypes(plan.Root), "NodeIndexSeek",
						"no applicable index exists, so no seek may be claimed")
					require.NotNil(t, planOperatorOfType(plan.Root, "NodeByLabelScan"),
						"without an applicable index the plan scans the label")
					signatures[parser][tc.name] = operatorTypes
					return
				}

				require.NotNil(t, seek, "an applicable index must plan an index seek")
				require.Equal(t, tc.seek, seek.Arguments["label"])
				require.Equal(t, "available", seek.Arguments["indexStatus"],
					"the seek is legitimate only while the schema reports an available index")
				signatures[parser][tc.name] = fmt.Sprintf("%s|indexStatus=%v",
					operatorTypes, seek.Arguments["indexStatus"])
			})
		}
	})

	require.Len(t, signatures, 2)
	for _, tc := range cases {
		require.Equal(t, signatures[config.ParserTypeNornic][tc.name],
			signatures[config.ParserTypeANTLR][tc.name],
			"both parser settings must reach the same applicability decision: %s", tc.name)
	}
}
