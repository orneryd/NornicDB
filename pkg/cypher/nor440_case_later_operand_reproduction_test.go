package cypher

// NOR-440 reproduction: an arithmetic expression whose CASE … END is not the
// first operand and is not in parentheses, where the CASE holds an operator
// outside its own nested parentheses or brackets.
//
// These tests are reproduction only: they assert the expected product
// contract (what Neo4j 5.26 answers) through the public executor
// (StorageExecutor.Execute), on both parsers. They are expected to fail while
// the defect is present; none of them pins the current answer, and a failure
// message reports the observed error so the failing class stays visible.
//
// The rows are the measured trigger family from the issue (a + or - anywhere
// inside the CASE, whatever the outer operator, because the +- tier is scanned
// before any other tier; a * / % whose outer operator is one of those; a ^
// whose outer operator is ^), the expression contexts the issue enumerates,
// and the healthy controls the issue records as working.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/caseinputs"
	"github.com/stretchr/testify/require"
)

// nor440Executor builds a fresh executor; every case runs against its own
// engine so mutations cannot leak between rows or parsers.
func nor440Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor440"))
}

// nor440Run executes one matrix row and returns its rows.
func nor440Run(t *testing.T, exec *StorageExecutor, tc caseinputs.Case) ([][]interface{}, error) {
	t.Helper()
	ctx := context.Background()
	for _, statement := range tc.Setup {
		if _, err := exec.Execute(ctx, statement, nil); err != nil {
			t.Fatalf("%s: setup %q failed: %v", tc.ID, statement, err)
		}
	}
	result, err := exec.Execute(ctx, tc.Query, tc.Params)
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

// nor440Assert executes one matrix row and asserts the product contract: the
// statement succeeds and answers the value Neo4j 5.26 answers.
func nor440Assert(t *testing.T, exec *StorageExecutor, tc caseinputs.Case, parser string) {
	t.Helper()
	rows, err := nor440Run(t, exec, tc)
	require.NoErrorf(t, err, "%s [parser=%s]: %s", tc.ID, parser, tc.Query)
	if tc.WantRows != nil {
		require.Equalf(t, tc.WantRows, rows, "%s [parser=%s]: %s", tc.ID, parser, tc.Query)
		return
	}
	require.Lenf(t, rows, 1, "%s [parser=%s]: %s", tc.ID, parser, tc.Query)
	require.Lenf(t, rows[0], 1, "%s [parser=%s]: %s", tc.ID, parser, tc.Query)
	require.Equalf(t, tc.Want, rows[0][0], "%s [parser=%s]: %s", tc.ID, parser, tc.Query)
}

// TestNOR440CaseLaterOperandTriggersEmbedded measures the whole measured
// trigger family through the embedded executor on both parsers.
func TestNOR440CaseLaterOperandTriggersEmbedded(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		parser := parser
		t.Run(parser, func(t *testing.T) {
			restoreParserTypeAfter(t)
			config.SetParserType(parser)
			require.Equal(t, parser, config.GetParserType())
			for _, tc := range caseinputs.Cases() {
				if tc.Class != "trigger" {
					continue
				}
				tc := tc
				t.Run(tc.ID, func(t *testing.T) {
					nor440Assert(t, nor440Executor(t), tc, parser)
				})
			}
		})
	}
}

// TestNOR440CaseLaterOperandControlsEmbedded guards the healthy forms the issue
// records as working: they must answer the Neo4j result on both parsers.
func TestNOR440CaseLaterOperandControlsEmbedded(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		parser := parser
		t.Run(parser, func(t *testing.T) {
			restoreParserTypeAfter(t)
			config.SetParserType(parser)
			require.Equal(t, parser, config.GetParserType())
			for _, tc := range caseinputs.Cases() {
				if tc.Class != "control" {
					continue
				}
				tc := tc
				t.Run(tc.ID, func(t *testing.T) {
					nor440Assert(t, nor440Executor(t), tc, parser)
				})
			}
		})
	}
}
