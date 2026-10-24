package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltTransactionalCallKeepsCommittedBatches(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "tenant"))
	writer := openBoltTestConn(t, port)
	require.NoError(t, SendRun(t, writer, "UNWIND [1,2,0] AS x CALL { WITH x CREATE (:B {v: 10/x}) } IN TRANSACTIONS OF 1 ROW", nil, nil))
	_, _, err = AssertFailure(t, writer)
	require.NoError(t, err)
	reader := openBoltTestConn(t, port)
	rows := runBoltQueryAndCollectRecords(t, reader, "MATCH (n:B) RETURN n.v ORDER BY n.v")
	require.Equal(t, [][]interface{}{{int64(5)}, {int64(10)}}, rows)
}

func TestBoltTransactionalCallBatchSizeExpressionContract(t *testing.T) {
	cases := []struct {
		name, size string
		invalid    bool
		params     map[string]any
		errorCode  string
	}{
		{"parameter_rows", "$n ROWS", false, map[string]any{"n": int64(1)}, ""},
		{"parameter_row", "$n ROW", false, map[string]any{"n": int64(1)}, ""},
		{"addition", "1+0 ROWS", false, nil, ""},
		{"parentheses", "(1) ROWS", false, nil, ""},
		{"function", "toInteger(1) ROWS", false, nil, ""},
		{"zero", "0 ROWS", true, nil, ""},
		{"negative", "-1 ROWS", true, nil, ""},
		{"fraction", "1.5 ROWS", true, nil, ""},
		{"integral_float", "1.0 ROWS", true, nil, ""},
		{"expression_zero", "1-1 ROWS", true, nil, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_zero", "$n ROWS", true, map[string]any{"n": int64(0)}, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_negative", "$n ROWS", true, map[string]any{"n": int64(-1)}, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_float", "$n ROWS", true, map[string]any{"n": 1.5}, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_null", "$n ROWS", true, map[string]any{"n": nil}, "Neo.ClientError.Statement.TypeError"},
		{"parameter_string", "$n ROWS", true, map[string]any{"n": "1"}, "Neo.ClientError.Statement.SyntaxError"},
	}

	for _, sample := range cases {
		t.Run(sample.name, func(t *testing.T) {
			base, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "tenant"))
			writer := openBoltTestConn(t, port)
			input := "[1,2,0,4]"
			code := "Neo.ClientError.Statement.ArithmeticError"
			if sample.invalid {
				input = "[1,2]"
				code = "Neo.ClientError.Statement.SyntaxError"
			}
			if sample.errorCode != "" {
				code = sample.errorCode
			}
			statement := "UNWIND " + input + " AS x CALL { WITH x CREATE (:BatchSizeRecord {v:12/x}) } IN TRANSACTIONS OF " + sample.size

			require.NoError(t, SendRun(t, writer, statement, sample.params, nil))
			actualCode, _, err := AssertFailure(t, writer)
			require.NoError(t, err)
			require.Equal(t, code, actualCode)
			reader := openBoltTestConn(t, port)
			rows := runBoltQueryAndCollectRecords(t, reader, "MATCH (n:BatchSizeRecord) RETURN n.v ORDER BY n.v")
			if sample.invalid {
				require.Empty(t, rows)
			} else {
				require.Equal(t, [][]interface{}{{int64(6)}, {int64(12)}}, rows)
			}
		})
	}
}
