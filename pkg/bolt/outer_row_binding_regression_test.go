package bolt

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoltOuterRowBindingInComprehensionProjection(t *testing.T) {
	query, err := os.ReadFile("../cypher/testdata/outer_row_binding.cypher")
	require.NoError(t, err)
	parameters, err := os.ReadFile("../cypher/testdata/outer_row_binding.params.json")
	require.NoError(t, err)
	_, port := startTransactionLifecycleServer(t)
	for _, explicit := range []bool{false, true} {
		mode := "auto"
		if explicit {
			mode = "explicit"
		}
		t.Run(mode, func(t *testing.T) {
			conn := openBoltTestConn(t, port)
			defer conn.Close()
			if explicit {
				beginExplicitTransaction(t, conn, nil)
			}
			var params map[string]any
			require.NoError(t, json.Unmarshal(parameters, &params))
			require.NoError(t, SendRun(t, conn, string(query), params, nil))
			kind, data, err := ReadMessage(conn)
			require.NoError(t, err)
			if kind == MsgFailure {
				failure, _, err := decodePackStreamMap(data, 0)
				require.NoError(t, err)
				t.Fatalf("RUN failure: %v", failure)
			}
			require.Equal(t, byte(MsgSuccess), kind)
			require.NoError(t, SendPull(t, conn, nil))
			var rows [][]any
			for {
				kind, data, err := ReadMessage(conn)
				require.NoError(t, err)
				if kind == MsgSuccess {
					break
				}
				if kind == MsgFailure {
					failure, _, err := decodePackStreamMap(data, 0)
					require.NoError(t, err)
					t.Fatalf("PULL failure: %v", failure)
				}
				require.Equal(t, byte(MsgRecord), kind)
				fields, _, err := decodePackStreamList(data, 0)
				require.NoError(t, err)
				rows = append(rows, fields)
			}
			require.Len(t, rows, 2)
			for index, category := range []string{"first", "second"} {
				require.Len(t, rows[index], 2)
				require.Equal(t, []any{map[string]any{
					"scheme": "code0", "value": "value0", "issuer": nil,
					"jurisdiction": nil, "period": nil, "category": category,
				}}, rows[index][0])
				require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, rows[index][1])
			}
			require.NotEqual(t, rows[0][1], rows[1][1])
			if explicit {
				require.NoError(t, SendCommit(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			}
		})
	}
}
