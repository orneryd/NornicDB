package bolt

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoltZeroRowCorrelatedCallAggregation(t *testing.T) {
	_, port := startTransactionLifecycleServer(t)
	seed := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, seed, "CREATE (:RowAggregateNode:RowAggregateItem {id:'n0',origin:'automatic',group_id:'group0',source_key:'s0',name:'item',cite_start:[],cite_end:[],identifier_values:[],identifier_schemes:[],identifier_issuers:[],identifier_jurisdictions:[],identifier_periods:[],identifier_categories:[],alias_names:[],alias_types:[]}), (:RowAggregateSource {id:'s0',text:''})")
	require.NoError(t, seed.Close())
	for _, fixture := range []string{"row_zero_aggregate.cypher", "row_zero_aggregate_minimal.cypher"} {
		query, err := os.ReadFile("../cypher/testdata/" + fixture)
		require.NoError(t, err)
		for _, explicit := range []bool{false, true} {
			mode := "auto"
			if explicit {
				mode = "explicit"
			}
			for _, populated := range []bool{false, true} {
				name := "empty"
				if populated {
					name = "nonempty"
				}
				t.Run(fixture+"/"+mode+"/"+name, func(t *testing.T) {
					conn := openBoltTestConn(t, port)
					defer conn.Close()
					if explicit {
						beginExplicitTransaction(t, conn, nil)
					}
					params := map[string]any{"active": []any{}, "items": []any{}, "current": true, "group_id": "group0", "source_key": "s0"}
					want := 0
					if populated {
						params["active"] = []any{map[string]any{"targets": []any{"n0"}}}
						params["items"] = []any{map[string]any{"continues": "n0", "own": "fallback"}}
						want = 1
					}
					require.NoError(t, SendRun(t, conn, string(query), params, nil))
					require.NoError(t, ReadSuccess(t, conn))
					require.NoError(t, SendPull(t, conn, nil))
					var records [][]any
					for {
						kind, data, err := ReadMessage(conn)
						require.NoError(t, err)
						if kind == MsgSuccess {
							break
						}
						if kind == MsgFailure {
							failure, _, err := decodePackStreamMap(data, 0)
							require.NoError(t, err)
							t.Fatalf("query failure: %v", failure)
						}
						require.Equal(t, byte(MsgRecord), kind)
						fields, _, err := decodePackStreamList(data, 0)
						require.NoError(t, err)
						records = append(records, fields)
					}
					require.Len(t, records, 1)
					require.Len(t, records[0], 3)
					subjects := records[0][0].([]any)
					require.Len(t, subjects, want)
					given := records[0][1].(map[string]any)
					require.Len(t, given["subject"], want)
					require.Empty(t, given["party"])
					require.Empty(t, given["role"])
					carry := records[0][2].(map[string]any)
					require.Len(t, carry["ids"], want)
					require.Len(t, carry["carried"], want)
					if populated {
						require.Equal(t, "n0", subjects[0].(map[string]any)["id"])
						require.Equal(t, []any{"n0"}, carry["ids"])
						require.Equal(t, []any{"n0"}, carry["carried"])
					}
					if explicit {
						require.NoError(t, SendCommit(t, conn))
						require.NoError(t, ReadSuccess(t, conn))
					}
				})
			}
		}
	}
}
