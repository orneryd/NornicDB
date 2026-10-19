package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// PackStream must carry literal cursor references regardless of how Cypher
// represents a page. Both individual and batched record writers participate.
func TestSearchStreamDeliveryBoltEncodedPage(t *testing.T) {
	for _, kind := range []string{"json", "prefixed"} {
		for _, count := range []int{1, 52} {
			t.Run(fmt.Sprintf("%s/%d", kind, count), func(t *testing.T) {
				session, _, svc := newPageDeliverySession(t, count)
				expression := "apoc.convert.toJson(page)"
				if kind == "prefixed" {
					expression = "'cursor:' + page.qid"
				}
				query := fmt.Sprintf("UNWIND range(1,%d) AS i CALL db.retrieve({mode:'id',n:1,repetition:i}) YIELD page RETURN %s AS page", count, expression)
				require.NoError(t, session.handleRun(buildRunMessageData(query, nil, nil)))
				require.NotNil(t, session.lastResult)
				require.Len(t, session.lastResult.Rows, count)
				qids := make([]string, count)
				for i, row := range session.lastResult.Rows {
					value, ok := row[0].(string)
					require.True(t, ok)
					if kind == "json" {
						var page struct {
							QID string `json:"qid"`
						}
						require.NoError(t, json.Unmarshal([]byte(value), &page))
						qids[i] = page.QID
					} else {
						qids[i] = strings.TrimPrefix(value, "cursor:")
					}
					require.NotEmpty(t, qids[i])
				}
				require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"n": int64(-1)})))
				session.continuationDelivery.Fail()
				for _, qid := range qids {
					page, err := svc.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "embedded", QID: qid, N: 2}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
					require.NoError(t, err, "a client can resume the literal cursor reference delivered in a serialized record")
					require.False(t, page.HasMore)
				}
				_, err := startReplacementPage(svc)
				require.NoError(t, err, "reading each delivered cursor to the end releases capacity")
			})
		}
	}
}
