package bolt

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamDeliveryBoltSemanticFailure(t *testing.T) {
	for _, kind := range []string{"pull_options", "discard_options", "run_with_pending_statement"} {
		t.Run(kind, func(t *testing.T) {
			session, _, svc := newPageDeliverySession(t, 1)
			if kind == "run_with_pending_statement" {
				require.NoError(t, session.handleBegin(encodePackStreamMap(map[string]any{})))
			}
			require.NoError(t, session.handleRun(buildRunMessageData("CALL db.retrieve({mode:'id',n:1})", nil, nil)))
			switch kind {
			case "pull_options":
				require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"n": "invalid"})))
			case "discard_options":
				require.NoError(t, session.handleDiscard(encodePackStreamMap(map[string]any{"n": "invalid"})))
			case "run_with_pending_statement":
				require.NoError(t, session.handleRun(buildRunMessageData("CREATE (", nil, nil)))
			}
			require.True(t, session.failedUntilReset)
			replacement, err := startReplacementPage(svc)
			require.NoError(t, err, "FAILURE prevents PULL until RESET and must retire undelivered pages")
			_, err = svc.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "replacement", QID: replacement.QID, Discard: true}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			require.NoError(t, session.handleReset(nil))
			require.NoError(t, session.handleRun(buildRunMessageData("CALL db.retrieve({mode:'id',n:1})", nil, nil)))
			require.NotNil(t, session.lastResult, "RESET permits a fresh delivery batch")
		})
	}
}

func TestSearchStreamDeliveryBoltDeliveredPageSurvivesFailure(t *testing.T) {
	session, _, svc := newPageDeliverySession(t, 1)
	require.NoError(t, session.handleRun(buildRunMessageData("CALL db.retrieve({mode:'id',n:1})", nil, nil)))
	qid := session.lastResult.Rows[0][0].(map[string]any)["qid"].(string)
	require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"n": int64(-1)})))
	require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"n": "invalid"})))
	require.True(t, session.failedUntilReset)
	page, err := svc.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "embedded", QID: qid, N: 2}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err, "FAILURE retires only pages that have not been delivered")
	require.False(t, page.HasMore)
}

func TestSearchStreamDeliveryBoltDiscardEncodedPage(t *testing.T) {
	session, _, svc := newPageDeliverySession(t, 2)
	require.NoError(t, session.handleRun(buildRunMessageData("UNWIND [1,2] AS i CALL db.retrieve({mode:'id',n:1,repetition:i}) YIELD page RETURN apoc.convert.toJson(page) AS page", nil, nil)))
	require.Len(t, session.lastResult.Rows, 2)
	qids := make([]string, 2)
	for i, row := range session.lastResult.Rows {
		var page struct {
			QID string `json:"qid"`
		}
		require.NoError(t, json.Unmarshal([]byte(row[0].(string)), &page))
		qids[i] = page.QID
	}
	require.NoError(t, session.handleDiscard(encodePackStreamMap(map[string]any{"n": int64(1)})))
	_, err := svc.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "embedded", QID: qids[0], N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrGoneQID, "partial DISCARD ends the selected cursor immediately")
	_, err = startReplacementPage(svc)
	require.NoError(t, err, "discarded encoded record releases one slot")
	require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"n": int64(-1)})))
	session.continuationDelivery.Fail()
	_, err = svc.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "embedded", QID: qids[1], N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err, "the remaining delivered encoded page survives")
}
