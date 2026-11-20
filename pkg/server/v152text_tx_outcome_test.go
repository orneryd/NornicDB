package server

// v1.5.2 verification, built from the text of Plane item N15-96 (NOR-296): an
// HTTP explicit transaction's COMMIT that meets the transaction's expiry may
// answer with an outcome that does not match what was stored. Oracle: whatever
// the COMMIT answers, it must agree with the durable state read afterwards.
// COMMIT is sent at offsets around the 300 ms transaction timeout.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

func TestV152TextCommitAnswerMatchesStoredOutcomeAtExpiry(t *testing.T) {
	const timeout = 300 * time.Millisecond
	s, authenticator := setupTestServerWithDatabaseConfig(t, func(config *nornicdb.Config) {
		config.Database.TransactionTimeout = timeout
	}, nil)
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	s.txSessions = txsession.NewManager(timeout, s.newExecutorForDatabase, t.Context())
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	mismatches := 0
	for i, offset := range []time.Duration{-100, -50, -20, -10, 0, 10, 20, 50, 100, 200} {
		key := fmt.Sprintf("v152-%d", i)
		status, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx", "CREATE (:V152Outcome {k:'"+key+"'})")
		require.Equal(t, http.StatusCreated, status)
		time.Sleep(timeout + offset*time.Millisecond)
		commitStatus, committed := lifecycleTransactionPost(t, s, token, opened.Commit)
		_ = strings.TrimSpace
		answeredSuccess := commitStatus == http.StatusOK && len(committed.Errors) == 0
		readStatus, read := lifecycleTransactionPost(t, s, token, "/db/nornic/tx/commit", "MATCH (n:V152Outcome {k:'"+key+"'}) RETURN count(n) AS c")
		require.Equal(t, http.StatusOK, readStatus)
		stored := fmt.Sprint(read.Results[0].Data[0].Row[0]) == "1"
		codes := []string{}
		for _, e := range committed.Errors {
			codes = append(codes, e.Code)
		}
		t.Logf("V152TEXT NOR-296 offset=%+dms commit status=%d errors=%v answeredSuccess=%v stored=%v", offset, commitStatus, codes, answeredSuccess, stored)
		if answeredSuccess != stored {
			mismatches++
		}
	}
	require.Zero(t, mismatches, "COMMIT answers disagreed with the stored outcome")
}
