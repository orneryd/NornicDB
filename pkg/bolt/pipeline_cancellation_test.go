package bolt

import (
	"context"
	"strconv"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestBoltPipelineStageDeadlines(t *testing.T) {
	base, port := startTransactionLifecycleServer(t)
	for _, explicit := range []bool{false, true} {
		for _, tail := range []string{
			"WITH DISTINCT x RETURN count(*)",
			"WITH x ORDER BY x DESC RETURN count(*)",
			"RETURN collect(x + 1)",
			"WITH x + 1 AS y RETURN sum(y)",
		} {
			t.Run(strconv.FormatBool(explicit)+"/"+tail, func(t *testing.T) {
				conn := openBoltTestConn(t, port)
				baselineReaders := base.ActiveReaders()
				timeout := map[string]any{"tx_timeout": int64(200)}
				started := time.Now()
				if explicit {
					beginExplicitTransaction(t, conn, timeout)
					timeout = nil
				}
				require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
				require.NoError(t, SendRun(t, conn, "UNWIND range(1, 10000000) AS x "+tail, nil, timeout))
				code, _, err := AssertFailure(t, conn)
				require.NoError(t, err)
				elapsed := time.Since(started)
				if explicit {
					require.Equal(t, transactionTimedOutCode, code)
				} else {
					standard, _ := nornicerrors.Neo4jStatus(context.DeadlineExceeded)
					require.Equal(t, standard, code)
				}
				require.Less(t, elapsed, 450*time.Millisecond)
				require.Equal(t, baselineReaders, base.ActiveReaders(), "timeout response must follow snapshot cleanup")
				t.Logf("tx_timeout=200ms response=%s overdue=%s explicit=%t", elapsed, elapsed-200*time.Millisecond, explicit)
				require.NoError(t, SendReset(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
				require.Equal(t, [][]any{{int64(1)}}, runBoltQueryAndCollectRecords(t, conn, "RETURN 1"))
			})
		}
	}
}
