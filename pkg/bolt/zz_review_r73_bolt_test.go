package bolt

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR73BoltEmptyUnwindWrite(t *testing.T) {
	_, port := startTransactionLifecycleServer(t)
	seed := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, seed, "CREATE (:ReviewR73Seed)")
	require.NoError(t, seed.Close())
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto", true: "explicit"}[explicit], func(t *testing.T) {
			conn := openBoltTestConn(t, port)
			defer conn.Close()
			if explicit {
				beginExplicitTransaction(t, conn, nil)
			}
			r := runBoltQueryAndCollectRecords(t, conn, "MATCH (n:ReviewR73Seed) UNWIND [] AS x CREATE (:ReviewR73Unexpected) RETURN count(*)")
			require.Equal(t, [][]any{{int64(0)}}, r)
			if explicit {
				require.NoError(t, SendRollback(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			}
		})
	}
}
