package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Replay the original seed fixture and both exact query texts. Autocommit
// reads with two rows and threshold one must stream; explicit transactions
// are deliberately outside ResultStream's documented scope.
func TestStreamMatchSeedsExactQueryContract(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("index=%t/tx=%t", indexed, explicit), func(t *testing.T) {
				exec, _ := newStreamTestExecutor(t, 1)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, err := exec.Execute(ctx, "CREATE (:Item {k: 5, s: 'x'}), (:Item {k: 5, s: 'x'}), (:Item {k: 5, s: 'y'}), (:Item {k: 6, s: 'x'})", nil)
				require.NoError(t, err)
				consume := func(query string, wantStream bool) {
					queryCtx, queryCancel := context.WithCancel(ctx)
					run := executeStreamed(queryCtx, exec, query, 1)
					defer func() { queryCancel(); <-run.stream.Done() }()
					var rows [][]interface{}
					streamed := false
					select {
					case <-run.stream.Started():
						streamed = true
						var done bool
						rows, done = run.stream.Next(-1)
						require.True(t, done)
						<-run.stream.Done()
					default:
						// executeStreamed returns only after Started or Done. Prefer
						// Started if both are closed: a stream can finish immediately
						// after buffering its threshold-crossing final row.
						<-run.stream.Done()
						if run.result != nil {
							rows = run.result.Rows
						}
					}
					t.Logf("EXACT_SEED query=%q streamed=%t rows=%v err=%v", query, streamed, rows, run.err)
					require.NoError(t, run.err)
					require.Equal(t, [][]interface{}{{"x"}, {"x"}}, rows)
					require.Equal(t, wantStream, streamed)
				}
				if indexed {
					// The original test consumes this exact unindexed read first.
					consume("MATCH (n:Item {k: 5, s: 'x'}) RETURN n.s AS s", true)
					_, err = exec.Execute(ctx, "CREATE INDEX item_k FOR (n:Item) ON (n.k)", nil)
					require.NoError(t, err)
				}
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) }()
				}
				query := "MATCH (n:Item {k: 5, s: 'x'}) RETURN n.s AS s"
				if indexed {
					query = "MATCH (n:Item {k: 5, s: 'x'}) RETURN n.s AS indexed"
				}
				consume(query, !explicit)
			})
		}
	}
}
