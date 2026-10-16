package bolt

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The actual Bolt RUN/PULL route must recognize a comment-separated COUNT
// subquery even when normalization is disabled. Each case gets its own socket
// and explicit transaction so a FAILURE cannot contaminate another control.
func TestBoltCountSubqueryCommentGap(t *testing.T) {
	previousParser := config.GetParserType()
	previousNormalization := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() {
		config.SetParserType(previousParser)
		config.SetCypherQueryNormalizationEnabled(previousNormalization)
	})
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "neo4j"))
	for _, parser := range []string{"nornic", "antlr"} {
		for _, normalize := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for _, comment := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/normalize=%t/explicit=%t/comment=%t", parser, normalize, explicit, comment), func(t *testing.T) {
						config.SetParserType(parser)
						config.SetCypherQueryNormalizationEnabled(normalize)
						conn := openBoltTestConn(t, port)
						require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
						if explicit {
							require.NoError(t, SendBegin(t, conn, nil))
							require.NoError(t, ReadSuccess(t, conn))
						}
						gap := " "
						if comment {
							gap = " /*gap*/ "
						}
						records := runBoltQueryAndCollectRecords(t, conn, "RETURN (COUNT"+gap+"{ LIMIT 1 RETURN 1 AS x }) AS v")
						require.Equal(t, [][]any{{int64(1)}}, records)
						if explicit {
							require.NoError(t, SendRollback(t, conn))
							require.NoError(t, ReadSuccess(t, conn))
						}
					})
				}
			}
		}
	}
}
