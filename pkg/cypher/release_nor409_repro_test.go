package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestReleaseNOR409CollectAPOCDeadline(t *testing.T) {
	prior := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(prior) })
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			exec, _ := newTestExecutor(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			result, err := exec.Execute(ctx, "WITH ['d2'] AS x RETURN apoc.coll.subtract(x, COLLECT { MATCH (o:PDCollection)<-[:MEMBER_OF]-(m:PDDocument) RETURN m.id AS id }) AS y", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{[]interface{}{"d2"}}}, result.Rows)
		})
	}
}
