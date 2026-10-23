package cypher_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/stretchr/testify/require"
)

// How the work after a cancellation grows with statement text, per shape and
// route: the client's own statement (execute) and statement text a procedure
// runs (dynamic). Sizes come from NORNICDB_PARSE_SIZES (PARSESCALE lines); a
// shape and route stop growing once one size takes longer than 20s. At
// NORNICDB_PARSE_PROFILE_SIZE each shape's uncanceled run is sampled
// (PARSEPROF lines: the functions its time is spent in).
func TestStatementTextParseScaling(t *testing.T) {
	sizes := os.Getenv("NORNICDB_PARSE_SIZES")
	if sizes == "" {
		t.Skip("NORNICDB_PARSE_SIZES lists the statement sizes to measure")
	}
	profileSize, _ := strconv.Atoi(os.Getenv("NORNICDB_PARSE_PROFILE_SIZE"))
	executor := testutil.SetupTestExecutor(t)
	run := func(ctx context.Context, route, text string) error {
		if route == "dynamic" {
			_, err := executor.Execute(ctx, "CALL apoc.cypher.run($q, {}) YIELD value RETURN count(value) AS n", map[string]interface{}{"q": text})
			return err
		}
		_, err := executor.Execute(ctx, text, nil)
		return err
	}
	stopped := map[string]bool{}
	for _, field := range strings.Split(sizes, ",") {
		size, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoError(t, err)
		for _, shape := range testutil.StatementShapes(size) {
			for _, route := range []string{"execute", "dynamic"} {
				key := shape.Name + "/" + route
				if stopped[key] {
					continue
				}
				ctx, cancel := context.WithCancel(context.Background())
				finished := make(chan error, 1)
				started := time.Now()
				go func() { finished <- run(ctx, route, shape.Text) }()
				time.Sleep(5 * time.Millisecond)
				canceledAt := time.Now()
				var sampler *testutil.StackSampler
				if size == profileSize {
					// The work after the cancellation, sampled.
					sampler = testutil.StartStackSampler(2*time.Millisecond, "nornicdb/pkg/cypher")
				}
				cancel()
				err := <-finished
				ended := time.Now()
				if sampler != nil {
					for _, line := range sampler.Stop(40) {
						t.Logf("PARSEPROF shape=%s route=%s bytes=%d after_cancel=%v %s", shape.Name, route, len(shape.Text), ended.Sub(canceledAt), line)
					}
				}
				late := ended.Sub(canceledAt)
				t.Logf("PARSESCALE shape=%s route=%s bytes=%d total=%v after_cancel=%v outcome=%v", shape.Name, route, len(shape.Text), ended.Sub(started), late, err)
				if late > 20*time.Second {
					stopped[key] = true
				}
			}
		}
	}
}
