package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestReviewR73PropertyFrames(t *testing.T) {
	ctx := context.Background()
	driver := fabricCallRowsReference(t)
	require.NotNil(t, driver)
	for index, query := range []string{
		"WITH 1 AS x RETURN COUNT { WITH 2 AS y RETURN x.a AS v } AS v",
		"WITH $p AS x CALL (x) { WITH 2 AS y RETURN x.a AS v } RETURN v",
		"WITH $p AS x UNWIND [] AS stop CALL (x) { WITH 2 AS y RETURN x.a AS v } RETURN v",
		"UNWIND [] AS stop WITH 1 AS x CALL (x) { WITH 2 AS y RETURN x.a AS v } RETURN v",
		"WITH 1 AS x FOREACH (x IN [{a:1}] | CREATE (:ReviewR73Frame {v:x.a})) RETURN 1 AS v",
		"WITH 1 AS x FOREACH (`x` IN [{a:1}] | CREATE (:ReviewR73Frame {v:`x`.a})) RETURN 1 AS v",
	} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			params := map[string]interface{}{"p": int64(1)}
			_, referr := neo4j.ExecuteQuery(ctx, driver, query, params, neo4j.EagerResultTransformer)
			refcode := ""
			if referr != nil {
				var ref *neo4j.Neo4jError
				require.ErrorAs(t, referr, &ref)
				refcode = ref.Code
			}
			for _, prefix := range []string{"", "USE comp.one "} {
				exec := newFabricCallRowsExecutor(t)
				result, err := exec.Execute(ctx, prefix+query, params)
				code := ""
				if err != nil {
					code, _ = nornicerrors.Neo4jStatus(err)
				}
				t.Logf("query=%q ref=%s referr=%v local=%s err=%v result=%v", prefix+query, refcode, referr, code, err, result)
				require.Equal(t, refcode, code)
			}
		})
	}
}
