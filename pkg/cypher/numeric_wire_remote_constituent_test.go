package cypher

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// Cross the real HTTP and Bolt constituent boundaries with signed limits and
// typed values in the same imported row, including explicit transactions.
func TestNumericWireRemoteConstituentComposition(t *testing.T) {
	exec, driver := fabricRemoteImportExecutor(t)
	const fields = "v AS v, date('2024-02-29') AS d, point({x:2.0,y:3.0}) AS p "
	const tail = " RETURN {v:v,nested:[v,2.0],day:d,point:p} AS value } RETURN value.v AS v, value.nested AS nested, value.day.year AS year, value.point.srid AS srid"
	strip := strings.NewReplacer("USE comp.remote ", "", "USE comp.http ", "", "USE comp ", "")
	for _, value := range []int64{math.MinInt64, -9007199254740993, -9007199254740991, 9007199254740991, 9007199254740993, math.MaxInt64} {
		for _, source := range []string{"local", "remote", "http"} {
			for _, destination := range []string{"remote", "http"} {
				t.Run(fmt.Sprintf("%d/from_%s/to_%s", value, source, destination), func(t *testing.T) {
					start := "WITH $v AS v WITH " + fields
					if source != "local" {
						start = "CALL { USE comp." + source + " WITH $v AS v RETURN " + fields + "} "
					}
					query := "USE comp " + start + "CALL (v,d,p) { USE comp." + destination + tail
					params := map[string]any{"v": value}
					ref, err := neo4j.ExecuteQuery(context.Background(), driver, strip.Replace(query), params, neo4j.EagerResultTransformer)
					require.NoError(t, err)
					var want [][]any
					for _, record := range ref.Records {
						want = append(want, record.Values)
					}
					fabricRunModes(t, exec, func(t *testing.T, runner *StorageExecutor) {
						got, err := runner.Execute(context.Background(), query, params)
						require.NoError(t, err, query)
						require.Equal(t, fabricTypedRows(want), fabricTypedRows(got.Rows), query)
					})
				})
			}
		}
	}
}
