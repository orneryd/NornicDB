package cypher

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// The existing remote codec and lookup tests cover NOR-23's transport/equality
// obligations. This pins the remaining combined public remote-ordering path
// with the same reference and transaction fixtures, on a tiny value population.
func TestNOR23RemoteNumericOrdering(t *testing.T) {
	driver := fabricCallRowsReference(t)
	require.NotNil(t, driver, "this reproduction requires the real 5.26.30 reference, not a skipped remote route")
	httpURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	require.NotEmpty(t, httpURI)
	forEachParser(t, func(t *testing.T) {
		exec := fabricDispatchRemoteExecutor(t)
		manager := exec.dbManager.(*testDatabaseManagerAdapter).manager
		require.NoError(t, manager.AddConstituent(t.Context(), "comp", multidb.ConstituentRef{Alias: "http", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: httpURI}))
		for _, values := range []struct{ name, list string }{
			{"adjacent signed integers", "[9223372036854775807,9007199254740993,-9007199254740992,-9223372036854775807-1,9007199254740992,-9007199254740993,9223372036854775806,-9223372036854775807]"},
			{"mixed numeric boundaries", "[9223372036854775808.0,9223372036854775807,9007199254740993,9007199254740992.0,-9007199254740992.0,-9007199254740993,-9223372036854775807-1]"},
		} {
			for _, direction := range []string{"ASC", "DESC"} {
				reference := "WITH " + values.list + " AS imported CALL (imported) { UNWIND imported AS v RETURN v } RETURN v ORDER BY v " + direction
				expected, err := neo4j.ExecuteQuery(t.Context(), driver, reference, nil, neo4j.EagerResultTransformer)
				require.NoError(t, err, reference)
				want := make([][]any, 0, len(expected.Records))
				for _, record := range expected.Records {
					want = append(want, record.Values)
				}
				require.NotEmpty(t, want)
				for _, destination := range []string{"remote", "http"} {
					t.Run(fmt.Sprintf("%s/%s/%s", values.name, direction, destination), func(t *testing.T) {
						query := "USE comp " + strings.Replace(reference, "{ UNWIND", "{ USE comp."+destination+" UNWIND", 1)
						fabricRunModes(t, exec, func(t *testing.T, runner *StorageExecutor) {
							got, err := runner.Execute(t.Context(), query, nil)
							require.NoError(t, err, query)
							require.Equal(t, []string{"v"}, got.Columns)
							require.Equal(t, fabricTypedRows(want), fabricTypedRows(got.Rows), "remote result must preserve numeric kinds and exact ordering")
						})
					})
				}
			}
		}
	})
}
