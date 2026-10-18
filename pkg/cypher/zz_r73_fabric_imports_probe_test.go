package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Rule 73 self-attack probe (temporary): CALL imports on the composite
// (Fabric) route, which reads imports with its own tokenizer.
func TestZZR73FabricImportsProbe(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()
	require.NoError(t, mgr.CreateDatabase("r73tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("r73cmp", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "r73tr", Type: "local", AccessMode: "read_write"},
	}))
	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()
	_, err = exec.Execute(ctx, "USE r73cmp.tr CREATE (:A {id:1}), (:A {id:2}), (:B)-[:R]->(:C {id:9})", nil)
	require.NoError(t, err)
	outer := "USE r73cmp CALL { USE r73cmp.tr MATCH (n:A) RETURN n, n.id AS k } "
	tail := " WITH k, c ORDER BY k RETURN collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r"
	for _, tc := range []struct{ name, call, want string }{
		{"F01 importing WITH scalar", "CALL { WITH k USE r73cmp.tr MATCH (m:A) WHERE m.id = k RETURN count(m) AS c }", `["1:1", "2:1"]`},
		{"F02 scope clause scalar", "CALL (k) { USE r73cmp.tr MATCH (m:A) WHERE m.id = k RETURN count(m) AS c }", `["1:1", "2:1"]`},
		{"F03 implicit read (extension)", "CALL { USE r73cmp.tr MATCH (m:A) WHERE m.id = k RETURN count(m) AS c }", `["1:1", "2:1"]`},
		{"F04 nested EXISTS declares n", "CALL { USE r73cmp.tr MATCH (m:B) WHERE EXISTS { (m)-->(n) } RETURN count(m) AS c }", `["1:1", "2:1"]`},
		{"F05 importing WITH node, nested EXISTS", "CALL { WITH n USE r73cmp.tr MATCH (m:B) WHERE EXISTS { (m)-->(n) } RETURN count(m) AS c }", `["1:0", "2:0"]`},
		{"F06 WITH k AS kk (Neo4j rejects)", "CALL { WITH k AS kk USE r73cmp.tr RETURN kk AS c }", `ERROR`},
		{"F07 WITH * import", "CALL { WITH * USE r73cmp.tr RETURN k AS c }", `["1:1", "2:2"]`},
		{"F08 WITH k WHERE (Neo4j rejects)", "CALL { WITH k WHERE k > 1 USE r73cmp.tr RETURN k AS c }", `ERROR`},
		{"F09 scope clause (*)", "CALL (*) { USE r73cmp.tr RETURN k AS c }", `["1:1", "2:2"]`},
		{"F10 scope clause () declares n", "CALL () { USE r73cmp.tr MATCH (m:B) WHERE EXISTS { (m)-->(n) } RETURN count(m) AS c }", `["1:1", "2:1"]`},
		{"F11 importing WITH two names", "CALL { WITH k, n USE r73cmp.tr MATCH (m:A) WHERE m.id = k RETURN count(m) AS c }", `["1:1", "2:1"]`},
		{"F12 property key k only", "CALL { USE r73cmp.tr MATCH (m:B) WHERE m.k IS NULL RETURN count(m) AS c }", `["1:1", "2:1"]`},
	} {
		query := outer + tc.call + tail
		got := ""
		result, err := exec.Execute(ctx, query, nil)
		if err != nil {
			got = "ERROR"
			if tc.want != "ERROR" {
				got = "ERROR " + strings.ReplaceAll(err.Error(), "\n", " ")
			}
		} else if len(result.Rows) == 1 {
			items, _ := result.Rows[0][0].([]interface{})
			quoted := make([]string, len(items))
			for i, item := range items {
				quoted[i] = fmt.Sprintf("%q", item)
			}
			got = "[" + strings.Join(quoted, ", ") + "]"
		} else {
			got = fmt.Sprintf("rows=%v", result.Rows)
		}
		verdict := "PASS"
		if got != tc.want {
			verdict = "FAIL"
		}
		t.Logf("R73|%s|%s|fabric|composite|got=%s|want=%s", verdict, tc.name, got, tc.want)
	}
}
