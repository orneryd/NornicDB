package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// The correlated remote-import contract: every variable a remote CALL body
// imports (WITH x, WITH *, a scope clause, with any whitespace or comments) is
// bound on the remote under its own identity, so caller parameters keep
// theirs and no variable is mistaken for Fabric's own; a scope clause's
// variables stay visible throughout the body; values keep their type over
// Bolt and HTTP, whichever participant they came from; graph entities are
// rejected explicitly, as Neo4j rejects importing them into a graph a USE
// clause selects. Expected rows and writes are the same statement's on one
// Neo4j database.
func TestFabricRemoteImportContract(t *testing.T) {
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("requires --ref neo-ref")
	}
	httpURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	type importCase struct {
		name, query, want string
		writes            int
		entity            bool
	}
	temporals := []struct{ name, expr, fields, want string }{
		{"date", "date('2024-02-29')", "v.year, v.month, v.day", "[2024,2,29]"},
		{"datetime", "datetime('2024-02-29T12:34:56.5+01:00')", "v.year, v.hour, v.second, v.millisecond, v.offset", `[2024,12,56,500,"+01:00"]`},
		{"localdatetime", "localdatetime('2024-02-29T12:34:56')", "v.year, v.month, v.day, v.hour", "[2024,2,29,12]"},
		{"time", "time('12:34:56+01:00')", "v.hour, v.minute, v.second, v.offset", `[12,34,56,"+01:00"]`},
		{"localtime", "localtime('12:34:56.000000123')", "v.hour, v.minute, v.second, v.nanosecond", "[12,34,56,123]"},
		{"duration", "duration('P1M1DT2H3.5S')", "v.months, v.days, v.hours, v.milliseconds", "[1,1,2,7203500]"},
	}
	cases := func(remote string) []importCase {
		use := "USE comp." + remote + " "
		tick := "`a`` RETURN 999 AS injected //`"
		cs := []importCase{
			{"unit", "USE comp UNWIND [1,2] AS x CALL { WITH x " + use + "CREATE (:FabricRemoteImport {run: $run, x: x}) } RETURN x ORDER BY x", "[[1],[2]]", 2, false},
			{"with_star", "USE comp UNWIND [1,2] AS x CALL { WITH * " + use + "CREATE (:FabricRemoteImport {run: $run, x: x}) } RETURN x ORDER BY x", "[[1],[2]]", 2, false},
			{"newline", "USE comp UNWIND [1,2] AS x CALL { WITH\nx " + use + "CREATE (:FabricRemoteImport {run: $run, x: x}) } RETURN x ORDER BY x", "[[1],[2]]", 2, false},
			{"scope_unit", "USE comp UNWIND [1,2] AS x CALL (x) { " + use + "CREATE (:FabricRemoteImport {run: $run, x: x}) } RETURN x ORDER BY x", "[[1],[2]]", 2, false},
			{"scope_star", "USE comp UNWIND [1,2] AS x CALL (*) { " + use + "CREATE (:FabricRemoteImport {run: $run, x: x}) } RETURN x ORDER BY x", "[[1],[2]]", 2, false},
			{"scope_return", "USE comp UNWIND [1,2] AS x CALL (x) { " + use + "RETURN x + 10 AS v } RETURN x, v ORDER BY x", "[[1,11],[2,12]]", 0, false},
			{"scope_empty", "USE comp UNWIND [1,2] AS x CALL () { " + use + "RETURN 5 AS v } RETURN x, v ORDER BY x", "[[1,5],[2,5]]", 0, false},
			{"optional_returning", "USE comp UNWIND [1,2] AS x OPTIONAL CALL { WITH x " + use + "WITH x WHERE x = 1 RETURN x + 10 AS v } RETURN x, v ORDER BY x", "[[1,11],[2,null]]", 0, false},
			{"union", "USE comp UNWIND [1,2] AS x CALL { WITH x " + use + "CREATE (:FabricRemoteImport {run: $run, x: x}) RETURN x + 10 AS v UNION ALL WITH x " + use + "CREATE (:FabricRemoteImport {run: $run, x: x}) RETURN x + 20 AS v } RETURN x, v ORDER BY x, v", "[[1,11],[1,21],[2,12],[2,22]]", 4, false},
			{"nested_returning", "USE comp UNWIND [1,2] AS x CALL { WITH x UNWIND [1,2] AS y CALL { WITH x, y " + use + "CREATE (:FabricRemoteImport {run: $run, x: x, y: y}) RETURN x * 10 + y AS v } RETURN v } RETURN x, v ORDER BY x, v", "[[1,11],[1,12],[2,21],[2,22]]", 4, false},
			{"parameter_identity", "USE comp UNWIND [1,2] AS x CALL { WITH x " + use + "RETURN x + $x AS v } RETURN x, v ORDER BY x", "[[1,101],[2,102]]", 0, false},
			{"quoted", "USE comp UNWIND [1,2] AS `odd name` CALL { WITH `odd name` " + use + "RETURN `odd name` + 10 AS v } RETURN v ORDER BY v", "[[11],[12]]", 0, false},
			{"escaped_tick", "USE comp UNWIND [1,2] AS `x``y` CALL { WITH `x``y` " + use + "RETURN `x``y` + 10 AS v } RETURN v ORDER BY v", "[[11],[12]]", 0, false},
			{"quoted_keyword", "USE comp UNWIND [1,2] AS `MATCH` CALL { WITH `MATCH` " + use + "RETURN `MATCH` + 10 AS v } RETURN v ORDER BY v", "[[11],[12]]", 0, false},
			{"injection_text", "USE comp UNWIND [1,2] AS " + tick + " CALL { WITH " + tick + " " + use + "RETURN " + tick + " + 10 AS v } RETURN v ORDER BY v", "[[11],[12]]", 0, false},
			{"structured", "USE comp WITH [1, null, {k: [true, 2.5]}] AS xs, {a: [1, 2], b: null} AS m, null AS z CALL { WITH xs, m, z " + use + "RETURN xs AS a, m AS b, z IS NULL AS c } RETURN a, b, c", `[[[1,null,{"k":[true,2.5]}],{"a":[1,2],"b":null},true]]`, 0, false},
			{"temporal_in_list", "USE comp WITH [date('2024-02-29'), 1] AS xs, {d: localtime('01:02:03')} AS m CALL { WITH xs, m " + use + "RETURN xs[0].year AS y, m.d.hour AS h } RETURN y, h", "[[2024,1]]", 0, false},
			{"tab_import", "USE comp UNWIND [1,2] AS x CALL { WITH\tx " + use + "RETURN x + 1 AS v } RETURN v ORDER BY v", "[[2],[3]]", 0, false},
			{"escaped_quote", "USE comp UNWIND [1,2] AS x CALL { WITH x " + use + "RETURN x + size('a\\'}b') AS v } RETURN v ORDER BY v", "[[5],[6]]", 0, false},
			{"escaped_quote_before_call", "USE comp WITH 'a\\'b{' AS s UNWIND [1,2] AS x CALL { WITH x " + use + "RETURN x + 1 AS v } RETURN s, v ORDER BY v", `[["a'b{",2],["a'b{",3]]`, 0, false},
			{"outer_variable_after_call", "USE comp WITH 'ab' AS s UNWIND [1,2] AS x CALL { WITH x " + use + "RETURN x + 1 AS v } RETURN s, v ORDER BY v", `[["ab",2],["ab",3]]`, 0, false},
			{"relay_integer_division", "USE comp CALL { USE comp.remote RETURN {a: [{n: -42}]} AS m } CALL { WITH m " + use + "RETURN m AS h } CALL { WITH h USE comp.remote RETURN h.a[0].n / 5 AS answer, toString(h.a[0].n) AS text } RETURN answer, text", `[[-8,"-42"]]`, 0, false},
			{"relay_integer_precision", "USE comp CALL { USE comp.remote RETURN {a: [{n: 9007199254740993}]} AS m } CALL { WITH m " + use + "RETURN m AS h } CALL { WITH h USE comp.remote RETURN h.a[0].n - 9007199254740992 AS answer, toString(h.a[0].n) AS text } RETURN answer, text", `[[1,"9007199254740993"]]`, 0, false},
			{"relay_float_control", "USE comp CALL { USE comp.remote RETURN {a: [{n: -42.0}]} AS m } CALL { WITH m " + use + "RETURN m AS h } CALL { WITH h USE comp.remote RETURN h.a[0].n / 5 AS answer, toString(h.a[0].n) AS text } RETURN answer, text", `[[-8.4,"-42.0"]]`, 0, false},
			{"outer_variable_after_scoped_call", "USE comp WITH 'ab' AS s UNWIND [1,2] AS x CALL (x) { " + use + "RETURN x + 1 AS v } RETURN s, x, v ORDER BY x", `[["ab",1,2],["ab",2,3]]`, 0, false},
			{"comment_import", "USE comp UNWIND [1,2] AS x CALL { WITH/* import */ x " + use + "RETURN x + 1 AS v } RETURN v ORDER BY v", "[[2],[3]]", 0, false},
			{"line_comment_import", "USE comp UNWIND [1,2] AS x CALL { WITH // import\n x " + use + "RETURN x + 1 AS v } RETURN v ORDER BY v", "[[2],[3]]", 0, false},
			{"comment_scope", "USE comp UNWIND [1,2] AS x CALL (x /* import */) { " + use + "RETURN x + 1 AS v } RETURN v ORDER BY v", "[[2],[3]]", 0, false},
			{"comment_before_scope", "USE comp UNWIND [1,2] AS x CALL /* scope */ (/* x */ x) { " + use + "RETURN x + 1 AS v } RETURN v ORDER BY v", "[[2],[3]]", 0, false},
			{"scope_quoted", "USE comp UNWIND [1,2] AS `a``b` CALL (`a``b`) { " + use + "RETURN `a``b` + 1 AS v } RETURN v ORDER BY v", "[[2],[3]]", 0, false},
			{"scope_survives_with", "USE comp UNWIND [1,2] AS x CALL (x) { " + use + "WITH 10 AS y RETURN x + y AS v } RETURN x, v ORDER BY x", "[[1,11],[2,12]]", 0, false},
			{"scope_star_survives_with", "USE comp UNWIND [1,2] AS x CALL (*) { " + use + "WITH 10 AS y RETURN x + y AS v } RETURN x, v ORDER BY x", "[[1,11],[2,12]]", 0, false},
			{"scope_write_after_with", "USE comp UNWIND [1,2] AS x CALL (x) { " + use + "WITH 10 AS y CREATE (:FabricRemoteImport {run: $run, x: x + y}) } RETURN x ORDER BY x", "[[1],[2]]", 2, false},
			{"scope_survives_nested_call", "USE comp UNWIND [1,2] AS x CALL (x) { " + use + "WITH 10 AS y CALL { USE comp.one RETURN 1 AS z } RETURN x + y + z AS v } RETURN x, v ORDER BY x", "[[1,12],[2,13]]", 0, false},
			{"scope_union", "USE comp UNWIND [1,2] AS x CALL (x) { " + use + "RETURN x + 10 AS v UNION ALL " + use + "RETURN x + 20 AS v } RETURN x, v ORDER BY x, v", "[[1,11],[1,21],[2,12],[2,22]]", 0, false},
			{"nested_scope", "USE comp UNWIND [1,2] AS x CALL (x) { WITH x, 10 AS y CALL (x, y) { " + use + "RETURN x + y AS v } RETURN v } RETURN x, v ORDER BY x", "[[1,11],[2,12]]", 0, false},
			{"reserved_parameters", "USE comp UNWIND [1,2] AS x CALL { WITH x " + use + "RETURN x + $x + $__fabric_import_0 + $__fabric_import_1 AS v } RETURN x, v ORDER BY x", "[[1,11101],[2,11102]]", 0, false},
			{"star_prefix_variable", "USE comp UNWIND [1,2] AS __fabric_import_0 CALL { WITH * " + use + "RETURN __fabric_import_0 + 1 AS v } RETURN v ORDER BY v", "[[2],[3]]", 0, false},
			{"prefix_variable_rows", "USE comp UNWIND [1,2] AS __fabric_row_x CALL { WITH __fabric_row_x " + use + "RETURN __fabric_row_x + 1 AS v } RETURN __fabric_row_x, v ORDER BY v", "[[1,2],[2,3]]", 0, false},
			{"only_column_row_wrapper_name", "USE comp UNWIND [1,2] AS __fabric_row CALL { WITH __fabric_row " + use + "RETURN __fabric_row + 1 AS v } RETURN __fabric_row ORDER BY __fabric_row", "[[1],[2]]", 0, false},
			{"only_column_generated_names", "USE comp UNWIND [1,2] AS __fabric_row_1 CALL { WITH __fabric_row_1 " + use + "RETURN __fabric_row_1 * 10 AS __fabric_apply_key } RETURN __fabric_apply_key ORDER BY __fabric_apply_key", "[[10],[20]]", 0, false},
			{"generated_parameter_names", "USE comp UNWIND [1,2] AS __fabric_import_0_1 CALL { WITH __fabric_import_0_1 " + use + "RETURN __fabric_import_0_1 + $__fabric_import_0 AS __fabric_apply_rows } RETURN __fabric_apply_rows ORDER BY __fabric_apply_rows", "[[1001],[1002]]", 0, false},
			{"point_local", "USE comp WITH point({x: 1.25, y: 2.5, srid: 7203}) AS p CALL { WITH p " + use + "RETURN p.x AS x, p.y AS y, p.srid AS s } RETURN x, y, s", "[[1.25,2.5,7203]]", 0, false},
			{"point_relay", "USE comp CALL { USE comp.remote RETURN point({x: 1.25, y: 2.5, srid: 7203}) AS p } CALL { WITH p " + use + "RETURN p.x AS x, p.y AS y, p.srid AS s } RETURN x, y, s", "[[1.25,2.5,7203]]", 0, false},
			{"point_nested_relay", "USE comp CALL { USE comp.remote RETURN {a: [point({x: 1.25, y: 2.5, srid: 7203})]} AS m } CALL { WITH m " + use + "RETURN m.a[0].x AS x, m.a[0].y AS y } RETURN x, y", "[[1.25,2.5]]", 0, false},
			{"nested_temporals", "USE comp WITH {`odd``key`: [{d: date('2024-02-29'), t: localtime('01:02:03.000000123')}]} AS m CALL { WITH m " + use + "RETURN m.`odd``key`[0].d.year AS y, m.`odd``key`[0].t.nanosecond AS n } RETURN y, n", "[[2024,123]]", 0, false},
			{"in_list_node", "USE comp CALL { USE comp.remote MATCH (n:FabricRemoteImportSeed {run: $run}) RETURN n } WITH [n] AS xs CALL { WITH xs " + use + "RETURN xs[0].v AS v } RETURN v", "", 0, true},
			{"in_map_node", "USE comp CALL { USE comp.remote MATCH (n:FabricRemoteImportSeed {run: $run}) RETURN n } WITH {a: [{n: n}]} AS m CALL { WITH m " + use + "RETURN m.a[0].n.v AS v } RETURN v", "", 0, true},
			{"remote_node", "USE comp CALL { USE comp.remote MATCH (n:FabricRemoteImportSeed {run: $run}) RETURN n } CALL { WITH n " + use + "RETURN n.v AS v } RETURN v", "", 0, true},
			{"remote_relationship", "USE comp CALL { USE comp.remote MATCH (:FabricRemoteImportSeed {run: $run})-[r:FABRIC_REMOTE_IMPORT]->() RETURN r } CALL { WITH r " + use + "RETURN r.v AS v } RETURN v", "", 0, true},
			{"remote_path", "USE comp CALL { USE comp.remote MATCH p = (:FabricRemoteImportSeed {run: $run})-[:FABRIC_REMOTE_IMPORT]->() RETURN p } CALL { WITH p " + use + "RETURN length(p) AS v } RETURN v", "", 0, true},
			{"local_node", "USE comp CALL { USE comp.one MATCH (n:P) RETURN n } CALL { WITH n " + use + "RETURN n.db AS v } RETURN v", "", 0, true},
		}
		for _, temporal := range temporals {
			cs = append(cs,
				importCase{"local_" + temporal.name, "USE comp WITH " + temporal.expr + " AS v CALL { WITH v " + use + "RETURN [" + temporal.fields + "] AS f } RETURN f", "[[" + temporal.want + "]]", 0, false},
				importCase{"relayed_" + temporal.name, "USE comp CALL { USE comp.remote RETURN " + temporal.expr + " AS v } CALL { WITH v " + use + "RETURN [" + temporal.fields + "] AS f } RETURN f", "[[" + temporal.want + "]]", 0, false})
			if httpURI != "" {
				cs = append(cs, importCase{"http_relayed_" + temporal.name, "USE comp CALL { USE comp.http RETURN " + temporal.expr + " AS v } CALL { WITH v " + use + "RETURN [" + temporal.fields + "] AS f } RETURN f", "[[" + temporal.want + "]]", 0, false})
			}
		}
		if httpURI != "" {
			cs = append(cs,
				importCase{"http_point_relay", "USE comp CALL { USE comp.http RETURN {a: [point({x: 1.25, y: 2.5, srid: 7203})]} AS m } CALL { WITH m " + use + "RETURN m.a[0].x AS x, m.a[0].y AS y } RETURN x, y", "[[1.25,2.5]]", 0, false},
				importCase{"http_node", "USE comp CALL { USE comp.http MATCH (n:FabricRemoteImportSeed {run: $run}) RETURN n } CALL { WITH n " + use + "RETURN n.v AS v } RETURN v", "", 0, true})
		}
		return cs
	}
	remotes := []string{"remote"}
	if httpURI != "" {
		remotes = append(remotes, "http")
	}
	strip := strings.NewReplacer("USE comp.remote ", "", "USE comp.http ", "", "USE comp.one ", "", "USE comp ", "")
	for _, normalization := range []string{"on", "off"} {
		t.Run("normalization="+normalization, func(t *testing.T) {
			if normalization == "off" {
				restore := config.WithCypherQueryNormalizationDisabled()
				defer restore()
			}
			exec := fabricDispatchRemoteExecutor(t)
			if httpURI != "" {
				mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
				require.NoError(t, mgr.AddConstituent(t.Context(), "comp", multidb.ConstituentRef{Alias: "http", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: httpURI}))
			}
			for _, remote := range remotes {
				for _, c := range cases(remote) {
					for _, explicit := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/%s/explicit=%v", remote, c.name, explicit), func(t *testing.T) {
							ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
							defer cancel()
							// Caller parameters named like Fabric's own keep their values.
							params := map[string]interface{}{"run": fmt.Sprintf("%s-%s-%d", remote, c.name, time.Now().UnixNano()), "x": int64(100),
								"__fabric_import_0": int64(1000), "__fabric_import_1": int64(10000), "__fabric_apply_keys": "caller", "__fabric_apply_rows": "caller"}
							defer func() {
								_, _ = neo4j.ExecuteQuery(context.Background(), driver, "MATCH (n) WHERE n.run = $run DETACH DELETE n", params, neo4j.EagerResultTransformer)
							}()
							if c.entity {
								_, err := neo4j.ExecuteQuery(ctx, driver, "CREATE (:FabricRemoteImportSeed {run: $run, v: 7})-[:FABRIC_REMOTE_IMPORT {v: 9}]->(:FabricRemoteImportEnd {run: $run})", params, neo4j.EagerResultTransformer)
								require.NoError(t, err)
							} else {
								reference := strip.Replace(c.query)
								ref, err := neo4j.ExecuteQuery(ctx, driver, reference, params, neo4j.EagerResultTransformer)
								require.NoError(t, err, "Neo4j query: %s", reference)
								rows := make([][]interface{}, 0, len(ref.Records))
								for _, record := range ref.Records {
									rows = append(rows, record.Values)
								}
								fabricDispatchRows(t, rows, c.want)
								require.EqualValues(t, c.writes, ref.Summary.Counters().NodesCreated(), "Neo4j writes")
								_, err = neo4j.ExecuteQuery(ctx, driver, "MATCH (n:FabricRemoteImport {run: $run}) DETACH DELETE n", params, neo4j.EagerResultTransformer)
								require.NoError(t, err)
							}
							runner := exec
							if explicit {
								runner = fabricCallRowsCompositeExecutor(t, exec)
								_, err := runner.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer func() { _, _ = runner.Execute(context.Background(), "ROLLBACK", nil) }()
							}
							result, err := runner.Execute(ctx, c.query, params)
							if c.entity {
								require.Error(t, err)
								code, _ := nornicerrors.Neo4jStatus(err)
								require.Equal(t, "Neo.ClientError.Statement.TypeError", code, "%v", err)
								var gql interface{ BoltErrorDetail() string }
								require.ErrorAs(t, err, &gql)
								require.Equal(t, "22N16", gql.BoltErrorDetail())
								require.ErrorContains(t, err, "Importing "+c.name[strings.LastIndex(c.name, "_")+1:]+" values in remote subqueries is currently not supported")
								return
							}
							require.NoError(t, err, "Fabric query: %s", c.query)
							fabricDispatchRows(t, result.Rows, c.want)
							if explicit {
								_, err := runner.Execute(ctx, "COMMIT", nil)
								require.NoError(t, err)
							}
							if c.writes > 0 {
								ref, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (n:FabricRemoteImport {run: $run}) RETURN count(n) AS c", params, neo4j.EagerResultTransformer)
								require.NoError(t, err)
								require.EqualValues(t, c.writes, ref.Records[0].Values[0], "durable writes")
							}
						})
					}
				}
			}
		})
	}
}

// A scoped CALL on a local constituent sees exactly its scope clause's
// variables, in autocommit and in an explicit composite transaction.
func TestFabricScopedCallLocal(t *testing.T) {
	ctx := context.Background()
	driver := fabricCallRowsReference(t)
	strip := strings.NewReplacer("USE comp.one ", "", "USE comp ", "")
	for _, normalization := range []string{"on", "off"} {
		t.Run("normalization="+normalization, func(t *testing.T) {
			if normalization == "off" {
				restore := config.WithCypherQueryNormalizationDisabled()
				defer restore()
			}
			exec := newFabricCallRowsExecutor(t)
			exec.cache = nil
			for _, tt := range []struct{ name, query, want string }{
				{"scope_x", "USE comp WITH 1 AS x CALL (x) { USE comp.one RETURN x AS v } RETURN v", "[[1]]"},
				{"scope_x_rows", "USE comp UNWIND [1,2] AS x CALL (x) { USE comp.one RETURN x + 10 AS v } RETURN x, v ORDER BY x", "[[1,11],[2,12]]"},
				{"scope_star", "USE comp UNWIND [1,2] AS x CALL (*) { USE comp.one RETURN x + 10 AS v } RETURN x, v ORDER BY x", "[[1,11],[2,12]]"},
				{"scope_empty", "USE comp UNWIND [1,2] AS x CALL () { USE comp.one RETURN 5 AS v } RETURN x, v ORDER BY x", "[[1,5],[2,5]]"},
				{"optional_scope", "USE comp UNWIND [1,2] AS x OPTIONAL CALL (x) { USE comp.one WITH x WHERE x = 1 RETURN x + 10 AS v } RETURN x, v ORDER BY x", "[[1,11],[2,null]]"},
				{"scope_survives_with", "USE comp UNWIND [1,2] AS x CALL (x) { USE comp.one WITH 10 AS y RETURN x + y AS v } RETURN x, v ORDER BY x", "[[1,11],[2,12]]"},
				{"only_column_row_wrapper_name", "USE comp UNWIND [1,2] AS __fabric_row CALL { WITH __fabric_row USE comp.one RETURN __fabric_row + 1 AS v } RETURN __fabric_row ORDER BY __fabric_row", "[[1],[2]]"},
				{"only_column_generated_names", "USE comp UNWIND [1,2] AS __fabric_row_1 CALL (__fabric_row_1) { USE comp.one RETURN __fabric_row_1 * 10 AS __fabric_apply_key } RETURN __fabric_apply_key ORDER BY __fabric_apply_key", "[[10],[20]]"},
				{"scope_comment", "USE comp UNWIND [1,2] AS x CALL (x /* import */) { USE comp.one RETURN x + 10 AS v } RETURN x, v ORDER BY x", "[[1,11],[2,12]]"},
				{"escaped_quote", "USE comp UNWIND [1,2] AS x CALL (x) { USE comp.one RETURN x + size('a\\'}b') AS v } RETURN x, v ORDER BY x", "[[1,5],[2,6]]"},
				{"escaped_quote_before_call", "USE comp WITH 'a\\'b{' AS s UNWIND [1,2] AS x CALL (x) { USE comp.one RETURN x + 10 AS v } RETURN s, x, v ORDER BY x", `[["a'b{",1,11],["a'b{",2,12]]`},
				{"outer_variable_after_scoped_call", "USE comp WITH 'ab' AS s UNWIND [1,2] AS x CALL (x) { USE comp.one RETURN x + 10 AS v } RETURN s, x, v ORDER BY x", `[["ab",1,11],["ab",2,12]]`},
				{"outer_variable_after_call", "USE comp WITH 'ab' AS s UNWIND [1,2] AS x CALL { WITH x USE comp.one RETURN x + 10 AS v } RETURN s, x, v ORDER BY x", `[["ab",1,11],["ab",2,12]]`},
				{"with_modifiers_before_call", "USE comp WITH [3,1,2] AS ys UNWIND ys AS y WITH y AS x ORDER BY x LIMIT 2 CALL (x) { USE comp.one RETURN x + 10 AS v } RETURN x, v ORDER BY x", "[[1,11],[2,12]]"},
			} {
				t.Run(tt.name, func(t *testing.T) {
					fabricCallDescriptorRun(t, exec, driver, tt.query, nil, tt.want)
				})
			}
			// CALL () imports nothing: the outer x is not defined in its body.
			t.Run("scope_empty_hides_outer", func(t *testing.T) {
				query := "USE comp UNWIND [1,2] AS x CALL () { USE comp.one RETURN x AS v } RETURN v"
				if driver != nil {
					_, err := neo4j.ExecuteQuery(ctx, driver, strip.Replace(query), nil, neo4j.EagerResultTransformer)
					require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError")
				}
				for _, explicit := range []bool{false, true} {
					runner := exec
					if explicit {
						runner = fabricCallRowsCompositeExecutor(t, exec)
						_, err := runner.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					_, err := runner.Execute(ctx, query, nil)
					if explicit {
						_, _ = runner.Execute(ctx, "ROLLBACK", nil)
					}
					require.Error(t, err, "explicit=%v", explicit)
				}
			})
		})
	}
}
