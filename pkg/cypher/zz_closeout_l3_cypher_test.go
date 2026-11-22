package cypher

// Close-out lane L3 (v1.5.2 verification campaign), cluster A: public
// reproductions for Plane items P-N152-95, P-NGEN-7, P-N15-136, P-N15-135,
// P-N151-52, P-N152-121 and P-N152-119. Test-side only. Every test runs the
// statement on both parser frontends itself (config.SetParserType), so one
// default-parser job covers both. Expected values are Neo4j 5.26 behaviour as
// quoted in each item (see the comment above each test).

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// closeoutL3EachParser runs fn once per parser frontend.
func closeoutL3EachParser(t *testing.T, fn func(t *testing.T)) {
	t.Helper()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		parser := parser
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			fn(t)
		})
	}
}

func closeoutL3Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
}

// ---------------------------------------------------------------------------
// P-N152-95 (NOR-66 / N15-201): RETURN {`ab`:1} AS m, 1 AS `ab`
// returns the map key ab, but the second column is named ab literally.
// Neo4j Community 5.26.30 returns the columns [m, ab] (item text).
// ---------------------------------------------------------------------------

func TestCloseoutL3_N152_95_EscapedAliasHeader(t *testing.T) {
	closeoutL3EachParser(t, func(t *testing.T) {
		exec := closeoutL3Executor(t)
		ctx := context.Background()

		t.Run("control_plain_alias", func(t *testing.T) {
			res, err := exec.Execute(ctx, "RETURN {`ab`:1} AS m, 1 AS `ab`", nil)
			require.NoError(t, err)
			t.Logf("CLOSEOUT_L3 N152-95 control columns=%q rows=%v", res.Columns, res.Rows)
			require.Equal(t, []string{"m", "ab"}, res.Columns)
		})

		for _, tc := range []struct {
			name, query string
			want        []string
			key         string
		}{
			{"ascii_escape", "RETURN {`a\\u0062`:1} AS m, 1 AS `a\\u0062`", []string{"m", "ab"}, "ab"},
			{"uppercase_escape", "RETURN {`A\\u0042`:1} AS m, 1 AS `A\\u0042`", []string{"m", "AB"}, "AB"},
			{"accent_escape", "RETURN {`caf\\u00e9`:1} AS m, 1 AS `caf\\u00e9`", []string{"m", "café"}, "café"},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				res, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err)
				t.Logf("CLOSEOUT_L3 N152-95 %s columns=%q rows=%v", tc.name, res.Columns, res.Rows)
				require.Len(t, res.Rows, 1)
				m, ok := res.Rows[0][0].(map[string]interface{})
				require.True(t, ok, "first column is a map, got %T", res.Rows[0][0])
				assert.Contains(t, m, tc.key, "map key decodes (already corrected per the item)")
				assert.Equal(t, tc.want, res.Columns, "escaped alias header must be decoded like the map key (Neo4j 5.26.30: %q)", tc.want)
			})
		}
	})
}

// ---------------------------------------------------------------------------
// P-NGEN-7 (NOR-316): schema DDL ending in OPTIONS with no map is accepted;
// Neo4j rejects it as a syntax error (item text).
// ---------------------------------------------------------------------------

func TestCloseoutL3_NGEN_7_OptionsWithoutMap(t *testing.T) {
	closeoutL3EachParser(t, func(t *testing.T) {
		ctx := context.Background()
		t.Run("control_options_empty_map", func(t *testing.T) {
			exec := closeoutL3Executor(t)
			_, err := exec.Execute(ctx, "CREATE INDEX probe_ok FOR (n:ProbeOK) ON (n.v) OPTIONS {}", nil)
			require.NoError(t, err, "OPTIONS {} is valid DDL")
			_, err = exec.Execute(ctx, "CREATE CONSTRAINT probe_okc FOR (n:ProbeOKC) REQUIRE n.id IS UNIQUE OPTIONS {}", nil)
			require.NoError(t, err, "OPTIONS {} is valid DDL")
		})
		for _, tc := range []struct{ name, query string }{
			{"index", "CREATE INDEX probe_q FOR (n:ProbeQ) ON (n.v) OPTIONS"},
			{"constraint", "CREATE CONSTRAINT probe_v FOR (n:ProbeV) REQUIRE n.id IS UNIQUE OPTIONS"},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				exec := closeoutL3Executor(t)
				_, err := exec.Execute(ctx, tc.query, nil)
				t.Logf("CLOSEOUT_L3 NGEN-7 %s err=%v", tc.name, err)
				require.Error(t, err, "Neo4j rejects OPTIONS without a map as a syntax error: %q", tc.query)
			})
		}
	})
}

// ---------------------------------------------------------------------------
// P-N15-136 (NOR-367): empty composite creation and conditional catalog
// mutations. Neo4j 5.26 (Operations Manual, composite databases and aliases):
// CREATE COMPOSITE DATABASE name creates an empty composite; IF NOT EXISTS on
// an existing alias and IF EXISTS on a missing composite are no-ops.
// ---------------------------------------------------------------------------

func TestCloseoutL3_N15_136_ConditionalCatalogMutations(t *testing.T) {
	closeoutL3EachParser(t, func(t *testing.T) {
		ctx := context.Background()

		t.Run("control_duplicate_alias_without_condition_errors", func(t *testing.T) {
			exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
			require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db136"}))
			_, err := exec.Execute(ctx, "CREATE ALIAS al136 FOR DATABASE db136", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE ALIAS al136 FOR DATABASE db136", nil)
			t.Logf("CLOSEOUT_L3 N15-136 control duplicate alias err=%v", err)
			require.Error(t, err)
		})
		t.Run("control_drop_missing_composite_without_condition_errors", func(t *testing.T) {
			exec, _, _ := newCreateDatabaseRegressionExecutor(t)
			_, err := exec.Execute(ctx, "DROP COMPOSITE DATABASE nosuch136", nil)
			t.Logf("CLOSEOUT_L3 N15-136 control drop missing composite err=%v", err)
			require.Error(t, err)
		})

		t.Run("empty_composite_create", func(t *testing.T) {
			exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
			_, err := exec.Execute(ctx, "CREATE COMPOSITE DATABASE empty136", nil)
			t.Logf("CLOSEOUT_L3 N15-136 empty composite err=%v composite=%v", err, manager.IsCompositeDatabase("empty136"))
			require.NoError(t, err, "Neo4j 5.26 creates a composite database with no constituents")
			require.True(t, manager.IsCompositeDatabase("empty136"))
		})
		t.Run("alias_if_not_exists_noop", func(t *testing.T) {
			exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
			require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db136"}))
			require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "other136"}))
			_, err := exec.Execute(ctx, "CREATE ALIAS al136 FOR DATABASE db136", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE ALIAS al136 IF NOT EXISTS FOR DATABASE other136", nil)
			target, resolveErr := manager.ResolveDatabase("al136")
			t.Logf("CLOSEOUT_L3 N15-136 alias IF NOT EXISTS err=%v target=%q resolveErr=%v", err, target, resolveErr)
			require.NoError(t, err, "IF NOT EXISTS on an existing alias is a no-op in Neo4j")
			require.NoError(t, resolveErr)
			require.Equal(t, "db136", target, "the no-op keeps the original target")
		})
		t.Run("drop_composite_if_exists_noop", func(t *testing.T) {
			exec, _, _ := newCreateDatabaseRegressionExecutor(t)
			_, err := exec.Execute(ctx, "DROP COMPOSITE DATABASE nosuch136 IF EXISTS", nil)
			t.Logf("CLOSEOUT_L3 N15-136 drop composite IF EXISTS err=%v", err)
			require.NoError(t, err, "DROP COMPOSITE DATABASE ... IF EXISTS on a missing name is a no-op in Neo4j")
		})
		t.Run("drop_alias_if_exists_noop", func(t *testing.T) {
			exec, _, _ := newCreateDatabaseRegressionExecutor(t)
			_, err := exec.Execute(ctx, "DROP ALIAS nosuch136 IF EXISTS FOR DATABASE", nil)
			t.Logf("CLOSEOUT_L3 N15-136 drop alias IF EXISTS err=%v", err)
			require.NoError(t, err, "DROP ALIAS ... IF EXISTS FOR DATABASE on a missing alias is a no-op in Neo4j")
		})
	})
}

// ---------------------------------------------------------------------------
// P-N15-135 (NOR-366): native CREATE/DROP DATABASE and ALIAS successful
// results versus the Neo4j no-record result contract (Neo4j 5.26.0
// WriteAdministrationCommand.returnColumns is empty, item text).
// ---------------------------------------------------------------------------

func TestCloseoutL3_N15_135_AdministrationResultSchemas(t *testing.T) {
	closeoutL3EachParser(t, func(t *testing.T) {
		ctx := context.Background()
		exec, manager, _ := newCreateDatabaseRegressionExecutor(t)

		t.Run("control_show_databases_has_columns", func(t *testing.T) {
			res, err := exec.Execute(ctx, "SHOW DATABASES", nil)
			require.NoError(t, err)
			t.Logf("CLOSEOUT_L3 N15-135 control SHOW DATABASES columns=%q", res.Columns)
			require.NotEmpty(t, res.Columns, "a read command declares columns")
		})

		steps := []struct{ name, query string }{
			{"create_database", "CREATE DATABASE d135"},
			{"create_database_target", "CREATE DATABASE t135"},
			{"create_alias", "CREATE ALIAS a135 FOR DATABASE t135"},
			{"drop_alias", "DROP ALIAS a135 FOR DATABASE"},
			{"drop_database", "DROP DATABASE d135"},
		}
		for _, step := range steps {
			step := step
			t.Run(step.name, func(t *testing.T) {
				res, err := exec.Execute(ctx, step.query, nil)
				require.NoError(t, err, step.query)
				require.NotNil(t, res)
				t.Logf("CLOSEOUT_L3 N15-135 %s columns=%q rows=%v", step.name, res.Columns, res.Rows)
				assert.Empty(t, res.Columns, "Neo4j administration write commands declare no result columns: %q", step.query)
				assert.Empty(t, res.Rows, "Neo4j administration write commands return no records: %q", step.query)
			})
		}
		require.True(t, manager.Exists("t135"))
		require.False(t, manager.Exists("d135"))
	})
}

// ---------------------------------------------------------------------------
// P-N151-52 (NOR-92): Cypher 25 composed queries (NEXT, conditional CALL)
// must agree across normal execution, EXPLAIN and PROFILE and across both
// parsers (acceptance: "consistent mode prefixes, columns and validation").
// ---------------------------------------------------------------------------

type closeoutL3Outcome struct {
	ok      bool
	columns string
	rows    string
	err     string
}

func closeoutL3Run(exec *StorageExecutor, query string) closeoutL3Outcome {
	res, err := exec.Execute(context.Background(), query, nil)
	if err != nil {
		return closeoutL3Outcome{err: err.Error()}
	}
	return closeoutL3Outcome{ok: true, columns: fmt.Sprint(res.Columns), rows: fmt.Sprint(res.Rows)}
}

func TestCloseoutL3_N151_52_ComposedQueriesAcrossModes(t *testing.T) {
	statements := []struct {
		name, body  string
		wantColumns string
		wantRows    string
	}{
		{"next_when", "RETURN 'outer' AS value NEXT WHEN true THEN RETURN value AS tag ELSE RETURN value AS tag", "[tag]", "[[outer]]"},
		{"conditional_call", "WITH 'outer' AS value CALL (value) { WHEN true THEN RETURN value AS tag ELSE RETURN value AS tag } RETURN tag", "[tag]", "[[outer]]"},
	}
	byParser := map[string]map[string]closeoutL3Outcome{}
	closeoutL3EachParser(t, func(t *testing.T) {
		parser := config.GetParserType()
		byParser[parser] = map[string]closeoutL3Outcome{}
		exec := closeoutL3Executor(t)

		t.Run("control_cypher25_plain_return", func(t *testing.T) {
			got := closeoutL3Run(exec, "CYPHER 25 RETURN 'outer' AS tag")
			t.Logf("CLOSEOUT_L3 N151-52 control %+v", got)
			require.True(t, got.ok, got.err)
			require.Equal(t, "[tag]", got.columns)
		})

		for _, st := range statements {
			st := st
			t.Run(st.name, func(t *testing.T) {
				normal := closeoutL3Run(exec, "CYPHER 25 "+st.body)
				explain := closeoutL3Run(exec, "CYPHER 25 EXPLAIN "+st.body)
				profile := closeoutL3Run(exec, "CYPHER 25 PROFILE "+st.body)
				t.Logf("CLOSEOUT_L3 N151-52 %s normal=%+v", st.name, normal)
				t.Logf("CLOSEOUT_L3 N151-52 %s explain=%+v", st.name, explain)
				t.Logf("CLOSEOUT_L3 N151-52 %s profile=%+v", st.name, profile)
				byParser[parser][st.name] = normal
				require.True(t, normal.ok, "normal execution: %s", normal.err)
				require.Equal(t, st.wantColumns, normal.columns)
				require.Equal(t, st.wantRows, normal.rows)
				assert.True(t, explain.ok, "EXPLAIN of a statement that executes must plan it: %s", explain.err)
				assert.Equal(t, st.wantColumns, explain.columns, "EXPLAIN columns")
				assert.True(t, profile.ok, "PROFILE of a statement that executes must run it: %s", profile.err)
				assert.Equal(t, st.wantColumns, profile.columns, "PROFILE columns")
				assert.Equal(t, st.wantRows, profile.rows, "PROFILE rows")
			})
		}

		// Item reproduction (3): the expected outcome is unconfirmed, so only
		// agreement between modes is asserted here; parsers are compared below.
		t.Run("star_scope_modes_agree", func(t *testing.T) {
			body := "WITH 'outer' AS value CALL (value) { WHEN true THEN RETURN *, 'tag' AS tag ELSE RETURN *, 'tag' AS tag } RETURN tag"
			normal := closeoutL3Run(exec, "CYPHER 25 "+body)
			explain := closeoutL3Run(exec, "CYPHER 25 EXPLAIN "+body)
			profile := closeoutL3Run(exec, "CYPHER 25 PROFILE "+body)
			t.Logf("CLOSEOUT_L3 N151-52 star normal=%+v explain=%+v profile=%+v", normal, explain, profile)
			byParser[parser]["star"] = normal
			assert.Equal(t, normal.ok, explain.ok, "EXPLAIN admits exactly what execution admits")
			assert.Equal(t, normal.ok, profile.ok, "PROFILE admits exactly what execution admits")
		})
	})
	t.Run("parsers_agree", func(t *testing.T) {
		nornic, antlr := byParser[config.ParserTypeNornic], byParser[config.ParserTypeANTLR]
		for _, key := range []string{"next_when", "conditional_call", "star"} {
			t.Logf("CLOSEOUT_L3 N151-52 parsers %s nornic=%+v antlr=%+v", key, nornic[key], antlr[key])
			assert.Equal(t, nornic[key].ok, antlr[key].ok, "%s: both parsers admit or reject alike", key)
			assert.Equal(t, nornic[key].columns, antlr[key].columns, "%s: both parsers give the same columns", key)
		}
	})
}

// ---------------------------------------------------------------------------
// P-N152-121 (NOR-314): RETURN/WITH projection headers must be the same across
// normal execution, EXPLAIN and PROFILE, fast paths and aggregation, on both
// parsers. Expected headers are Neo4j 5.26 (expression text as written, alias
// when given, RETURN * in alphabetical order).
// ---------------------------------------------------------------------------

func TestCloseoutL3_N152_121_ProjectionHeadersAcrossModes(t *testing.T) {
	cases := []struct {
		name, query string
		want        []string
	}{
		{"return_alias", "MATCH (n:P) RETURN n.name AS name ORDER BY name", []string{"name"}},
		{"group_count_star", "MATCH (n:P) RETURN n.name, count(*)", []string{"n.name", "count(*)"}},
		{"count_fast_path", "MATCH (n:P) RETURN count(n)", []string{"count(n)"}},
		{"count_star_alias", "MATCH (n:P) RETURN count(*) AS c", []string{"c"}},
		{"unwind_fast_path", "UNWIND [1,2,3] AS x RETURN x", []string{"x"}},
		{"unwind_aggregate", "UNWIND [1,2,3] AS x RETURN sum(x) AS s, collect(x)", []string{"s", "collect(x)"}},
		{"with_star", "WITH 1 AS b, 2 AS a RETURN *", []string{"a", "b"}},
		{"unwind_match_star", "UNWIND [1] AS z MATCH (n:P) RETURN *", []string{"n", "z"}},
		{"with_reorder", "MATCH (n:P) WITH n.name AS name, n.age AS age RETURN age, name ORDER BY name", []string{"age", "name"}},
		{"expressions_as_written", "MATCH (n:P) RETURN n.age+1, toUpper(n.name)", []string{"n.age+1", "toUpper(n.name)"}},
		{"distinct", "MATCH (n:P) RETURN DISTINCT n.name AS name", []string{"name"}},
		{"avg_grouped", "MATCH (n:P) RETURN n.name AS name, avg(n.age) AS a", []string{"name", "a"}},
		{"with_count", "MATCH (n:P) WITH count(n) AS c RETURN c", []string{"c"}},
		{"procedure_yield", "CALL db.labels() YIELD label RETURN label", []string{"label"}},
	}
	closeoutL3EachParser(t, func(t *testing.T) {
		exec := closeoutL3Executor(t)
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:P {name:'a', age:1}), (:P {name:'b', age:2})", nil)
		require.NoError(t, err)

		t.Run("control_simple_return", func(t *testing.T) {
			res, err := exec.Execute(ctx, "RETURN 1 AS one", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"one"}, res.Columns)
		})
		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
					res, err := exec.Execute(ctx, mode+tc.query, nil)
					label := strings.TrimSpace(mode)
					if label == "" {
						label = "RUN"
					}
					if !assert.NoError(t, err, "%s %s", label, tc.query) {
						continue
					}
					t.Logf("CLOSEOUT_L3 N152-121 %s %s columns=%q rows=%d", tc.name, label, res.Columns, len(res.Rows))
					assert.Equal(t, tc.want, res.Columns, "%s header of %q", label, tc.query)
				}
			})
		}
	})
}

// ---------------------------------------------------------------------------
// P-N152-119 (NOR-310): a caller mutating a returned node, relationship, map
// or list value must not reach later rows of the same result or stored data.
// Materialized Execute rows; decay off and on (the statement view exists only
// with decay). Fixture: newPublicConsumerViewExecutor (repository).
// ---------------------------------------------------------------------------

func closeoutL3FirstOfList(v interface{}) (interface{}, bool) {
	switch typed := v.(type) {
	case []interface{}:
		if len(typed) > 0 {
			return typed[0], true
		}
	case []string:
		if len(typed) > 0 {
			return typed[0], true
		}
	}
	return nil, false
}

func closeoutL3SetFirstOfList(v interface{}, value string) bool {
	switch typed := v.(type) {
	case []interface{}:
		if len(typed) > 0 {
			typed[0] = value
			return true
		}
	case []string:
		if len(typed) > 0 {
			typed[0] = value
			return true
		}
	}
	return false
}

func closeoutL3EntityProperties(t *testing.T, v interface{}) map[string]interface{} {
	t.Helper()
	switch typed := v.(type) {
	case *storage.Node:
		return typed.Properties
	case *storage.Edge:
		return typed.Properties
	case map[string]interface{}:
		if props, ok := typed["properties"].(map[string]interface{}); ok {
			return props
		}
		return typed
	}
	t.Fatalf("unexpected entity carrier %T", v)
	return nil
}

func TestCloseoutL3_N152_119_ReturnedValueMutationDoesNotLeak(t *testing.T) {
	for _, decay := range []bool{false, true} {
		decay := decay
		for _, kind := range []string{"node", "relationship"} {
			kind := kind
			t.Run(fmt.Sprintf("decay=%v/%s", decay, kind), func(t *testing.T) {
				exec, be := newPublicConsumerViewExecutor(t, 1)
				be.SetDecayEnabled(decay)
				ctx := context.Background()
				setup := "MATCH (d:Doc {title:'a'}) SET d.tags=['original']"
				query := "MATCH (d:Doc {title:'a'}) UNWIND [1,2,3] AS i RETURN d AS e, d.tags AS tags, properties(d) AS props"
				if kind == "relationship" {
					setup = "MATCH (:Doc {title:'a'})-[r:LINK]->() SET r.tags=['original']"
					query = "MATCH (:Doc {title:'a'})-[r:LINK]->() UNWIND [1,2,3] AS i RETURN r AS e, r.tags AS tags, properties(r) AS props"
				}
				_, err := exec.Execute(ctx, setup, nil)
				require.NoError(t, err)

				res, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Len(t, res.Rows, 3)

				// Control: every row starts with the stored values.
				for i, row := range res.Rows {
					first, ok := closeoutL3FirstOfList(closeoutL3EntityProperties(t, row[0])["tags"])
					require.True(t, ok, "row %d entity tags list", i)
					require.Equal(t, "original", first)
				}

				row0 := res.Rows[0]
				entityProps := closeoutL3EntityProperties(t, row0[0])
				require.True(t, closeoutL3SetFirstOfList(entityProps["tags"], "mutated-entity-list"))
				entityProps["title"] = "mutated-entity-title"
				require.True(t, closeoutL3SetFirstOfList(row0[1], "mutated-cell-list"))
				props, ok := row0[2].(map[string]interface{})
				require.True(t, ok, "properties() is a map, got %T", row0[2])
				props["title"] = "mutated-props-title"
				if list, ok := props["tags"]; ok {
					closeoutL3SetFirstOfList(list, "mutated-props-list")
				}

				for i := 1; i < len(res.Rows); i++ {
					row := res.Rows[i]
					rowProps := closeoutL3EntityProperties(t, row[0])
					first, _ := closeoutL3FirstOfList(rowProps["tags"])
					cell, _ := closeoutL3FirstOfList(row[1])
					pmap, _ := row[2].(map[string]interface{})
					pfirst, _ := closeoutL3FirstOfList(pmap["tags"])
					t.Logf("CLOSEOUT_L3 N152-119 row %d entity.tags[0]=%v entity.title=%v cell[0]=%v props.title=%v props.tags[0]=%v",
						i, first, rowProps["title"], cell, pmap["title"], pfirst)
					assert.Equal(t, "original", first, "row %d entity list must not see row 0's mutation", i)
					assert.NotEqual(t, "mutated-entity-title", rowProps["title"], "row %d entity map", i)
					assert.Equal(t, "original", cell, "row %d list cell", i)
					assert.NotEqual(t, "mutated-props-title", pmap["title"], "row %d properties() map", i)
					assert.Equal(t, "original", pfirst, "row %d properties() list", i)
				}

				again, err := exec.Execute(ctx, strings.Replace(query, "UNWIND [1,2,3] AS i ", "", 1), nil)
				require.NoError(t, err)
				require.Len(t, again.Rows, 1)
				againFirst, _ := closeoutL3FirstOfList(again.Rows[0][1])
				t.Logf("CLOSEOUT_L3 N152-119 later statement tags[0]=%v", againFirst)
				assert.Equal(t, "original", againFirst, "a later statement must read the stored list")

				if kind == "node" {
					stored, err := be.GetNode(t.Context(), "nor310:a")
					require.NoError(t, err)
					sf, _ := closeoutL3FirstOfList(stored.Properties["tags"])
					assert.Equal(t, "original", sf, "stored node list")
					assert.Equal(t, "a", stored.Properties["title"], "stored node title")
				} else {
					stored, err := be.GetEdge(t.Context(), "nor310:linka")
					require.NoError(t, err)
					sf, _ := closeoutL3FirstOfList(stored.Properties["tags"])
					assert.Equal(t, "original", sf, "stored edge list")
					assert.Equal(t, "link", stored.Properties["title"], "stored edge title")
				}
			})
		}
	}
}
