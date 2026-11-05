package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// NOR-300 bounded follow-up: CREATE INDEX ... OPTIONS and CREATE CONSTRAINT
// ... OPTIONS without a payload are malformed. This implementation admits
// OPTIONS and one payload — one map literal or one parameter reference, in the
// ANTLR grammar as well as in the native parser — while the native parser
// checked only that the tail started with the keyword, accepted the statement
// and applied the schema change, so the two parsers disagreed. Both parser
// configurations must reject the keyword without a payload with the
// established SyntaxError classification and leave the schema untouched, while
// a valid map (including an empty one), a valid parameter and an omitted
// OPTIONS still create the schema object. A map stays valid when the statement
// ends with trivia after it: the map is the whole OPTIONS payload, and the
// comment, whitespace or delimiter that follows belongs to statement framing,
// not to this tail.
//
// The parser selection is process-wide, so each parser's run is its own
// process (see runCypherTestProcess).
func TestNOR300CreateIndexOptionsRequiresMap(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		// Every index DDL form that admits an OPTIONS tail: the unqualified name
		// and RANGE / VECTOR forms, the relationship form, the legacy
		// ON :Label(p) form and FULLTEXT.
		missingMap := []string{
			"CREATE INDEX nor300_generic FOR (n:D) ON (n.p) OPTIONS",
			"CREATE RANGE INDEX nor300_range FOR (n:D) ON (n.p) OPTIONS",
			"CREATE VECTOR INDEX nor300_vector FOR (n:D) ON (n.e) OPTIONS",
			"CREATE INDEX nor300_rel FOR ()-[r:D]-() ON (r.p) OPTIONS",
			"CREATE INDEX nor300_legacy ON :D(p) OPTIONS",
			"CREATE FULLTEXT INDEX nor300_full FOR (n:D) ON EACH [n.t] OPTIONS",
		}
		accepted := []struct{ query, index string }{
			{"CREATE INDEX nor300_map FOR (n:D) ON (n.p) OPTIONS { indexProvider: 'range-1.0' }", "nor300_map"},
			{"CREATE INDEX nor300_emptymap FOR (n:D) ON (n.q) OPTIONS {}", "nor300_emptymap"},
			{"CREATE INDEX nor300_tight FOR (n:D) ON (n.s) OPTIONS{indexProvider: 'range-1.0'}", "nor300_tight"},
			{"CREATE INDEX nor300_omitted FOR (n:D) ON (n.r)", "nor300_omitted"},
			{"CREATE VECTOR INDEX nor300_vector_map FOR (n:D) ON (n.e) OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }", "nor300_vector_map"},
			{"CREATE FULLTEXT INDEX nor300_full_map FOR (n:D) ON EACH [n.t] OPTIONS { indexProvider: 'fulltext-1.0' }", "nor300_full_map"},
		}
		// Trivia after the map, and the statement's one optional delimiter,
		// belong to the shared trivia reader and to statement framing. They may
		// not turn a literal map into invalid OPTIONS syntax, whether or not the
		// canonical pass removed the comments first.
		trailingTrivia := []struct{ query, index string }{
			{"CREATE INDEX nor300_line FOR (n:D) ON (n.p) OPTIONS { indexProvider: 'range-1.0' } // trailing note", "nor300_line"},
			{"CREATE INDEX nor300_block FOR (n:D) ON (n.p) OPTIONS { indexProvider: 'range-1.0' } /* trailing note */", "nor300_block"},
			{"CREATE INDEX nor300_newline FOR (n:D) ON (n.p) OPTIONS {}\n// trailing note\n", "nor300_newline"},
			{"CREATE INDEX nor300_delimiter FOR (n:D) ON (n.p) OPTIONS {};", "nor300_delimiter"},
			{"CREATE INDEX nor300_comment_delimiter FOR (n:D) ON (n.p) OPTIONS {} /* note */ ;", "nor300_comment_delimiter"},
			{"CREATE INDEX nor300_nested FOR (n:D) ON (n.p) OPTIONS { indexConfig: { `a`: 1 } } // note", "nor300_nested"},
			{"CREATE INDEX nor300_quoted FOR (n:D) ON (n.p) OPTIONS { indexProvider: '}{' } /* note */", "nor300_quoted"},
			{"CREATE INDEX nor300_rel_trivia FOR ()-[r:D]-() ON (r.p) OPTIONS {} // note", "nor300_rel_trivia"},
			{"CREATE VECTOR INDEX nor300_vector_trivia FOR (n:D) ON (n.e) OPTIONS { indexConfig: { `vector.dimensions`: 3 } } // note", "nor300_vector_trivia"},
			{"CREATE FULLTEXT INDEX nor300_full_trivia FOR (n:D) ON EACH [n.t] OPTIONS { indexProvider: 'fulltext-1.0' } /* note */", "nor300_full_trivia"},
		}
		// Real trailing syntax after the map stays rejected: the tail is OPTIONS
		// and one map, not a prefix of a longer statement.
		trailingSyntax := []string{
			"CREATE INDEX nor300_junk FOR (n:D) ON (n.p) OPTIONS {} junk",
			"CREATE INDEX nor300_after FOR (n:D) ON (n.p) OPTIONS {} MATCH (n) RETURN n",
			"CREATE INDEX nor300_string FOR (n:D) ON (n.p) OPTIONS 'x'",
		}

		t.Run("missing map is a SyntaxError without schema mutation", func(t *testing.T) {
			for _, query := range missingMap {
				exec, _ := newTestExecutor(t)
				before := readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name")
				_, err := exec.Execute(t.Context(), query, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), query)
			}
		})

		t.Run("valid map and omitted OPTIONS still create the index", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			for _, control := range accepted {
				_, err := exec.Execute(t.Context(), control.query, nil)
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.index, control.query)
			}
		})

		t.Run("trailing trivia after the map keeps the literal map valid", func(t *testing.T) {
			for _, control := range trailingTrivia {
				// Each statement needs its own schema target and store: an index on
				// an already-indexed label and property is refused by name.
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), control.query, nil)
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.index, control.query)
			}
		})

		t.Run("trailing real syntax after the map is a SyntaxError without schema mutation", func(t *testing.T) {
			for _, query := range trailingSyntax {
				exec, _ := newTestExecutor(t)
				before := readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name")
				_, err := exec.Execute(t.Context(), query, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), query)
			}
		})

		// NORNICDB_CYPHER_QUERY_NORMALIZATION=false runs the statement as sent, so
		// the comment after the map reaches the tail unchanged. The tail reads
		// trivia through the statement's own reader, so admission does not depend
		// on the canonical pass having erased the comment.
		t.Run("trailing trivia after the map survives uncanonicalized text", func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			for _, control := range trailingTrivia {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), control.query, nil)
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.index, control.query)
			}
			exec, _ := newTestExecutor(t)
			before := readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name")
			_, err := exec.Execute(t.Context(), "CREATE INDEX nor300_bare_comment FOR (n:D) ON (n.p) OPTIONS // note", nil)
			requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", "bare OPTIONS with a comment")
			require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"))
		})
	})
}

func TestNOR300CreateConstraintOptionsRequiresMap(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		// Every FOR ... REQUIRE shape whose OPTIONS tail the shared constraint
		// head admits: a constraint and its owned property index are schema
		// objects, so a bare OPTIONS created both.
		missingMap := []string{
			"CREATE CONSTRAINT nor300c_unique FOR (n:NOR300C) REQUIRE n.p IS UNIQUE OPTIONS",
			"CREATE CONSTRAINT nor300c_exists FOR (n:NOR300C) REQUIRE n.p IS NOT NULL OPTIONS",
			"CREATE CONSTRAINT nor300c_type FOR (n:NOR300C) REQUIRE n.p IS TYPED STRING OPTIONS",
			"CREATE CONSTRAINT nor300c_nodekey FOR (n:NOR300C) REQUIRE (n.a, n.b) IS NODE KEY OPTIONS",
		}
		accepted := []struct{ query, constraint string }{
			{"CREATE CONSTRAINT nor300c_map FOR (n:NOR300C) REQUIRE n.p IS UNIQUE OPTIONS { indexProvider: 'range-1.0' }", "nor300c_map"},
			{"CREATE CONSTRAINT nor300c_emptymap FOR (n:NOR300C) REQUIRE n.q IS UNIQUE OPTIONS {}", "nor300c_emptymap"},
			{"CREATE CONSTRAINT nor300c_tight FOR (n:NOR300C) REQUIRE n.r IS UNIQUE OPTIONS{indexProvider: 'range-1.0'}", "nor300c_tight"},
			{"CREATE CONSTRAINT nor300c_omitted FOR (n:NOR300C) REQUIRE n.s IS UNIQUE", "nor300c_omitted"},
			{"CREATE CONSTRAINT nor300c_nodekey_map FOR (n:NOR300C) REQUIRE (n.c, n.d) IS NODE KEY OPTIONS {}", "nor300c_nodekey_map"},
		}
		// The same trailing-trivia contract the index forms carry: the map is the
		// OPTIONS payload, and what ends the statement after it is not.
		trailingTrivia := []struct{ query, constraint string }{
			{"CREATE CONSTRAINT nor300c_line FOR (n:NOR300C) REQUIRE n.t IS UNIQUE OPTIONS {} // note", "nor300c_line"},
			{"CREATE CONSTRAINT nor300c_block FOR (n:NOR300C) REQUIRE n.u IS UNIQUE OPTIONS { indexProvider: 'range-1.0' } /* note */", "nor300c_block"},
			{"CREATE CONSTRAINT nor300c_nodekey_trivia FOR (n:NOR300C) REQUIRE (n.e, n.f) IS NODE KEY OPTIONS {} // note", "nor300c_nodekey_trivia"},
			{"CREATE CONSTRAINT nor300c_delimiter FOR (n:NOR300C) REQUIRE n.v IS NOT NULL OPTIONS {};", "nor300c_delimiter"},
		}
		trailingSyntax := []string{
			"CREATE CONSTRAINT nor300c_junk FOR (n:NOR300C) REQUIRE n.w IS UNIQUE OPTIONS {} junk",
			"CREATE CONSTRAINT nor300c_string FOR (n:NOR300C) REQUIRE n.x IS UNIQUE OPTIONS 'x'",
		}

		t.Run("missing map is a SyntaxError without schema mutation", func(t *testing.T) {
			for _, query := range missingMap {
				exec, _ := newTestExecutor(t)
				before := readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name")
				_, err := exec.Execute(t.Context(), query, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), query)
			}
		})

		t.Run("valid map and omitted OPTIONS still create the constraint", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			for _, control := range accepted {
				_, err := exec.Execute(t.Context(), control.query, nil)
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), control.constraint, control.query)
			}
		})

		t.Run("trailing trivia after the map keeps the literal map valid", func(t *testing.T) {
			for _, control := range trailingTrivia {
				// Each statement needs its own schema target and store: a constraint
				// on an already-constrained label and property is refused by name.
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), control.query, nil)
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), control.constraint, control.query)
			}
		})

		t.Run("trailing real syntax after the map is a SyntaxError without schema mutation", func(t *testing.T) {
			for _, query := range trailingSyntax {
				exec, _ := newTestExecutor(t)
				before := readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name")
				_, err := exec.Execute(t.Context(), query, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), query)
			}
		})

		t.Run("trailing trivia after the map survives uncanonicalized text", func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			for _, control := range trailingTrivia {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), control.query, nil)
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), control.constraint, control.query)
			}
			exec, _ := newTestExecutor(t)
			before := readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name")
			_, err := exec.Execute(t.Context(), "CREATE CONSTRAINT nor300c_bare_comment FOR (n:NOR300C) REQUIRE n.y IS UNIQUE OPTIONS /* note */", nil)
			requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", "bare OPTIONS with a comment")
			require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"))
		})

		// The three constraint shapes below read their one OPTIONS tail through the
		// same owner, so they admit exactly the same tails: an absent tail, a
		// payload, and a payload followed by trivia. A bare OPTIONS keyword has no
		// payload and stays a SyntaxError for all three.
		exec, _ := newTestExecutor(t)
		shapes := []struct {
			owner string
			base  string
			parse func(string) (bool, error)
		}{
			{
				owner: "node key",
				base:  "CREATE CONSTRAINT nor300_node_key FOR (n:Legacy) REQUIRE (n.tenant, n.id) IS NODE KEY",
				parse: func(query string) (bool, error) {
					parsed, err := exec.parseCreateConstraintNodeKeyDDL(t.Context(), query)
					return len(parsed.properties) == 2, err
				},
			},
			{
				owner: "property type",
				base:  "CREATE CONSTRAINT nor300_type FOR (n:Legacy) REQUIRE n.active IS TYPED BOOLEAN",
				parse: func(query string) (bool, error) {
					parsed, err := exec.parseCreateConstraintTypeDDL(t.Context(), query)
					return parsed.property == "active", err
				},
			},
			{
				owner: "single property",
				base:  "CREATE CONSTRAINT nor300_unique FOR (n:Legacy) REQUIRE n.email IS UNIQUE",
				parse: func(query string) (bool, error) {
					parsed, err := exec.parseCreateConstraintSimplePropertyDDL(t.Context(), query)
					return parsed.property == "email", err
				},
			},
		}
		for _, branch := range shapes {
			t.Run(branch.owner+" OPTIONS tail", func(t *testing.T) {
				_, err := branch.parse(branch.base + " OPTIONS")
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", branch.base+" OPTIONS")
				for _, tail := range []string{
					"",
					" OPTIONS {}",
					" OPTIONS {indexProvider: 'range-1.0'}",
					" OPTIONS {} // note",
					" OPTIONS {} /* note */",
				} {
					parsed, err := branch.parse(branch.base + tail)
					require.NoError(t, err, branch.base+tail)
					require.True(t, parsed, branch.base+tail)
				}
			})
		}
	})
}

// schemaIndexOptions reads the options column SHOW INDEXES reports for one
// index: how a caller observes the configuration an index actually applied.
func schemaIndexOptions(t *testing.T, exec *StorageExecutor, name string) map[string]interface{} {
	t.Helper()
	result, err := exec.Execute(t.Context(), "SHOW INDEXES YIELD name, options RETURN name, options ORDER BY name", nil)
	require.NoError(t, err)
	for _, row := range result.Rows {
		if indexName, _ := row[0].(string); indexName == name {
			options, _ := row[1].(map[string]interface{})
			return options
		}
	}
	t.Fatalf("index %q is not listed by SHOW INDEXES", name)
	return nil
}

// requireVectorIndexConfig asserts the vector configuration SHOW INDEXES
// reports for an index: dimensions as an integer and the distance function as
// the schema stores it (upper case).
func requireVectorIndexConfig(t *testing.T, options map[string]interface{}, dimensions int64, similarity string) {
	t.Helper()
	config, ok := options["indexConfig"].(map[string]interface{})
	require.True(t, ok, "%#v", options)
	require.Equal(t, dimensions, config["vector.dimensions"], "%#v", options)
	require.Equal(t, similarity, config["vector.similarity_function"], "%#v", options)
}

// NOR-300 follow-up: the OPTIONS payload is one map literal or one parameter
// reference. Neo4j reads OPTIONS $options exactly as OPTIONS {…} and applies
// the configuration it carries, so both parser configurations must admit it
// through the public Execute path, bind it at the execution boundary, and
// reject an unusable binding without changing the schema.
func TestNOR300IndexOptionsBindParameterPayload(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		// One shape per index form that admits an OPTIONS tail. Each case uses
		// its own store and targets a distinct label and property.
		accepted := []struct {
			name, query, index string
			opts               map[string]interface{}
		}{
			{"generic index", "CREATE INDEX p_generic FOR (n:PG) ON (n.p) OPTIONS $opts", "p_generic", map[string]interface{}{}},
			{"range index", "CREATE RANGE INDEX p_range FOR (n:PR) ON (n.p) OPTIONS $opts", "p_range", map[string]interface{}{"indexProvider": "range-1.0"}},
			{"relationship index", "CREATE INDEX p_rel FOR ()-[r:PREL]-() ON (r.p) OPTIONS $opts", "p_rel", map[string]interface{}{}},
			{"fulltext index", "CREATE FULLTEXT INDEX p_full FOR (n:PF) ON EACH [n.t] OPTIONS $opts", "p_full", map[string]interface{}{"indexProvider": "fulltext-1.0"}},
			{"text index", "CREATE TEXT INDEX p_text FOR (n:PT) ON (n.t) OPTIONS $opts", "p_text", map[string]interface{}{}},
			{"point index", "CREATE POINT INDEX p_point FOR (n:PP) ON (n.loc) OPTIONS $opts", "p_point", map[string]interface{}{}},
			{"vector index", "CREATE VECTOR INDEX p_vector FOR (n:PV) ON (n.e) OPTIONS $opts", "p_vector",
				map[string]interface{}{"indexConfig": map[string]interface{}{"vector.dimensions": int64(3), "vector.similarity_function": "cosine"}}},
		}
		t.Run("a bound parameter map creates the schema object", func(t *testing.T) {
			for _, control := range accepted {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), control.query, map[string]interface{}{"opts": control.opts})
				require.NoError(t, err, control.name)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.index, control.name)
			}
		})

		// The legacy ON :Label(p) head is a native-parser form: the ANTLR
		// grammar never admitted it, which is a separate, pre-existing
		// divergence. Its OPTIONS payload binds through the same owner.
		if !config.IsANTLRParser() {
			t.Run("the legacy ON form binds the same payload", func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), "CREATE INDEX p_legacy ON :PL(p) OPTIONS $opts", map[string]interface{}{"opts": map[string]interface{}{}})
				require.NoError(t, err)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), "p_legacy")
			})
		}

		// The payload is the whole OPTIONS argument: what ends the statement
		// after it is statement framing, read by the shared trivia reader.
		trivia := []struct{ query, index string }{
			{"CREATE INDEX p_line FOR (n:P1) ON (n.p) OPTIONS $opts // trailing note", "p_line"},
			{"CREATE INDEX p_block FOR (n:P2) ON (n.p) OPTIONS $opts /* trailing note */", "p_block"},
			{"CREATE INDEX p_delimiter FOR (n:P3) ON (n.p) OPTIONS $opts;", "p_delimiter"},
			{"CREATE INDEX p_comment_delimiter FOR (n:P4) ON (n.p) OPTIONS $opts /* note */ ;", "p_comment_delimiter"},
		}
		t.Run("trailing trivia after a parameter payload keeps it valid", func(t *testing.T) {
			for _, control := range trivia {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), control.query, map[string]interface{}{"opts": map[string]interface{}{}})
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.index, control.query)
			}
		})
		t.Run("trailing trivia after a parameter payload survives uncanonicalized text", func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			for _, control := range trivia {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), control.query, map[string]interface{}{"opts": map[string]interface{}{}})
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.index, control.query)
			}
		})

		// Real trailing syntax is not statement framing.
		trailingSyntax := []string{
			"CREATE INDEX p_junk FOR (n:PJ) ON (n.p) OPTIONS $opts junk",
			"CREATE INDEX p_after FOR (n:PA) ON (n.p) OPTIONS $opts MATCH (n) RETURN n",
			"CREATE INDEX p_double FOR (n:PD) ON (n.p) OPTIONS $a $b",
		}
		t.Run("real trailing syntax after a parameter payload is a SyntaxError without a schema change", func(t *testing.T) {
			for _, query := range trailingSyntax {
				exec, _ := newTestExecutor(t)
				before := readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name")
				_, err := exec.Execute(t.Context(), query, map[string]interface{}{"opts": map[string]interface{}{}, "a": map[string]interface{}{}, "b": map[string]interface{}{}})
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), query)
			}
		})

		t.Run("a quoted parameter name reads as the canonical scanner reads it", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), "CREATE INDEX p_quoted FOR (n:PQ) ON (n.p) OPTIONS $`opts name`", map[string]interface{}{
				"opts name": map[string]interface{}{"indexProvider": "range-1.0"},
			})
			require.NoError(t, err)
			require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), "p_quoted")
		})

		t.Run("an inline parameter and a pure function value inside a literal map are admitted", func(t *testing.T) {
			for _, query := range []string{
				"CREATE INDEX p_inline FOR (n:PI) ON (n.p) OPTIONS {indexProvider: $provider}",
				"CREATE INDEX p_inline FOR (n:PI) ON (n.p) OPTIONS {indexProvider: toLower('RANGE-1.0')}",
			} {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), query, map[string]interface{}{"provider": "range-1.0"})
				require.NoError(t, err, query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), "p_inline", query)
			}
		})

		// Neo4j classifies an unusable payload as InvalidArguments — not as a
		// type error — and reports a parameter the statement needs but wasn't
		// given as ParameterMissing. Neither mutates the schema.
		rejected := []struct {
			name  string
			opts  map[string]interface{}
			bound bool
			code  string
		}{
			{"wrong type", map[string]interface{}{"opts": "bad"}, true, "Neo.ClientError.General.InvalidArguments"},
			{"null", map[string]interface{}{"opts": nil}, true, "Neo.ClientError.General.InvalidArguments"},
			{"list", map[string]interface{}{"opts": []interface{}{"a"}}, true, "Neo.ClientError.General.InvalidArguments"},
			{"missing", nil, false, "Neo.ClientError.Statement.ParameterMissing"},
		}
		t.Run("an unusable parameter payload is rejected without a schema change", func(t *testing.T) {
			for _, control := range rejected {
				exec, _ := newTestExecutor(t)
				before := readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name")
				var params map[string]interface{}
				if control.bound {
					params = control.opts
				}
				_, err := exec.Execute(t.Context(), "CREATE INDEX p_reject FOR (n:PX) ON (n.p) OPTIONS $opts", params)
				requireErrorStatus(t, err, control.code, control.name)
				require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.name)
			}
		})

		t.Run("IF NOT EXISTS reports an unusable payload", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), "CREATE INDEX p_exists FOR (n:PX) ON (n.p)", nil)
			require.NoError(t, err)
			_, err = exec.Execute(t.Context(), "CREATE INDEX p_exists IF NOT EXISTS FOR (n:PX) ON (n.p) OPTIONS $opts", map[string]interface{}{"opts": "bad"})
			requireErrorStatus(t, err, "Neo.ClientError.General.InvalidArguments", "IF NOT EXISTS with a wrong-typed payload")
		})

		// The payload is read from this execution's parameters: a value bound
		// to an earlier execution of the same statement is never reused.
		t.Run("a parameter value is not retained across executions", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			query := "CREATE VECTOR INDEX p_repeat IF NOT EXISTS FOR (n:PRP) ON (n.e) OPTIONS $opts"
			_, err := exec.Execute(t.Context(), query, map[string]interface{}{"opts": map[string]interface{}{
				"indexConfig": map[string]interface{}{"vector.dimensions": int64(4)},
			}})
			require.NoError(t, err)
			_, err = exec.Execute(t.Context(), query, map[string]interface{}{"opts": "bad"})
			requireErrorStatus(t, err, "Neo.ClientError.General.InvalidArguments", query)
		})

		// The supported vector configuration must actually apply, from a
		// parameter map and from an inline parameter alike.
		t.Run("vector configuration declared through a parameter map is applied", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), "CREATE VECTOR INDEX p_cfg FOR (n:PV1) ON (n.e) OPTIONS $opts", map[string]interface{}{"opts": map[string]interface{}{
				"indexConfig": map[string]interface{}{"vector.dimensions": int64(7), "vector.similarity_function": "euclidean"},
			}})
			require.NoError(t, err)
			requireVectorIndexConfig(t, schemaIndexOptions(t, exec, "p_cfg"), 7, "EUCLIDEAN")
		})
		t.Run("vector configuration declared through an inline parameter is applied", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), "CREATE VECTOR INDEX p_cfg_inline FOR (n:PV2) ON (n.e) OPTIONS {indexConfig: {`vector.dimensions`: $dims}}", map[string]interface{}{
				"dims": int64(5),
			})
			require.NoError(t, err)
			// The similarity function stays at its default.
			requireVectorIndexConfig(t, schemaIndexOptions(t, exec, "p_cfg_inline"), 5, "COSINE")
		})
		t.Run("a wrong-typed vector configuration value is rejected without an index", func(t *testing.T) {
			for _, value := range []interface{}{"3", 3.5, true, nil} {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), "CREATE VECTOR INDEX p_cfg_bad FOR (n:PV3) ON (n.e) OPTIONS $opts", map[string]interface{}{"opts": map[string]interface{}{
					"indexConfig": map[string]interface{}{"vector.dimensions": value},
				}})
				requireErrorStatus(t, err, "Neo.ClientError.General.InvalidArguments", fmt.Sprintf("dimensions %#v", value))
				require.NotContains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), "p_cfg_bad")
			}
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), "CREATE VECTOR INDEX p_cfg_bad FOR (n:PV3) ON (n.e) OPTIONS $opts", map[string]interface{}{"opts": map[string]interface{}{
				"indexConfig": map[string]interface{}{"vector.similarity_function": int64(7)},
			}})
			requireErrorStatus(t, err, "Neo.ClientError.General.InvalidArguments", "similarity function")
			require.NotContains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), "p_cfg_bad")
		})
	})
}

// NOR-300 follow-up: CREATE CONSTRAINT admits the same OPTIONS payload, so a
// parameter map must be admitted and must be a map at execution; a payload
// that isn't one is InvalidArguments and creates nothing.
func TestNOR300ConstraintOptionsBindParameterPayload(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		accepted := []struct{ name, query, constraint string }{
			{"uniqueness", "CREATE CONSTRAINT c_unique FOR (n:CU) REQUIRE n.p IS UNIQUE OPTIONS $opts", "c_unique"},
			{"existence", "CREATE CONSTRAINT c_exists FOR (n:CE) REQUIRE n.p IS NOT NULL OPTIONS $opts", "c_exists"},
			{"node key", "CREATE CONSTRAINT c_nodekey FOR (n:CK) REQUIRE (n.a, n.b) IS NODE KEY OPTIONS $opts", "c_nodekey"},
			{"property type", "CREATE CONSTRAINT c_type FOR (n:CT) REQUIRE n.p IS TYPED STRING OPTIONS $opts", "c_type"},
			{"relationship uniqueness", "CREATE CONSTRAINT c_rel FOR ()-[r:CR]-() REQUIRE r.p IS UNIQUE OPTIONS $opts", "c_rel"},
		}
		for _, control := range accepted {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), control.query, map[string]interface{}{"opts": map[string]interface{}{}})
			require.NoError(t, err, control.name)
			require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), control.constraint, control.name)
		}

		t.Run("an inline parameter inside a literal constraint map is admitted", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), "CREATE CONSTRAINT c_inline FOR (n:CI) REQUIRE n.p IS UNIQUE OPTIONS {indexProvider: $provider}", map[string]interface{}{
				"provider": "range-1.0",
			})
			require.NoError(t, err)
			require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), "c_inline")
		})

		rejected := []struct {
			name  string
			opts  map[string]interface{}
			bound bool
			code  string
		}{
			{"wrong type", map[string]interface{}{"opts": "bad"}, true, "Neo.ClientError.General.InvalidArguments"},
			{"null", map[string]interface{}{"opts": nil}, true, "Neo.ClientError.General.InvalidArguments"},
			{"missing", nil, false, "Neo.ClientError.Statement.ParameterMissing"},
		}
		for _, control := range rejected {
			exec, _ := newTestExecutor(t)
			before := readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name")
			var params map[string]interface{}
			if control.bound {
				params = control.opts
			}
			_, err := exec.Execute(t.Context(), "CREATE CONSTRAINT c_reject FOR (n:CX) REQUIRE n.p IS UNIQUE OPTIONS $opts", params)
			requireErrorStatus(t, err, control.code, control.name)
			require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), control.name)
		}
	})
}

// EXPLAIN compiles statement metadata, so it must never bind the OPTIONS
// payload: a missing or wrong-typed parameter may not surface there, and no
// schema object may be created. Explaining a schema command is not this
// change's subject, so the control compares the explained statement's outcome
// with and without the payload instead of asserting a shape of its own.
func TestNOR300SchemaOptionsExplainDoesNotBind(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		controls := []struct{ plain, payload, index string }{
			{"EXPLAIN CREATE INDEX e_plain FOR (n:EE) ON (n.p)", "EXPLAIN CREATE INDEX e_plain FOR (n:EE) ON (n.p) OPTIONS $opts", "e_plain"},
			{"EXPLAIN CREATE VECTOR INDEX e_vector FOR (n:EV) ON (n.e)", "EXPLAIN CREATE VECTOR INDEX e_vector FOR (n:EV) ON (n.e) OPTIONS $opts", "e_vector"},
		}
		for _, control := range controls {
			exec, _ := newTestExecutor(t)
			_, plainErr := exec.Execute(t.Context(), control.plain, nil)
			require.NotContains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.index, control.plain)

			// No parameters at all, then a payload of the wrong type: neither
			// may be read while the plan is compiled.
			payloadParameters := []map[string]interface{}{nil, map[string]interface{}{"opts": "bad"}}
			for _, params := range payloadParameters {
				result, err := exec.Execute(t.Context(), control.payload, params)
				if plainErr != nil {
					require.Error(t, err, control.payload)
					require.Equal(t, plainErr.Error(), err.Error(), control.payload)
					if code, _ := nornicerrors.Neo4jStatus(err); code != "" {
						require.NotEqual(t, "Neo.ClientError.Statement.ParameterMissing", code, control.payload)
						require.NotEqual(t, "Neo.ClientError.General.InvalidArguments", code, control.payload)
					}
				} else {
					require.NoError(t, err, control.payload)
					require.NotNil(t, result, control.payload)
				}
			}
			require.NotContains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.index, control.payload)
		}
	})
}

// Private-only probes of the shared owners: admission of the payload forms and
// the typed binding matrix, which the public paths above reach through
// Execute. These assertions are not parser-dependent.
func TestSchemaOptionsPayloadOwners(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	admitted := []struct{ tail, literal, parameter string }{
		{"", "", ""},
		{"OPTIONS {}", "{}", ""},
		{"OPTIONS {indexProvider: 'range-1.0'}", "{indexProvider: 'range-1.0'}", ""},
		{"OPTIONS { indexConfig: { `vector.dimensions`: 3 } } // note", "{ indexConfig: { `vector.dimensions`: 3 } }", ""},
		{"OPTIONS {} ;", "{}", ""},
		{"OPTIONS $opts", "", "opts"},
		{"OPTIONS $`opts name`", "", "opts name"},
		{"OPTIONS $opts /* note */", "", "opts"},
		{"OPTIONS $opts;", "", "opts"},
		{"OPTIONS $opts // note", "", "opts"},
	}
	for _, control := range admitted {
		options, err := schemaOptionsTail(control.tail)
		require.NoError(t, err, control.tail)
		require.Equal(t, control.literal, options.literal, control.tail)
		require.Equal(t, control.parameter, options.parameter, control.tail)
	}

	rejected := []string{
		"OPTIONS",
		"OPTIONS // note",
		"OPTIONS 'x'",
		"OPTIONS 1",
		"OPTIONS {} junk",
		"OPTIONS {} }",
		"OPTIONS {indexProvider: 'range-1.0'",
		"OPTIONS $opts junk",
		"OPTIONS $opts $more",
		"OPTIONS $",
		"OPTIONS $(dynamic)",
		"OPTIONS $opts.f",
		"TRAILING",
	}
	for _, tail := range rejected {
		_, err := schemaOptionsTail(tail)
		requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", tail)
	}

	// Binding: a literal map is evaluated by the statement evaluator, a
	// parameter is read from this statement, and a non-map value is
	// InvalidArguments.
	options, err := schemaOptionsTail("OPTIONS {indexProvider: toLower('RANGE-1.0')}")
	require.NoError(t, err)
	values, err := exec.schemaOptionMap(t.Context(), options, "range index")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"indexProvider": "range-1.0"}, values)

	bound := withQueryParams(t.Context(), map[string]interface{}{"opts": map[string]interface{}{"indexProvider": "range-1.0"}})
	options, err = schemaOptionsTail("OPTIONS $opts")
	require.NoError(t, err)
	values, err = exec.schemaOptionMap(bound, options, "range index")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"indexProvider": "range-1.0"}, values)

	for _, value := range []interface{}{"bad", int64(1), true, []interface{}{}} {
		_, err := schemaOptionMapValue(value, "range index")
		requireErrorStatus(t, err, "Neo.ClientError.General.InvalidArguments", fmt.Sprintf("%#v", value))
	}
	_, err = schemaOptionMapValue(nil, "range index")
	requireErrorStatus(t, err, "Neo.ClientError.General.InvalidArguments", "null")
}

func TestSchemaVectorOptionsOwner(t *testing.T) {
	dimensions, similarity, err := schemaVectorOptions(nil, 384, "cosine")
	require.NoError(t, err)
	require.Equal(t, 384, dimensions)
	require.Equal(t, "cosine", similarity)

	dimensions, similarity, err = schemaVectorOptions(map[string]interface{}{
		"indexConfig": map[string]interface{}{"vector.dimensions": int64(3), "vector.similarity_function": "euclidean"},
	}, 384, "cosine")
	require.NoError(t, err)
	require.Equal(t, 3, dimensions)
	require.Equal(t, "euclidean", similarity)

	// An absent option keeps its default; an empty similarity name does too.
	dimensions, similarity, err = schemaVectorOptions(map[string]interface{}{
		"indexConfig": map[string]interface{}{"vector.similarity_function": ""},
	}, 384, "cosine")
	require.NoError(t, err)
	require.Equal(t, 384, dimensions)
	require.Equal(t, "cosine", similarity)

	for _, values := range []map[string]interface{}{
		{"indexConfig": "nope"},
		{"indexConfig": nil},
		{"indexConfig": map[string]interface{}{"vector.dimensions": "3"}},
		{"indexConfig": map[string]interface{}{"vector.dimensions": 3.5}},
		{"indexConfig": map[string]interface{}{"vector.similarity_function": int64(7)}},
	} {
		_, _, err := schemaVectorOptions(values, 384, "cosine")
		requireErrorStatus(t, err, "Neo.ClientError.General.InvalidArguments", fmt.Sprintf("%#v", values))
	}
}
