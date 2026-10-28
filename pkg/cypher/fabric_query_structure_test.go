package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	grammar "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func fabricStructureRows(t *testing.T, rows [][]interface{}, want string) {
	t.Helper()
	encoded, err := json.Marshal(rows)
	require.NoError(t, err)
	require.JSONEq(t, want, string(encoded))
}

func fabricGrammarWords() []string {
	lexer := grammar.NewCypherLexer(antlr4.NewInputStream(""))
	var words []string
	for _, literal := range lexer.GetLiteralNames() {
		word := strings.Trim(literal, "'")
		if word == "" {
			continue
		}
		valid := true
		for _, r := range word {
			if r < 'A' || r > 'Z' {
				if r < 'a' || r > 'z' {
					valid = false
					break
				}
			}
		}
		if valid {
			words = append(words, strings.ToUpper(word))
		}
	}
	return words
}

// Every word read by Fabric's clause/modifier/import/write boundary, used in
// each projection and name position. Neo4j is the oracle for ambiguous bare
// names (including expression operators); quoted names are always controls.
func TestFabricQueryStructureGeneratedNames(t *testing.T) {
	ctx := context.Background()
	base := newFabricCallRowsExecutor(t)
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("generated oracle matrix requires --ref neo-ref (Neo4j 5.26.30)")
	}
	_, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (n:FabricStructureName) DETACH DELETE n", nil, neo4j.EagerResultTransformer)
	require.NoError(t, err)
	_, err = neo4j.ExecuteQuery(ctx, driver, "CREATE (:FabricStructureName {v:7})", nil, neo4j.EagerResultTransformer)
	require.NoError(t, err)
	_, err = base.Execute(ctx, "USE comp.one CREATE (:FabricStructureName {v:7})", nil)
	require.NoError(t, err)
	words := fabricGrammarWords()
	require.Greater(t, len(words), 100, "generate from the complete keyword vocabulary")
	contexts := []struct{ name, query string }{
		{"first_projection", "WITH 7 AS %[1]s RETURN %[1]s AS k"},
		{"distinct_projection", "WITH 7 AS %[1]s WITH DISTINCT %[1]s RETURN %[1]s AS k"},
		{"later_projection", "WITH 1 AS dummy, 7 AS %[1]s WITH dummy, %[1]s RETURN %[1]s AS k"},
		{"expression", "WITH 7 AS %[1]s RETURN 0 + %[1]s AS k"},
		{"function_argument", "WITH 7 AS %[1]s RETURN coalesce(%[1]s,0) AS k"},
		{"single_function_argument", "WITH 7 AS %[1]s RETURN toInteger(%[1]s) AS k"},
		{"predicate", "WITH 7 AS %[1]s WITH %[1]s WHERE %[1]s = 7 RETURN %[1]s AS k"},
		{"unwind_binding", "UNWIND [7] AS %[1]s RETURN %[1]s AS k"},
		{"foreach_binding", "FOREACH (%[1]s IN [7] | CREATE (:FabricStructureForeach {v:%[1]s})) RETURN 7 AS k"},
		{"comprehension_binding", "RETURN [%[1]s IN [7] | %[1]s][0] AS k"},
		{"reduce_accumulator", "RETURN reduce(%[1]s=0,x IN [7] | %[1]s+x) AS k"},
		{"reduce_element", "RETURN reduce(acc=0,%[1]s IN [7] | acc+%[1]s) AS k"},
		{"predicate_binding", "RETURN CASE WHEN any(%[1]s IN [7] WHERE %[1]s=7) THEN 7 ELSE 0 END AS k"},
		{"map_value", "WITH 7 AS %[1]s RETURN {v:%[1]s}.v AS k"},
		{"map_shorthand", "WITH {v:0} AS m,7 AS %[1]s RETURN m{%[1]s}.%[1]s AS k"},
		{"pattern_comprehension", "CREATE (s)-[:FabricCompRel]->(:FabricCompTarget {v:7}) RETURN [(s)-[:FabricCompRel]->(%[1]s) | %[1]s.v] AS k"},
		{"named_pattern_comprehension", "CREATE (s)-[:FabricCompRel]->(:FabricCompTarget {v:7}) RETURN [%[1]s = (s)-[:FabricCompRel]->(t) | length(%[1]s)] AS k"},
		{"property_and_map_key", "WITH {%[1]s:7} AS m RETURN m.%[1]s AS k"},
		{"map_projection", "WITH {%[1]s:7} AS m RETURN m{.%[1]s}.%[1]s AS k"},
		{"pattern_binding", "MATCH (%[1]s:FabricStructureName) RETURN %[1]s.v AS k"},
		{"path_binding", "MATCH %[1]s = (n:FabricStructureName) RETURN n.v AS k"},
		{"label_name", "CREATE (n:%[1]s {v:7}) RETURN n.v AS k"},
		{"relationship_type", "CREATE ()-[r:%[1]s {v:7}]->() RETURN r.v AS k"},
		{"relationship_binding", "CREATE ()-[%[1]s:FabricStructureRel {v:7}]->() RETURN %[1]s.v AS k"},
		{"set_map_target", "CREATE (%[1]s:FabricNameMutation) SET %[1]s += {v:7} RETURN %[1]s.v AS k"},
		{"set_label_target", "CREATE (%[1]s {v:7}) SET %[1]s:FabricNameMutation RETURN %[1]s.v AS k"},
		{"remove_label_target", "CREATE (%[1]s:FabricNameMutation {v:7}) REMOVE %[1]s:FabricNameMutation RETURN %[1]s.v AS k"},
		{"yield_alias", "CALL db.labels() YIELD label AS %[1]s WITH %[1]s WHERE %[1]s='FabricStructureName' RETURN 7 AS k"},
		{"order_operand", "WITH 7 AS %[1]s RETURN %[1]s AS k ORDER BY %[1]s DESC SKIP 0 LIMIT 1"},
	}
	for _, normalization := range []bool{true, false} {
		t.Run(fmt.Sprintf("normalization_%v", normalization), func(t *testing.T) {
			restore := func() {}
			if !normalization {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, word := range words {
				for _, quoted := range []bool{false, true} {
					name := strings.ToLower(word)
					if quoted {
						name = "`" + name + "`"
					}
					for _, position := range contexts {
						t.Run(fmt.Sprintf("%s/quoted_%v/%s", word, quoted, position.name), func(t *testing.T) {
							body := fmt.Sprintf(position.query, name)
							query := "USE comp CALL { USE comp.one " + body + " } RETURN k"
							fabricCompareGrammarCase(t, base, driver, query, strings.NewReplacer("USE comp.one ", "", "USE comp ", "").Replace(query))
						})
					}
					for _, position := range []struct{ name, query string }{
						{"import_after_distinct", "USE comp WITH 7 AS %[1]s WITH DISTINCT %[1]s CALL { WITH %[1]s USE comp.one RETURN %[1]s AS k } RETURN k"},
						{"call_scope", "USE comp WITH 7 AS %[1]s CALL (%[1]s) { USE comp.one RETURN %[1]s AS k } RETURN k"},
						{"call_scope_star", "USE comp WITH 7 AS %[1]s CALL (*) { USE comp.one RETURN %[1]s AS k } RETURN k"},
						{"dynamic_graph_argument", "USE comp WITH 'comp.one' AS %[1]s CALL { WITH %[1]s USE graph.byName(%[1]s) RETURN 7 AS k } RETURN k"},
						{"final_projection", "USE comp CALL { USE comp.one RETURN 7 AS %[1]s } RETURN %[1]s AS k"},
						{"final_distinct", "USE comp CALL { USE comp.one RETURN 7 AS %[1]s } RETURN DISTINCT %[1]s AS k"},
						{"final_order", "USE comp CALL { USE comp.one RETURN 7 AS %[1]s } RETURN %[1]s ORDER BY %[1]s DESC SKIP 0 LIMIT 1"},
					} {
						t.Run(fmt.Sprintf("%s/quoted_%v/%s", word, quoted, position.name), func(t *testing.T) {
							query := fmt.Sprintf(position.query, name)
							if position.name == "dynamic_graph_argument" && !quoted && (word == "ALL" || word == "DISTINCT") {
								// Both modifiers leave this selector with zero arguments. Community
								// gates dynamic USE; its scalar DISTINCT control instead reports
								// invalid modifier use before arity. Keep these error contexts separate.
								referenceError := "Insufficient parameters"
								if word == "DISTINCT" {
									referenceError = "Invalid use of DISTINCT with function 'byName'"
								}
								reference := "WITH 'comp.one' AS " + name + " RETURN graph.byName(" + name + ")"
								fabricCompareGrammarCase(t, base, driver, query, reference, "Insufficient parameters", referenceError)
								return
							}
							fabricCompareGrammarCase(t, base, driver, query, strings.NewReplacer("USE graph.byName("+name+") ", "", "USE comp.one ", "", "USE comp ", "").Replace(query))
						})
					}
				}
			}
		})
	}
}

func fabricCompareGrammarCase(t *testing.T, base *StorageExecutor, driver neo4j.DriverWithContext, query, reference string, wantError ...string) {
	t.Helper()
	ctx := context.Background()
	ref, referenceErr := neo4j.ExecuteQuery(ctx, driver, reference, nil, neo4j.EagerResultTransformer)
	if len(wantError) > 0 {
		// The first fragment is native; an optional second is the reference's
		// context-specific diagnostic, when its earlier validation differs.
		referenceError := wantError[0]
		if len(wantError) > 1 {
			referenceError = wantError[1]
		}
		require.ErrorContains(t, referenceErr, referenceError, "Neo4j argument/modifier control")
		var classified *neo4j.Neo4jError
		require.ErrorAs(t, referenceErr, &classified)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.Code)
	} else if referenceErr != nil {
		if e, ok := referenceErr.(*neo4j.Neo4jError); ok && (e.Code == "Neo.ClientError.Statement.SyntaxError" || e.Code == "Neo.ClientError.Statement.SemanticError") {
			t.Skipf("Neo4j does not admit this spelling as a variable in this context: %v", referenceErr)
		}
		require.NoError(t, referenceErr, "Neo4j oracle execution")
	}
	var want []byte
	if referenceErr == nil {
		rows := make([][]interface{}, 0, len(ref.Records))
		for _, row := range ref.Records {
			rows = append(rows, row.Values)
		}
		var err error
		want, err = json.Marshal(rows)
		require.NoError(t, err)
	}
	var columns []string
	if ref != nil {
		columns = ref.Keys
	}
	fabricAssertGrammarCase(t, base, query, columns, string(want), wantError...)
}

// fabricAssertGrammarCase owns the native implicit/explicit result contract.
// Reference callers supply observed outcomes; isolated tests supply fixed outcomes.
func fabricAssertGrammarCase(t *testing.T, base *StorageExecutor, query string, columns []string, want string, wantError ...string) {
	t.Helper()
	ctx := context.Background()
	for _, explicit := range []bool{false, true} {
		exec := base
		if explicit {
			exec = fabricCallRowsCompositeExecutor(t, base)
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		result, err := exec.Execute(ctx, query, nil)
		if explicit {
			_, endErr := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, endErr)
		}
		if len(wantError) > 0 {
			require.ErrorContains(t, err, wantError[0], "explicit=%v query=%s", explicit, query)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			continue
		}
		require.NoError(t, err, "explicit=%v query=%s", explicit, query)
		require.Equal(t, columns, result.Columns, "explicit=%v query=%s", explicit, query)
		fabricStructureRows(t, result.Rows, want)
	}
}

func TestFabricQueryStructureGeneratedSeparators(t *testing.T) {
	ctx := context.Background()
	base := newFabricCallRowsExecutor(t)
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("generated oracle matrix requires --ref neo-ref (Neo4j 5.26.30)")
	}
	for _, normalization := range []bool{true, false} {
		t.Run(fmt.Sprintf("normalization_%v", normalization), func(t *testing.T) {
			restore := func() {}
			if !normalization {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, separator := range []string{" ", "  ", "\n", "\t", " /* UNION CALL RETURN WITH USE */ ", " // UNION CALL USE\n"} {
				for _, shape := range []struct{ name, query string }{
					{"use_graph", "USE comp CALL { USE%sGRAPH comp.one RETURN 7 AS k } RETURN k"},
					{"with_distinct", "USE comp CALL { USE comp.one UNWIND [7,7] AS k WITH%sDISTINCT k RETURN k } RETURN k"},
					{"return_distinct", "USE comp CALL { USE comp.one UNWIND [7,7] AS k RETURN%sDISTINCT k } RETURN k"},
					{"optional_call", "USE comp OPTIONAL%sCALL { USE comp.one RETURN 7 AS k } RETURN k"},
					{"order_by", "USE comp CALL { USE comp.one UNWIND [1,3,2] AS k RETURN k } RETURN k ORDER%sBY k DESC SKIP 1 LIMIT 1"},
					{"union_all", "USE comp CALL { USE comp.one RETURN 1 AS k UNION%sALL USE comp.two RETURN 1 AS k } RETURN count(*) AS k"},
					{"optional_match", "USE comp CALL { USE comp.one OPTIONAL%sMATCH (n:MissingFabricStructureName) RETURN count(n) AS k } RETURN k"},
					{"is_not_null", "USE comp CALL { USE comp.one WITH 7 AS k WHERE k IS%sNOT%sNULL RETURN k } RETURN k"},
					{"starts_with", "USE comp CALL { USE comp.one WITH 'x' AS k WHERE k STARTS%sWITH 'x' RETURN 7 AS k } RETURN k"},
					{"ends_with", "USE comp CALL { USE comp.one WITH 'x' AS k WHERE k ENDS%sWITH 'x' RETURN 7 AS k } RETURN k"},
					{"detach_delete", "USE comp CALL { USE comp.one CREATE (n:FabricStructureDelete) DETACH%sDELETE n RETURN 7 AS k } RETURN k"},
					{"on_create_set", "USE comp CALL { USE comp.one MERGE (n:FabricStructureMerge {v:7}) ON%sCREATE%sSET n.v=7 ON%sMATCH%sSET n.v=7 RETURN n.v AS k } RETURN k"},
				} {
					t.Run(fmt.Sprintf("%s/%q", shape.name, separator), func(t *testing.T) {
						count := strings.Count(shape.query, "%s")
						args := make([]interface{}, count)
						for i := range args {
							args[i] = separator
						}
						query := fmt.Sprintf(shape.query, args...)
						reference := strings.NewReplacer("USE"+separator+"GRAPH comp.one ", "", "USE comp.one ", "", "USE comp.two ", "", "USE comp ", "").Replace(query)
						fabricCompareGrammarCase(t, base, driver, query, reference)
					})
				}
			}
		})
	}
	_ = ctx
}
