package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestKeywordNamedMutationTargetsPublic(t *testing.T) {
	for _, normalization := range []bool{true, false} {
		t.Run(fmt.Sprint("normalization_", normalization), func(t *testing.T) {
			restore := func() {}
			if !normalization {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, parser := range []string{"nornic", "antlr"} {
				t.Run(parser, func(t *testing.T) {
					old := config.GetParserType()
					config.SetParserType(parser)
					defer config.SetParserType(old)
					for _, route := range []string{"native", "constituent", "call"} {
						for _, explicit := range []bool{false, true} {
							for _, quoted := range []bool{false, true} {
								for _, tc := range []struct{ name, query string }{{"set", "CREATE (%[1]s:KeywordMutation) SET %[1]s += {v:7} RETURN %[1]s.v AS k, labels(%[1]s) AS ls"}, {"where", "CREATE (%[1]s {v:7}) SET %[1]s:KeywordMutation RETURN %[1]s.v AS k, labels(%[1]s) AS ls"}} {
									t.Run(fmt.Sprintf("%s/explicit_%v/quoted_%v/%s", route, explicit, quoted, tc.name), func(t *testing.T) {
										e := newFabricCallRowsExecutor(t)
										name := tc.name
										if quoted {
											name = "`" + name + "`"
										}
										q := fmt.Sprintf(tc.query, name)
										read := "MATCH (n:KeywordMutation) RETURN n.v AS k"
										switch route {
										case "constituent":
											q = "USE comp.one " + q
											read = "USE comp.one " + read
										case "call":
											q = "USE comp CALL { USE comp.one " + q + " } RETURN k,ls"
											read = "USE comp.one " + read
										}
										if explicit {
											_, err := e.Execute(t.Context(), "BEGIN", nil)
											require.NoError(t, err)
											t.Cleanup(func() {
												if e.txContext != nil && e.txContext.active {
													_, err := e.Execute(context.Background(), "ROLLBACK", nil)
													require.NoError(t, err)
												}
											})
										}
										r, err := e.Execute(t.Context(), q, nil)
										require.NoError(t, err)
										require.Equal(t, [][]interface{}{{int64(7), []interface{}{"KeywordMutation"}}}, r.Rows)
										if explicit {
											_, err = e.Execute(t.Context(), "COMMIT", nil)
											require.NoError(t, err)
										}
										r, err = e.Execute(t.Context(), read, nil)
										require.NoError(t, err)
										require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
									})
								}
							}
						}
					}
				})
			}
		})
	}
}
func TestKeywordSetAssignmentBodyAndUnaryRoles(t *testing.T) {
	for _, gap := range []string{" ", "/*c*/", " //c\n"} {
		body := "set" + gap + "+= {v:7}"
		require.Equal(t, body, collapseChainedSetClauses(body))
		require.Equal(t, body+", set.x=8", collapseChainedSetClauses(body+" SET set.x=8"))
	}
	for _, text := range []string{"RETURN +3", "RETURN -3", "SET +3", "SET -3", "set + /*c*/= {v:7}"} {
		end := strings.IndexByte(text, ' ')
		require.False(t, clauseKeywordNamedByContext(text, 0, end, text[:end]), text)
	}
	for _, text := range []string{"set += {v:7}", "return += {v:7}", "where += {v:7}"} {
		end := strings.IndexByte(text, ' ')
		require.True(t, clauseKeywordNamedByContext(text, 0, end, text[:end]), text)
	}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if !clauseKeywordNamedByContext("set += {v:7}", 0, 3, "SET") {
			panic("lost target")
		}
	}))
}
func TestElementPredicateSpansPreserveWhereBindings(t *testing.T) {
	for _, pattern := range []string{"(where {v:7})", "(`where` {v:7})", "(n {where:7})", "(n {v:'WHERE'})", "()", "( )", "(n:Label)", "(n:Label WHERE n.v=1", "[where:LINK {v:7}]", "[`where`:LINK {v:7}]"} {
		require.Empty(t, elementPredicateSpans(pattern, 0, len(pattern)), pattern)
	}
	for _, pattern := range []string{"(n WHERE n.v>0)", "(:Label WHERE x>0)", "[:LINK WHERE x>0]", "(n   WHERE n.v>0)", "(where WHERE where.v>0)", "(`where` WHERE `where`.v>0)", "[r:LINK WHERE r.v>0]", "[where:LINK WHERE where.v>0]"} {
		want := strings.LastIndex(pattern, "WHERE")
		require.Equal(t, [][2]int{{want, len(pattern) - 1}}, elementPredicateSpans(pattern, 0, len(pattern)), pattern)
		structure := matchPatternStructure(pattern)
		require.Equal(t, len(pattern), len(structure))
		require.Equal(t, pattern[:want], structure[:want])
		require.Equal(t, pattern[len(pattern)-1:], structure[len(structure)-1:])
	}
	pattern := "prefix (where WHERE where.v>0) suffix"
	open := strings.IndexByte(pattern, '(')
	close := strings.IndexByte(pattern, ')')
	require.Equal(t, [][2]int{{strings.Index(pattern, "WHERE"), close}}, elementPredicateSpans(pattern, open, close+1))
	scope := matchSemanticScope{}
	addMatchPatternBindingKinds(scope, "CREATE (where {v:7})")
	require.Equal(t, matchBindingNode, scope["where"])
}
