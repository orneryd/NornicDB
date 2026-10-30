package statementfactstest

import (
	"fmt"
	"strings"
	"testing"
)

// ProjectionPermutation keeps the extension and its standard Cypher oracle
// explicit. The equivalent adds a leading legacy import, never changes an item.
type ProjectionPermutation struct {
	Case
	OracleQuery string
	Rows        [][]any
}

func ProjectionPermutationCases() []ProjectionPermutation {
	var samples []ProjectionPermutation
	for nameIndex, written := range []string{"value", "``", "`WHERE`"} {
		for _, aggregate := range []bool{false, true} {
			marker := int64(3)
			items := []string{"2 AS " + written, written + "+1 AS y", "3 AS marker"}
			if aggregate {
				items[1], items[2], marker = "sum("+written+"+1) AS y", "count(*) AS marker", 1
			}
			for siteIndex, site := range []string{"with", "return", "call_tail_with", "call_tail_return", "union", "union_all", "where", "order"} {
				for _, empty := range []bool{false, true} {
					for permutation, order := range [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
						project := strings.Join([]string{items[order[0]], items[order[1]], items[order[2]]}, ", ")
						prefix := "WITH 7 AS " + written
						if empty {
							prefix = "UNWIND [] AS " + written
						}
						body := "UNWIND [1] AS seed WITH " + project + " RETURN y AS x, marker AS m"
						rows := [][]any{{int64(8), marker}}
						switch site {
						case "call_tail_with":
							body = "UNWIND [1] AS seed CALL { RETURN 1 AS kept } WITH " + project + " RETURN y AS x, marker AS m"
						case "where":
							body = "UNWIND [1] AS seed WITH " + project + fmt.Sprintf(" WHERE %s=2 AND y=8 AND marker=%d RETURN y AS x, marker AS m", written, marker)
						case "order":
							body = "UNWIND [1] AS seed WITH " + project + " ORDER BY " + written + ", y DESC RETURN y AS x, marker AS m"
						case "union", "union_all":
							union := " UNION "
							if site == "union_all" {
								union = " UNION ALL "
								rows = append(rows, []any{int64(8), marker})
							}
							reversed := strings.Join([]string{items[order[2]], items[order[1]], items[order[0]]}, ", ")
							body += union + "UNWIND [1] AS seed WITH " + reversed + " RETURN marker AS m, y AS x"
						}
						query := prefix + " CALL { " + body + " } RETURN x, m"
						// Each UNION branch has its own standard legacy import.
						oracleBody := "WITH " + written + " " + body
						if site == "union" || site == "union_all" {
							union := " UNION "
							if site == "union_all" {
								union = " UNION ALL "
							}
							parts := strings.Split(body, union)
							oracleBody = "WITH " + written + " " + parts[0] + union + "WITH " + written + " " + parts[1]
						}
						oracle := prefix + " CALL { " + oracleBody + " } RETURN x, m"
						if site == "return" || site == "call_tail_return" {
							// Keep projected shadow names local, avoiding a CALL export
							// collision unrelated to simultaneous projection reads.
							middle := " UNWIND [1] AS seed "
							if site == "call_tail_return" {
								middle = " CALL { RETURN 1 AS seed } "
							}
							query = "CALL { WITH 7 AS " + written + middle + "RETURN " + project + " } RETURN y AS x, marker AS m"
							if empty {
								query = "UNWIND [] AS emptyInput " + query
							}
							oracle = query

						}
						if empty {
							rows = nil
						}
						samples = append(samples, ProjectionPermutation{Case: Case{Name: fmt.Sprintf("name%d/aggregate=%v/site%d_%s/empty=%v/permutation%d", nameIndex, aggregate, siteIndex, site, empty, permutation), Query: query, Columns: []string{"x", "m"}}, OracleQuery: oracle, Rows: rows})
					}
				}
			}
		}
	}
	return append(samples, projectionBranchScopeCases()...)
}

// A branch's legacy imports expire even when a sibling retains the same name.
// The reference makes implicit imports explicit without changing the projection
// under test. Scoped controls use the same retained lifetime as CALL (value).
func projectionBranchScopeCases() []ProjectionPermutation {
	var samples []ProjectionPermutation
	for _, mode := range []string{"legacy", "implicit", "scoped"} {
		for _, sibling := range []string{"legacy", "implicit", "scoped", "none"} {
			for _, union := range []string{"UNION", "UNION ALL"} {
				for _, reversed := range []bool{false, true} {
					for _, empty := range []bool{false, true} {
						for _, tail := range []string{"ORDER BY value", "WHERE value=7"} {
							for _, distinct := range []bool{false, true} {
								for _, itemOrder := range []bool{false, true} {
									items := []string{"count(*) AS total", "1 AS marker"}
									rows := [][]any{{int64(2)}}
									if distinct {
										items[0] = "seed AS total"
										rows = [][]any{{int64(1)}, {int64(2)}}
									}
									if itemOrder {
										items[0], items[1] = items[1], items[0]
									}
									projection := strings.Join(items, ", ")
									if distinct {
										projection = "DISTINCT " + projection
									}
									left := "UNWIND [1,2] AS seed WITH " + projection + " " + tail + " RETURN total AS x"
									oracleLeft := left
									frame := "CALL { "
									code := ""
									switch mode {
									case "legacy":
										left = "WITH value " + left
										oracleLeft = left
										code = "Neo.ClientError.Statement.SyntaxError"
									case "implicit":
										oracleLeft = "WITH value CALL (value) { " + left + " } RETURN x"
										// A weak bare-CALL input is readable in predicates, but
										// it is not an explicit retained import for aggregate
										// or DISTINCT ordering. Keep the release rejection.
										if strings.HasPrefix(tail, "ORDER BY") {
											code = "Neo.ClientError.Statement.SyntaxError"
											oracleLeft = "WITH value " + left
										}
									case "scoped":
										frame = "CALL (value) { "
										left = "WITH value " + left
										oracleLeft = left
									}
									right, oracleRight := "RETURN value AS x", "WITH value RETURN value AS x"
									value := int64(7)
									switch sibling {
									case "legacy":
										right = oracleRight
									case "scoped":
										right = "CALL (value) { RETURN value AS x } RETURN x"
										oracleRight = "WITH value " + right
									case "none":
										right, oracleRight, value = "RETURN 11 AS x", "RETURN 11 AS x", 11
									}
									rows = append(rows, []any{value})
									if reversed {
										left, right = right, left
										oracleLeft, oracleRight = oracleRight, oracleLeft
									}
									prefix := "WITH 7 AS value "
									if empty {
										prefix = "UNWIND [] AS value "
									}
									if empty || code != "" {
										rows = nil
									}
									samples = append(samples, ProjectionPermutation{Case: Case{
										Name:    fmt.Sprintf("branch_scope/%s/sibling=%s/%s/reversed=%v/empty=%v/tail=%s/distinct=%v/itemsReversed=%v", mode, sibling, union, reversed, empty, tail, distinct, itemOrder),
										Query:   prefix + frame + left + " " + union + " " + right + " } RETURN x ORDER BY x",
										Columns: []string{"x"}, Code: code,
									}, OracleQuery: prefix + frame + oracleLeft + " " + union + " " + oracleRight + " } RETURN x ORDER BY x", Rows: rows})
								}
							}
						}
					}
				}
			}
		}
	}
	return samples
}

func NewProjectionPermutationOracle(t *testing.T, samples []ProjectionPermutation) ([]Case, *ProjectionOracle) {
	oracle := NewProjectionOracle(t)
	if oracle == nil {
		oracle = &ProjectionOracle{}
	}
	oracle.referenceQueries = map[string]string{}
	oracle.expectedRows = map[string][][]any{}
	cases := make([]Case, 0, len(samples))
	for _, sample := range samples {
		cases = append(cases, sample.Case)
		oracle.referenceQueries[sample.Name] = sample.OracleQuery
		oracle.expectedRows[sample.Name] = sample.Rows
	}
	return cases, oracle
}
