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
