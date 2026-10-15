package differential

import "fmt"

// ExpectedValueNormalizer adapts JSON oracle values to an Executor's Outcome
// representation. Routes that tag Bolt types reuse their existing result
// normalizer here. JSON routes need no adapter and keep number lexemes intact.
// Implementations must not mutate the input, which is shared by both servers.
type ExpectedValueNormalizer interface {
	NormalizeExpectedValue(value any) any
}

// assertExpectedRows checks the independent oracle before differential agreement
// can classify equal errors or equal wrong rows as a match. Returning a harness
// error also prevents known/unstable entries and ratchet updates from waiving it.
func (pair *Pair) assertExpectedRows(issue int, statement IssueStatement, reference, actual Outcome) error {
	if statement.ExpectedRows == nil {
		return nil
	}
	for _, backend := range []struct {
		name     string
		executor Executor
		outcome  Outcome
	}{{"Neo4j", pair.Neo4j, reference}, {"NornicDB", pair.NornicDB, actual}} {
		if !matchesExpectedRows(statement.Query, *statement.ExpectedRows, backend.executor, backend.outcome) {
			return fmt.Errorf("issue #%d statement %q expected_rows mismatch on %s: expected %s, got %s",
				issue, statement.ID, backend.name, mustJSON(*statement.ExpectedRows), mustJSON(backend.outcome))
		}
	}
	return nil
}

func matchesExpectedRows(query string, expected [][]any, executor Executor, actual Outcome) bool {
	if actual.Failed() {
		return false
	}
	normalizer, normalize := executor.(ExpectedValueNormalizer)
	rows := make([][]any, len(expected))
	for index, row := range expected {
		// Validate widths before returned ORDER BY keys can index a row.
		if row == nil || len(row) != len(actual.Columns) {
			return false
		}
		rows[index] = append([]any{}, row...)
		if normalize {
			for column, value := range row {
				rows[index][column] = normalizer.NormalizeExpectedValue(value)
			}
		}
	}
	for _, row := range actual.Rows {
		if len(row) != len(actual.Columns) {
			return false
		}
	}
	shape := returnShape(query, actual.Columns)
	// An explicit oracle checks values even for an arbitrary LIMIT/SKIP. Keep
	// the existing ordering, tie, collection and token-list comparison rules.
	return sameRows(Outcome{Columns: actual.Columns, Rows: rows}, actual, comparison{
		ordered:        orderBy.MatchString(query),
		tieKeys:        shape.sortKeys,
		unorderedLists: unorderedCollect(query),
		tokenColumns:   shape.tokenColumns,
	})
}
