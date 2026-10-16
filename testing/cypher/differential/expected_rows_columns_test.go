package differential

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A collect() makes list values unordered, but never lets a value move to
// another column: [[1],3,2] is not [[1],2,3].
func TestIssueExpectedRowsKeepColumnPositionsWithCollect(t *testing.T) {
	query := "RETURN collect(1) AS xs, 2 AS a, 3 AS b"
	columns := []string{"xs", "a", "b"}
	right := Outcome{Columns: columns, Rows: [][]any{{[]any{1}, 2, 3}}}
	wrong := Outcome{Columns: columns, Rows: [][]any{{[]any{1}, 3, 2}}}
	issues := loadExpectedRowsCase(t, query, `,"expected_rows":[[[1],2,3]]`)
	for _, test := range []struct {
		name              string
		reference, actual Outcome
		wantError         bool
	}{
		{"correct", right, right, false},
		{"permuted actual", right, wrong, true},
		{"permuted reference", wrong, right, true},
		{"both permuted", wrong, wrong, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			pair := &Pair{
				Neo4j:    &fakeServer{answers: map[string]Outcome{query: test.reference}},
				NornicDB: &fakeServer{answers: map[string]Outcome{query: test.actual}},
			}
			_, err := RunIssues(context.Background(), issues, pair, Options{StatementTimeout: time.Second})
			if test.wantError {
				require.ErrorContains(t, err, "expected_rows")
			} else {
				require.NoError(t, err)
			}
		})
	}

	// The same rule holds for Bolt's typed representation.
	typed := func(value string) any { return map[string]any{"Integer": value} }
	bolt := func(first, second string) Outcome {
		return Outcome{Columns: columns, Rows: [][]any{{[]any{typed("1")}, typed(first), typed(second)}}}
	}
	var normalize func(any) (any, error)
	normalize = func(value any) (any, error) {
		switch v := value.(type) {
		case json.Number:
			return typed(v.String()), nil
		case []any:
			items := make([]any, len(v))
			for index, item := range v {
				items[index], _ = normalize(item)
			}
			return items, nil
		}
		return value, nil
	}
	normalizer := func(answer Outcome) Executor {
		return expectedValueServer{
			fakeServer: &fakeServer{answers: map[string]Outcome{query: answer}},
			normalize:  normalize,
		}
	}
	pair := &Pair{Neo4j: normalizer(bolt("2", "3")), NornicDB: normalizer(bolt("3", "2"))}
	_, err := RunIssues(context.Background(), issues, pair, Options{StatementTimeout: time.Second})
	require.ErrorContains(t, err, "expected_rows mismatch on NornicDB")
	pair.NornicDB = normalizer(bolt("2", "3"))
	_, err = RunIssues(context.Background(), issues, pair, Options{StatementTimeout: time.Second})
	require.NoError(t, err)
}

// The shared comparator, which differential agreement also uses, keeps
// column positions while list cells stay unordered.
func TestSameRowsKeepsColumnsWithUnorderedLists(t *testing.T) {
	columns := []string{"xs", "a", "b"}
	how := comparison{unorderedLists: true}
	reference := Outcome{Columns: columns, Rows: [][]any{{[]any{1, 2}, 2, 3}}}
	require.True(t, sameRows(reference, Outcome{Columns: columns, Rows: [][]any{{[]any{2, 1}, 2, 3}}}, how))
	require.False(t, sameRows(reference, Outcome{Columns: columns, Rows: [][]any{{[]any{1, 2}, 3, 2}}}, how))
	require.False(t, sameRows(reference, Outcome{Columns: columns, Rows: [][]any{{2, []any{1, 2}, 3}}}, how))
	ordered := comparison{ordered: true, tieKeys: []int{1}, unorderedLists: true}
	tied := Outcome{Columns: columns, Rows: [][]any{{[]any{1}, 1, 2}, {[]any{2}, 1, 3}}}
	require.True(t, sameRows(tied, Outcome{Columns: columns, Rows: [][]any{{[]any{2}, 1, 3}, {[]any{1}, 1, 2}}}, ordered))
	require.False(t, sameRows(tied, Outcome{Columns: columns, Rows: [][]any{{[]any{1}, 2, 1}, {[]any{2}, 3, 1}}}, ordered))
}
