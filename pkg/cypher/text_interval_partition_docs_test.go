package cypher

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Execute the published statement after MATCH, with no test-only WITH bindings.
func documentedTextIntervalPartition(t testing.TB) string {
	t.Helper()
	doc, err := os.ReadFile("../../docs/api-reference/cypher-functions/README.md")
	require.NoError(t, err)
	_, statement, ok := strings.Cut(strings.ReplaceAll(string(doc), "\r\n", "\n"), "// [start, end) split into the intervals' clipped parts with their role, gaps as 'own'\n")
	require.True(t, ok, "partition recipe must be documented")
	statement, _, ok = strings.Cut(statement, "\n\n// The 1-based")
	require.True(t, ok, "partition recipe must end before the line-position recipe")
	return "\n" + statement
}

func TestTextIntervalPartitionDocumentedStatement(t *testing.T) {
	exec := newTextIntervalExecutor(t)
	row := textIntervalRow(t, exec, documentedTextIntervalPartition(t), map[string]interface{}{"start": int64(5), "end": int64(30)})
	require.Equal(t, []interface{}{textPart(5, 11, "own"), textPart(11, 25, "quoted"), textPart(25, 26, "own"), textPart(26, 30, "own")}, row[0])
}

func TestTextIntervalPartitionOrderAndOverlap(t *testing.T) {
	for _, tc := range []struct {
		name   string
		starts []interface{}
		ends   []interface{}
		roles  []interface{}
		want   []interface{}
	}{
		{"nested", []interface{}{int64(0), int64(5)}, []interface{}{int64(10), int64(7)}, []interface{}{"first", "second"}, []interface{}{textPart(0, 10, "first")}},
		{"unordered nested", []interface{}{int64(5), int64(0)}, []interface{}{int64(7), int64(10)}, []interface{}{"second", "first"}, []interface{}{textPart(0, 10, "first")}},
		{"overlapping suffix", []interface{}{int64(0), int64(5)}, []interface{}{int64(7), int64(10)}, []interface{}{"first", "second"}, []interface{}{textPart(0, 7, "first"), textPart(7, 10, "second")}},
		{"unordered overlapping suffix", []interface{}{int64(5), int64(0)}, []interface{}{int64(10), int64(7)}, []interface{}{"second", "first"}, []interface{}{textPart(0, 7, "first"), textPart(7, 10, "second")}},
		{"unordered gaps", []interface{}{int64(7), int64(2)}, []interface{}{int64(9), int64(4)}, []interface{}{"last", "first"}, []interface{}{textPart(0, 2, "own"), textPart(2, 4, "first"), textPart(4, 7, "own"), textPart(7, 9, "last"), textPart(9, 10, "own")}},
		{"equal starts", []interface{}{int64(2), int64(2)}, []interface{}{int64(5), int64(8)}, []interface{}{"first", "second"}, []interface{}{textPart(0, 2, "own"), textPart(2, 5, "first"), textPart(5, 8, "second"), textPart(8, 10, "own")}},
		{"clipped", []interface{}{int64(8), int64(-2)}, []interface{}{int64(15), int64(4)}, []interface{}{"last", "first"}, []interface{}{textPart(0, 4, "first"), textPart(4, 8, "own"), textPart(8, 10, "last")}},
		{"empty", []interface{}{}, []interface{}{}, []interface{}{}, []interface{}{textPart(0, 10, "own")}},
		{"absent", nil, nil, nil, []interface{}{textPart(0, 10, "own")}},
		{"empty interval", []interface{}{int64(3)}, []interface{}{int64(3)}, []interface{}{"empty"}, []interface{}{textPart(0, 10, "own")}},
		{"outside span", []interface{}{int64(-5), int64(12)}, []interface{}{int64(-1), int64(15)}, []interface{}{"before", "after"}, []interface{}{textPart(0, 10, "own")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := newTextIntervalExecutor(t)
			_, err := exec.Execute(context.Background(), "MATCH (s:Source) SET s.starts = $starts, s.ends = $ends, s.roles = $roles", map[string]interface{}{"starts": tc.starts, "ends": tc.ends, "roles": tc.roles})
			require.NoError(t, err)
			row := textIntervalRow(t, exec, documentedTextIntervalPartition(t), map[string]interface{}{"start": int64(0), "end": int64(10)})
			require.Equal(t, tc.want, row[0])
		})
	}
}

func TestTextIntervalPartitionEmptySpan(t *testing.T) {
	exec := newTextIntervalExecutor(t)
	row := textIntervalRow(t, exec, documentedTextIntervalPartition(t), map[string]interface{}{"start": int64(12), "end": int64(12)})
	require.Equal(t, []interface{}{}, row[0])
}

func TestTextIntervalPartitionMultipleSources(t *testing.T) {
	exec := newTextIntervalExecutor(t)
	_, err := exec.Execute(context.Background(), "CREATE (:Source {id: 'source-2', starts: [0, 5], ends: [10, 7], roles: ['first', 'second']})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(context.Background(), "MATCH (s:Source)"+documentedTextIntervalPartition(t), map[string]interface{}{"start": int64(0), "end": int64(10)})
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{[]interface{}{textPart(0, 10, "own")}},
		{[]interface{}{textPart(0, 10, "first")}},
	}, result.Rows)
}
