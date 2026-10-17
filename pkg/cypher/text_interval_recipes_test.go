package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Text intervals and line positions need no function of their own: these
// statements, as the functions reference gives them, compute them over a
// text and parallel start/end/metadata lists, in code points, and Neo4j
// 5.26 returns the same rows (each expected value below is its output).

const (
	// textIntervalOverlaps clips each interval overlapping the half-open span
	// [start, stop) to it, in the lists' order, with the interval's index and
	// metadata; an empty span [start, start) stands for [start, start + 1).
	textIntervalOverlaps = `
WITH s, $start AS start, CASE WHEN $end > $start THEN $end ELSE $start + 1 END AS stop
RETURN [i IN range(0, size(coalesce(s.starts, [])) - 1)
        WHERE s.starts[i] < stop AND start < s.ends[i]
        | {index: i,
           start: CASE WHEN start > s.starts[i] THEN start ELSE s.starts[i] END,
           end: CASE WHEN stop < s.ends[i] THEN stop ELSE s.ends[i] END,
           label: s.labels[i]}] AS overlaps`

	// textIntervalPartition splits [start, end) into the intervals' clipped
	// parts with their role, and the gaps between them as 'own'.
	textIntervalPartition = `
WITH s, $start AS start, $end AS end
WITH start, end, reduce(acc = {position: start, parts: []}, i IN range(0, size(s.starts) - 1) |
  CASE WHEN (CASE WHEN s.starts[i] > start THEN s.starts[i] ELSE start END)
            >= (CASE WHEN s.ends[i] < end THEN s.ends[i] ELSE end END) THEN acc
  ELSE {position: CASE WHEN s.ends[i] < end THEN s.ends[i] ELSE end END,
        parts: acc.parts
          + CASE WHEN acc.position < (CASE WHEN s.starts[i] > start THEN s.starts[i] ELSE start END)
                 THEN [{start: acc.position, end: CASE WHEN s.starts[i] > start THEN s.starts[i] ELSE start END, role: 'own'}]
                 ELSE [] END
          + [{start: CASE WHEN s.starts[i] > start THEN s.starts[i] ELSE start END,
              end: CASE WHEN s.ends[i] < end THEN s.ends[i] ELSE end END, role: s.roles[i]}]}
  END) AS acc
RETURN acc.parts + CASE WHEN acc.position < end THEN [{start: acc.position, end: end, role: 'own'}] ELSE [] END AS parts`

	// textLinePosition is the 1-based line, the 0-based column and the line's
	// text at a code-point offset.
	textLinePosition = `
WITH s.text AS text, $offset AS offset
WITH text, offset, substring(text, 0, offset) AS before
WITH text, offset, before, size(before) - size(replace(before, '\n', '')) AS breaks
RETURN breaks + 1 AS line, size(last(split(before, '\n'))) AS column, split(text, '\n')[breaks] AS context`

	// textNumberedLines is the span's lines as "n: line", numbered from first.
	textNumberedLines = `
WITH split(substring(s.text, $start, $end - $start), '\n') AS lines, $first AS first
RETURN reduce(out = '', i IN range(0, size(lines) - 1) |
  out + CASE WHEN i = 0 THEN '' ELSE '\n' END + toString(first + i) + ': ' + lines[i]) AS numbered`
)

// textIntervalFixture is a text with an astral character, an empty line and
// a non-ASCII last line, and three sections with a gap at offset 25.
const textIntervalFixture = `CREATE (:Source {id: 'source-1', text: 'Hi 😀 there\nquoted > line\n\nlast é',
  starts: [0, 11, 26], ends: [11, 25, 32], roles: ['own', 'quoted', 'own'], labels: ['intro', 'quote', 'tail']})`

func newTextIntervalExecutor(t testing.TB) *StorageExecutor {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "textintervals"))
	_, err := exec.Execute(context.Background(), textIntervalFixture, nil)
	require.NoError(t, err)
	return exec
}

func textIntervalRow(t *testing.T, exec *StorageExecutor, recipe string, params map[string]interface{}) []interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), "MATCH (s:Source {id: 'source-1'})"+recipe, params)
	require.NoError(t, err, params)
	require.Len(t, result.Rows, 1, params)
	return result.Rows[0]
}

func textSpan(index int64, start, end int64, label string) map[string]interface{} {
	return map[string]interface{}{"index": index, "start": start, "end": end, "label": label}
}

func textPart(start, end int64, role string) map[string]interface{} {
	return map[string]interface{}{"start": start, "end": end, "role": role}
}

func TestTextIntervalRecipesMatchNeo4j(t *testing.T) {
	exec := newTextIntervalExecutor(t)

	for _, c := range []struct {
		start, end int64
		want       []interface{}
	}{
		{4, 13, []interface{}{textSpan(0, 4, 11, "intro"), textSpan(1, 11, 13, "quote")}},
		{25, 25, []interface{}{}}, // the empty span [25, 26) is the gap
		{26, 26, []interface{}{textSpan(2, 26, 27, "tail")}},
		{0, 32, []interface{}{textSpan(0, 0, 11, "intro"), textSpan(1, 11, 25, "quote"), textSpan(2, 26, 32, "tail")}},
		{30, 40, []interface{}{textSpan(2, 30, 32, "tail")}},
	} {
		row := textIntervalRow(t, exec, textIntervalOverlaps, map[string]interface{}{"start": c.start, "end": c.end})
		require.Equal(t, c.want, row[0], "[%d, %d)", c.start, c.end)
	}

	row := textIntervalRow(t, exec, textIntervalPartition, map[string]interface{}{"start": int64(5), "end": int64(30)})
	require.Equal(t, []interface{}{textPart(5, 11, "own"), textPart(11, 25, "quoted"), textPart(25, 26, "own"), textPart(26, 30, "own")}, row[0])
	row = textIntervalRow(t, exec, textIntervalPartition, map[string]interface{}{"start": int64(12), "end": int64(14)})
	require.Equal(t, []interface{}{textPart(12, 14, "quoted")}, row[0])

	for offset, want := range map[int64][]interface{}{
		0:  {int64(1), int64(0), "Hi 😀 there"},
		3:  {int64(1), int64(3), "Hi 😀 there"},
		20: {int64(2), int64(9), "quoted > line"},
		25: {int64(3), int64(0), ""},
		31: {int64(4), int64(5), "last é"},
		32: {int64(4), int64(6), "last é"},
	} {
		require.Equal(t, want, textIntervalRow(t, exec, textLinePosition, map[string]interface{}{"offset": offset}), "offset %d", offset)
	}

	for _, c := range []struct {
		start, end, first int64
		want              string
	}{
		{11, 32, 2, "2: quoted > line\n3: \n4: last é"},
		{10, 11, 1, "1: \n2: "},
		{3, 3, 1, "1: "},
		{3, 4, 1, "1: 😀"},
	} {
		row := textIntervalRow(t, exec, textNumberedLines, map[string]interface{}{"start": c.start, "end": c.end, "first": c.first})
		require.Equal(t, c.want, row[0], "[%d, %d)", c.start, c.end)
	}
}

// The overlap statement over parameter lists and many spans at once, as a
// caller places a page of hits: one row per span, in UNWIND order.
func TestTextIntervalOverlapsOverParameterLists(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "textintervals"))
	result, err := exec.Execute(context.Background(), `
WITH $starts AS starts, $ends AS ends
UNWIND $spans AS q
WITH starts, ends, q.start AS start, CASE WHEN q.end > q.start THEN q.end ELSE q.start + 1 END AS stop
RETURN start, [i IN range(0, size(starts) - 1) WHERE starts[i] < stop AND start < ends[i] | i] AS hits`,
		map[string]interface{}{
			"starts": []interface{}{int64(0), int64(5), int64(5), int64(20)},
			"ends":   []interface{}{int64(10), int64(6), int64(30), int64(21)},
			"spans": []interface{}{
				map[string]interface{}{"start": int64(5), "end": int64(5)},
				map[string]interface{}{"start": int64(10), "end": int64(20)},
				map[string]interface{}{"start": int64(30), "end": int64(31)},
			},
		})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{int64(5), []interface{}{int64(0), int64(1), int64(2)}},
		{int64(10), []interface{}{int64(2)}},
		{int64(30), []interface{}{}},
	}, result.Rows)
}
