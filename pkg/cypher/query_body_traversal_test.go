package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Structural rules must see expression bodies even when the surrounding
// statement cannot resolve its names. Quoted text, comments and maps are
// not query bodies, and every reported span addresses the original source.
func TestReadQueryBodyExpressionWindows(t *testing.T) {
	source := "/* CALL { RETURN 0 } */ WITH 'COUNT { RETURN 0 }' AS text, `EXISTS { decoy }` AS quoted " +
		"CALL ( missing ) { RETURN COLLECT { WITH missing RETURN missing } AS values } " +
		"RETURN EXISTS { MATCH (n) RETURN n } AS present, COUNT { MATCH (m) RETURN m } AS total, {key: 1} AS map // COLLECT { RETURN 0 }"
	type body struct {
		container string
		text      string
	}
	want := []body{
		{"CALL", " RETURN COLLECT { WITH missing RETURN missing } AS values "},
		{"COLLECT", " WITH missing RETURN missing "},
		{"EXISTS", " MATCH (n) RETURN n "},
		{"COUNT", " MATCH (m) RETURN m "},
	}
	var got []body
	require.NoError(t, forEachQueryBody(source, queryBodyContainerKeywords[:], func(container string, start, end int) error {
		require.Greater(t, start, 0)
		require.Less(t, end, len(source))
		require.Equal(t, byte('{'), source[start-1])
		require.Equal(t, byte('}'), source[end])
		got = append(got, body{container, source[start:end]})
		return nil
	}))
	require.Equal(t, want, got)

	var selected []body
	require.NoError(t, forEachQueryBody(source, []string{"COLLECT"}, func(container string, start, end int) error {
		selected = append(selected, body{container, source[start:end]})
		return nil
	}))
	require.Equal(t, []body{want[1]}, selected)

	stop := errors.New("stop structural validation")
	visits := 0
	err := forEachQueryBody(source, queryBodyContainerKeywords[:], func(string, int, int) error {
		visits++
		return stop
	})
	require.ErrorIs(t, err, stop)
	require.Equal(t, 1, visits, "a rule error must stop the traversal")
	require.NoError(t, forEachQueryBody("RETURN 1", queryBodyContainerKeywords[:], func(string, int, int) error {
		t.Fatal("a statement without braces has no query bodies")
		return nil
	}))
}
