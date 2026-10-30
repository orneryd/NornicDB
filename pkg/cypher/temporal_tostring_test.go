package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// toString() and toStringOrNull() write a time's seconds even when they are
// zero; the value's own text, which HTTP results use, leaves them out. Both
// as Neo4j 5.26.30 does (#818).
func TestTemporalToStringWritesSeconds(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	for query, want := range map[string]string{
		"RETURN toString(datetime('2020-01-02T03:04:00Z')) AS s":                                                               "2020-01-02T03:04:00Z",
		"RETURN toString(localtime('12:34')) AS s":                                                                             "12:34:00",
		"RETURN toString(time('12:34+01:00')) AS s":                                                                            "12:34:00+01:00",
		"RETURN toString(localdatetime('2020-01-02T03:04')) AS s":                                                              "2020-01-02T03:04:00",
		"WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS d RETURN toString(d) AS s": "1984-10-11T12:00:00+01:00[Europe/Stockholm]",
		"RETURN toString(datetime('2020-01-02T03:04:05Z')) AS s":                                                               "2020-01-02T03:04:05Z",
		"RETURN toString(datetime('2020-01-02T03:04:00.5Z')) AS s":                                                             "2020-01-02T03:04:00.5Z",
		"RETURN toStringOrNull(localtime('12:34')) AS s":                                                                       "12:34:00",
		"RETURN toStringOrNull(datetime('2020-01-02T03:04Z')) AS s":                                                            "2020-01-02T03:04:00Z",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}

	result, err := exec.Execute(ctx, "RETURN datetime('2020-01-02T03:04Z') AS d, localtime('12:34') AS t", nil)
	require.NoError(t, err)
	require.Equal(t, "2020-01-02T03:04Z", fmt.Sprint(result.Rows[0][0]), "the value's own text leaves zero seconds out")
	require.Equal(t, "12:34", fmt.Sprint(result.Rows[0][1]))
}

func TestFormatCypherValueStringTemporalPointers(t *testing.T) {
	clock := time.Date(2020, 1, 2, 3, 4, 0, 0, time.UTC)
	require.Equal(t, "03:04:00", formatCypherValueString(&CypherLocalTime{Time: clock}))
	require.Equal(t, "03:04:00Z", formatCypherValueString(&CypherTime{Time: clock}))
	require.Equal(t, "2020-01-02T03:04:00", formatCypherValueString(&CypherLocalDateTime{Time: clock}))
	require.Equal(t, "2020-01-02T03:04:00Z", formatCypherValueString(&CypherDateTime{Time: clock}))
}
