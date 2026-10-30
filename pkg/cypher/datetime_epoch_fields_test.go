package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// datetime({epochSeconds | epochMillis, …}) as Neo4j 5.26.30 and 2026.09
// answer it (recorded): the epoch is the base instant, in the map's
// timezone, that the other fields override.
func TestDatetimeEpochFields(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "datetime_epoch_fields"))
	ctx := context.Background()
	for expression, want := range map[string]string{
		"datetime({epochSeconds: 1700000000})":                           "2023-11-14T22:13:20Z",
		"datetime({epochMillis: 1700000000123})":                         "2023-11-14T22:13:20.123Z",
		"datetime({epochSeconds: 1700000000, nanosecond: 5})":            "2023-11-14T22:13:20.000000005Z",
		"datetime({epochSeconds: 1700000000, millisecond: 5})":           "2023-11-14T22:13:20.005Z",
		"datetime({epochSeconds: 1700000000, microsecond: 5})":           "2023-11-14T22:13:20.000005Z",
		"datetime({epochMillis: 1700000000123, nanosecond: 5})":          "2023-11-14T22:13:20.000000005Z",
		"datetime({epochSeconds: 1700000000, timezone: '+02:00'})":       "2023-11-15T00:13:20+02:00",
		"datetime({epochSeconds: 1700000000, timezone: 'Europe/Paris'})": "2023-11-14T23:13:20+01:00[Europe/Paris]",
		"datetime({epochSeconds: -1})":                                   "1969-12-31T23:59:59Z",
		"datetime({epochMillis: -1})":                                    "1969-12-31T23:59:59.999Z",
		"datetime({epochSeconds: 1700000000, year: 2020})":               "2020-11-14T22:13:20Z",
		"datetime({epochSeconds: 1700000000, second: 1})":                "2023-11-14T22:13:01Z",
		"datetime({epochSeconds: 1700000000, week: 2})":                  "2023-01-10T22:13:20Z",
	} {
		result, err := exec.Execute(ctx, "RETURN toString("+expression+") AS v", nil)
		require.NoError(t, err, expression)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, expression)
	}
	for expression, code := range map[string]string{
		"datetime({epochSeconds: 1.5})":                                        "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochSeconds: '1'})":                                        "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochSeconds: null})":                                       "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochMillis: 1.5})":                                         "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochSeconds: 1, epochMillis: 2})":                          "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochSeconds: 1, datetime: datetime('2020-01-01T00:00Z')})": "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochSeconds: 1, date: date('2020-01-01')})":                "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochSeconds: 1, time: time('12:00Z')})":                    "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochSeconds: 1700000000, nanosecond: 1000000000})":         "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochSeconds: 1700000000, month: 2, day: 30})":              "Neo.ClientError.Statement.ArgumentError",
		"datetime({epochSeconds: 1700000000, hour: 25})":                       "Neo.ClientError.Statement.ArgumentError",
		"localdatetime({epochSeconds: 1700000000})":                            "Neo.ClientError.Statement.TypeError",
		"date({epochSeconds: 1700000000})":                                     "Neo.ClientError.Statement.TypeError",
		"time({epochSeconds: 1})":                                              "Neo.ClientError.Statement.TypeError",
		"localtime({epochMillis: 1})":                                          "Neo.ClientError.Statement.TypeError",
	} {
		_, err := exec.Execute(ctx, "RETURN toString("+expression+") AS v", nil)
		require.Error(t, err, expression)
		got, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, code, got, expression)
	}
}
