// Public Bolt transport coverage for the approved historical read API:
//
//	CALL db.temporal.asOf(at, query, parameters = {}) YIELD row RETURN row
//
// The fixture is a retention-enabled native Badger engine, so a previous
// property value is still resolvable at the caller's cutoff instead of having
// been overwritten in place.
package bolt

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// temporalHistoryBoltStore is the retention-enabled native Badger fixture:
// NewMemoryEngineWithMVCCHistory configures MaxVersionsPerKey=100, and the
// fixture owner closes the engine at test cleanup.
func temporalHistoryBoltStore(t *testing.T) storage.Engine {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngineWithMVCCHistory(), storage.ErrStorageClosed))
	return storage.NewNamespacedEngine(base, "temporal_bolt")
}

func temporalHistoryBoltDriver(t *testing.T, port int) neo4j.DriverWithContext {
	t.Helper()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(context.Background())) })
	return driver
}

// temporalHistoryBoltRun runs one auto-commit statement and returns its
// column names and first-column values. A server failure surfaces as err.
func temporalHistoryBoltRun(t *testing.T, driver neo4j.DriverWithContext, query string, params map[string]any) ([]string, []any, error) {
	t.Helper()
	ctx := t.Context()
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "temporal_bolt"})
	defer func() { require.NoError(t, session.Close(context.Background())) }()
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, nil, err
	}
	keys, err := result.Keys()
	if err != nil {
		return nil, nil, err
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return keys, nil, err
	}
	rows := make([]any, 0, len(records))
	for _, record := range records {
		require.Len(t, record.Values, 1)
		rows = append(rows, record.Values[0])
	}
	return keys, rows, nil
}

func TestTemporalAsOfBoltReturnsRetainedPropertyAndWholeRow(t *testing.T) {
	_, port := startBoltIntegrationServer(t, temporalHistoryBoltStore(t), boltIntegrationAutocommit)
	driver := temporalHistoryBoltDriver(t, port)

	_, _, err := temporalHistoryBoltRun(t, driver, "CREATE (:Account {name:'before'})", nil)
	require.NoError(t, err)
	at := time.Now().UTC()
	_, _, err = temporalHistoryBoltRun(t, driver, "MATCH (n:Account) SET n.name='after'", nil)
	require.NoError(t, err)

	keys, rows, err := temporalHistoryBoltRun(t, driver,
		`CALL db.temporal.asOf($at, 'MATCH (n:Account) RETURN n.name AS name') YIELD row RETURN row.name AS name`,
		map[string]any{"at": at})
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, keys)
	require.Equal(t, []any{"before"}, rows, "the retained property value at the cutoff")

	keys, rows, err = temporalHistoryBoltRun(t, driver,
		`CALL db.temporal.asOf($at, 'RETURN 1 AS one, true AS two') YIELD row RETURN row`,
		map[string]any{"at": at})
	require.NoError(t, err)
	require.Equal(t, []string{"row"}, keys)
	require.Len(t, rows, 1)
	require.Equal(t, map[string]any{"one": int64(1), "two": true}, rows[0], "the whole row is one public map value")
}

func TestTemporalAsOfBoltRejectsMutatingNestedStatement(t *testing.T) {
	_, port := startBoltIntegrationServer(t, temporalHistoryBoltStore(t), boltIntegrationAutocommit)
	driver := temporalHistoryBoltDriver(t, port)

	_, _, err := temporalHistoryBoltRun(t, driver,
		`CALL db.temporal.asOf($at, 'CREATE (:Forbidden)') YIELD row RETURN row`,
		map[string]any{"at": time.Now().UTC()})
	require.Error(t, err)
	require.Contains(t, err.Error(), "read-only")
	var refusal *neo4j.Neo4jError
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, "Neo.ClientError.Statement.AccessMode", refusal.Code)

	_, rows, err := temporalHistoryBoltRun(t, driver, "MATCH (n:Forbidden) RETURN count(n) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, []any{int64(0)}, rows, "the refused nested write left no entity")
}

func TestTemporalAsOfBoltKeepsDeprecatedIntervalSignature(t *testing.T) {
	_, port := startBoltIntegrationServer(t, temporalHistoryBoltStore(t), boltIntegrationAutocommit)
	driver := temporalHistoryBoltDriver(t, port)

	_, _, err := temporalHistoryBoltRun(t, driver,
		`CREATE (:Interval {key:'a', valid_from:'2024-01-01T00:00:00Z', valid_to:'2024-02-01T00:00:00Z', name:'interval'})`, nil)
	require.NoError(t, err)

	keys, rows, err := temporalHistoryBoltRun(t, driver,
		`CALL db.temporal.asOf('Interval', 'key', 'a', 'valid_from', 'valid_to', '2024-01-15T00:00:00Z') YIELD node RETURN node.name AS name`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, keys)
	require.Equal(t, []any{"interval"}, rows)
}
