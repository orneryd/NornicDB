// Verification tests for hunt leads L-H9-001, L-H9-005, L-H9-007 and L-H9-008
// (v1.5.2 verification campaign, H9 verifier lane, 2026-11-21). Test-only,
// not for merge. Each lead test fails exactly at the disagreement the lead
// states; each control next to it is expected to pass.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/apocmeta"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyH9Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	return NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
}

func verifyH9Scalar(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}) interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	require.Len(t, result.Rows, 1, query)
	require.NotEmpty(t, result.Rows[0], query)
	t.Logf("VERIFY_H9 %q -> %#v", query, result.Rows[0][0])
	return result.Rows[0][0]
}

func verifyH9Exec(t *testing.T, exec *StorageExecutor, query string) {
	t.Helper()
	_, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
}

// L-H9-001: a node whose label starts with "_" (the migration guide's
// _Migrated) is written but cannot be read back on a user database.
func testVerifyH9UnderscoreFlow(t *testing.T, label string) {
	exec := verifyH9Executor(t)
	verifyH9Exec(t, exec, "CREATE (:"+label+" {_neo4j_id: 'x1', k: 1, _neo4j_labels: ['Person']})")
	assert.Equal(t, int64(1), verifyH9Scalar(t, exec, "MATCH (n:"+label+") RETURN count(n) AS c", nil), "count by label")
	assert.Equal(t, int64(1), verifyH9Scalar(t, exec, "MATCH (n) WHERE n.k = 1 RETURN count(n) AS c", nil), "count by property")
	// The guide's promotion statement, verbatim apart from the label.
	verifyH9Exec(t, exec, "MATCH (n:"+label+") WHERE 'Person' IN n._neo4j_labels SET n:Person REMOVE n:"+label+" REMOVE n._neo4j_labels")
	assert.Equal(t, int64(1), verifyH9Scalar(t, exec, "MATCH (n:Person) WHERE n.k = 1 RETURN count(n) AS c", nil), "promoted Person")
}

func TestVerifyH9_001_UnderscoreLabelNodeIsReadable(t *testing.T) {
	testVerifyH9UnderscoreFlow(t, "_Migrated")
}

func TestVerifyH9_001_Control_PlainLabel(t *testing.T) {
	testVerifyH9UnderscoreFlow(t, "Migrated")
}

// L-H9-005: the map constructor and the string constructor of the same local
// time in a DST fall-back overlap give the same instant (the earlier one,
// offset -04:00 for America/New_York on 2024-11-03 01:30).
func TestVerifyH9_005_MapConstructorOverlapMatchesStringForm(t *testing.T) {
	exec := verifyH9Executor(t)
	mapOffset := verifyH9Scalar(t, exec, "RETURN datetime({year: 2024, month: 11, day: 3, hour: 1, minute: 30, timezone: 'America/New_York'}).offset AS s", nil)
	stringOffset := verifyH9Scalar(t, exec, "RETURN datetime('2024-11-03T01:30[America/New_York]').offset AS s", nil)
	mapEpoch := verifyH9Scalar(t, exec, "RETURN datetime({year: 2024, month: 11, day: 3, hour: 1, minute: 30, timezone: 'America/New_York'}).epochSeconds AS s", nil)
	stringEpoch := verifyH9Scalar(t, exec, "RETURN datetime('2024-11-03T01:30[America/New_York]').epochSeconds AS s", nil)
	assert.Equal(t, "-04:00", mapOffset, "map constructor offset in the overlap")
	assert.Equal(t, stringEpoch, mapEpoch, "map and string constructors, same local time")
	assert.Equal(t, "-04:00", stringOffset, "string constructor offset in the overlap")
}

// Control: outside any transition the two forms agree; in the spring gap both
// shift forward to 03:30-04:00 as documented.
func TestVerifyH9_005_Control_NoTransitionAndGap(t *testing.T) {
	exec := verifyH9Executor(t)
	assert.Equal(t,
		verifyH9Scalar(t, exec, "RETURN datetime('2024-11-04T01:30[America/New_York]').epochSeconds AS s", nil),
		verifyH9Scalar(t, exec, "RETURN datetime({year: 2024, month: 11, day: 4, hour: 1, minute: 30, timezone: 'America/New_York'}).epochSeconds AS s", nil))
	assert.Equal(t, "2024-03-10T03:30:00-04:00[America/New_York]",
		verifyH9Scalar(t, exec, "RETURN toString(datetime({year: 2024, month: 3, day: 10, hour: 2, minute: 30, timezone: 'America/New_York'})) AS s", nil))
}

// L-H9-007 premise check: the APOC page names two sources, the executor's
// built-in catalog and the optional apoc.so plugin. apoc.math.round is not in
// the built-in catalog, so a server without the plugin reports it unknown;
// apoc.create.node is listed as not implemented. A built-in name works.
func TestVerifyH9_007_PremiseCheck(t *testing.T) {
	exec := verifyH9Executor(t)
	for _, name := range []string{"apoc.math.round", "apoc.text.camelCase", "apoc.coll.duplicates", "apoc.bitwise.and"} {
		native := apocmeta.IsNative(name)
		t.Logf("VERIFY_H9 apocmeta.IsNative(%s) = %v", name, native)
		assert.False(t, native, name)
	}
	_, err := exec.Execute(context.Background(), "RETURN apoc.math.round(3.7) AS x", nil)
	t.Logf("VERIFY_H9 RETURN apoc.math.round(3.7) without plugin -> err=%v", err)
	assert.Error(t, err)
	_, err = exec.Execute(context.Background(), "CALL apoc.create.node(['PW304'], {v: 1})", nil)
	t.Logf("VERIFY_H9 CALL apoc.create.node without plugin -> err=%v", err)
	assert.Error(t, err)
	assert.True(t, apocmeta.IsNative("apoc.coll.toSet"))
	got := verifyH9Scalar(t, exec, "RETURN size(apoc.coll.toSet([1, 1, 2])) AS x", nil)
	assert.Equal(t, int64(2), got)
}

// L-H9-008: one failing inner batch is reported in failedBatches and
// errorMessages; the call itself succeeds and the other batch commits.
func TestVerifyH9_008_PeriodicIterateReportsArithmeticBatchFailure(t *testing.T) {
	exec := verifyH9Executor(t)
	query := "CALL apoc.periodic.iterate('UNWIND [0, 1] AS i RETURN i', 'CREATE (:FuPI {v: 1 / i})', {batchSize: 1}) YIELD batches, failedBatches, errorMessages RETURN batches, failedBatches, errorMessages"
	result, err := exec.Execute(context.Background(), query, nil)
	t.Logf("VERIFY_H9 %q -> err=%v", query, err)
	require.NoError(t, err, "the call fails as a whole")
	require.Len(t, result.Rows, 1)
	t.Logf("VERIFY_H9 row %#v", result.Rows[0])
	assert.Equal(t, int64(2), result.Rows[0][0], "batches")
	assert.Equal(t, int64(1), result.Rows[0][1], "failedBatches")
	messages, _ := result.Rows[0][2].(map[string]interface{})
	assert.Len(t, messages, 1, "errorMessages")
	assert.Equal(t, int64(1), verifyH9Scalar(t, exec, "MATCH (n:FuPI) RETURN count(n) AS c", nil), "committed batch")
}

// Control: a batch that fails on a uniqueness constraint (not an expression
// error) is reported in failedBatches/errorMessages and the call succeeds.
func TestVerifyH9_008_Control_ConstraintBatchFailureIsReported(t *testing.T) {
	exec := verifyH9Executor(t)
	verifyH9Exec(t, exec, "CREATE CONSTRAINT fu_u FOR (n:FuU) REQUIRE n.k IS UNIQUE")
	verifyH9Exec(t, exec, "CREATE (:FuU {k: 1})")
	query := "CALL apoc.periodic.iterate('UNWIND [1, 2] AS i RETURN i', 'CREATE (:FuU {k: i})', {batchSize: 1}) YIELD batches, failedBatches, errorMessages RETURN batches, failedBatches, errorMessages"
	result, err := exec.Execute(context.Background(), query, nil)
	t.Logf("VERIFY_H9 %q -> err=%v", query, err)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	t.Logf("VERIFY_H9 row %#v", result.Rows[0])
	assert.Equal(t, int64(2), result.Rows[0][0], "batches")
	assert.Equal(t, int64(1), result.Rows[0][1], "failedBatches")
	messages, _ := result.Rows[0][2].(map[string]interface{})
	assert.Len(t, messages, 1, "errorMessages")
	assert.Equal(t, int64(2), verifyH9Scalar(t, exec, "MATCH (n:FuU) RETURN count(n) AS c", nil))
}
