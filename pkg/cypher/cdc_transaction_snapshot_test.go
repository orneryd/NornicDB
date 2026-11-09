package cypher

import (
	snapshotFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// cdcResolveQuery lists the changes after $from and looks each changed node
// up in the same statement, as a change-feed consumer resolves its events.
const cdcResolveQuery = "CALL db.cdc.query($from) YIELD id, event " +
	"WITH id, event OPTIONAL MATCH (n) WHERE elementId(n) = event.elementId " +
	"RETURN id, event.elementId AS elementId, n IS NOT NULL AS found"

type cdcResolvedChange struct {
	id, elementID string
	found         bool
}

func cdcResolve(t *testing.T, exec *StorageExecutor, from string) []cdcResolvedChange {
	t.Helper()
	result, err := exec.Execute(context.Background(), cdcResolveQuery, map[string]interface{}{"from": from})
	require.NoError(t, err)
	changes := make([]cdcResolvedChange, len(result.Rows))
	for index, row := range result.Rows {
		changes[index] = cdcResolvedChange{id: row[0].(string), elementID: row[1].(string), found: row[2].(bool)}
	}
	return changes
}

func cdcElementIDs(t *testing.T, exec *StorageExecutor, label string) []string {
	t.Helper()
	result, err := exec.Execute(context.Background(), "MATCH (n:"+label+") RETURN elementId(n)", nil)
	require.NoError(t, err)
	ids := make([]string, len(result.Rows))
	for index, row := range result.Rows {
		ids[index] = row[0].(string)
	}
	return ids
}

func requireAllFound(t *testing.T, changes []cdcResolvedChange) {
	t.Helper()
	missing := 0
	for _, change := range changes {
		if !change.found {
			missing++
		}
	}
	require.Zero(t, missing, "%d of %d listed changes are not visible to the same statement", missing, len(changes))
}

func newCDCSnapshotStore(t *testing.T, writeBehind bool) storage.Engine {
	t.Helper()
	if !writeBehind {
		return newTestServerLikeNamespacedStore(t)
	}
	engine, err := snapshotFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	return storage.NewNamespacedEngine(engine, "test")
}

// Inside an explicit transaction db.cdc.query lists only the changes its
// snapshot holds, so every listed change is visible to the same statement's
// lookups; commits after BEGIN are not listed. db.cdc.current there is the
// snapshot's position: a cursor taken there returns exactly the later
// commits afterwards, nothing lost or repeated. An identifier past the
// snapshot is FutureIdentifier. A transaction begun later sees every change
// (positive control), and the transaction's own uncommitted writes are not
// change events. The changes before BEGIN span more than one internal page.
func TestChangeDataCaptureQueryIsBoundedByTheTransactionSnapshot(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		name := "write-behind off"
		if writeBehind {
			name = "write-behind on"
		}
		t.Run(name, func(t *testing.T) {
			store := newCDCSnapshotStore(t, writeBehind)
			capture, ok := storage.ChangeCaptureOf(store)
			require.True(t, ok)
			capture.SetChangeCapture("test", storage.ChangeCaptureFull)
			ctx := context.Background()
			writer, reader := NewStorageExecutor(store), NewStorageExecutor(store)

			const before, after = 1200, 30
			start := cdcID(t, writer, "db.cdc.current")
			_, err := writer.Execute(ctx, "UNWIND range(1, $n) AS i CREATE (:Before {i: i})", map[string]interface{}{"n": before})
			require.NoError(t, err)

			_, err = reader.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			for i := 0; i < after; i++ {
				_, err = writer.Execute(ctx, "CREATE (:After {i: $i})", map[string]interface{}{"i": i})
				require.NoError(t, err)
			}
			latest := cdcID(t, writer, "db.cdc.current")

			listed := cdcResolve(t, reader, start)
			requireAllFound(t, listed)
			require.Len(t, listed, before, "commits after BEGIN must not be listed")
			cursor := cdcID(t, reader, "db.cdc.current")
			require.Empty(t, cdcResolve(t, reader, cursor))
			_, err = reader.Execute(ctx, "CALL db.cdc.query($from)", map[string]interface{}{"from": latest})
			requireStatusCode(t, err, "Neo.TransientError.ChangeDataCapture.FutureIdentifier")
			_, err = reader.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)

			// The cursor taken inside the transaction resumes exactly at the
			// later commits.
			later := cdcResolve(t, reader, cursor)
			requireAllFound(t, later)
			laterIDs := make([]string, len(later))
			for index, change := range later {
				laterIDs[index] = change.elementID
			}
			require.ElementsMatch(t, cdcElementIDs(t, reader, "After"), laterIDs)
			seen := make(map[string]bool, len(listed)+len(later))
			for _, change := range append(append([]cdcResolvedChange(nil), listed...), later...) {
				require.False(t, seen[change.id], "change %s listed twice", change.id)
				seen[change.id] = true
			}

			// A transaction begun after the commits lists and sees them all.
			_, err = reader.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			all := cdcResolve(t, reader, start)
			requireAllFound(t, all)
			require.Len(t, all, before+after)

			// Its own uncommitted writes are not change events.
			_, err = reader.Execute(ctx, "CREATE (:Own)", nil)
			require.NoError(t, err)
			require.Len(t, cdcResolve(t, reader, start), before+after)
			_, err = reader.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)

			// Auto-commit reads every committed change.
			require.Len(t, cdcResolve(t, writer, start), before+after)
		})
	}
}
