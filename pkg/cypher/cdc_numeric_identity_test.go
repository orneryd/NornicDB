package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCDCNumericIdentity(t *testing.T) {
	t.Run("equal selectors preserve adjacent integer distinction", func(t *testing.T) {
		exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
		cursor := cdcID(t, exec, "db.cdc.current")
		for _, value := range []int64{9007199254740992, 9007199254740993} {
			_, err := exec.Execute(context.Background(), "CREATE (:NumericCDC {v:$value})", map[string]interface{}{"value": value})
			require.NoError(t, err)
		}
		rows := cdcRows(t, exec, cursor, map[string]interface{}{"select": "n", "key": map[string]interface{}{"v": float64(9007199254740992)}})
		require.Len(t, rows, 1)
		require.Equal(t, int64(9007199254740992), stateOf(rows[0], "after")["properties"].(map[string]interface{})["v"])
		rows = cdcRows(t, exec, cursor, map[string]interface{}{"select": "n", "key": map[string]interface{}{"v": int64(9007199254740993)}})
		require.Len(t, rows, 1)
		require.Equal(t, int64(9007199254740993), stateOf(rows[0], "after")["properties"].(map[string]interface{})["v"])
	})
	t.Run("native unsigned event is exact", func(t *testing.T) {
		exec, store := newCDCExecutor(t, storage.ChangeCaptureFull)
		cursor := cdcID(t, exec, "db.cdc.current")
		_, err := store.CreateNode(context.Background(), &storage.Node{ID: "wide", Labels: []string{"NumericCDC"}, Properties: map[string]interface{}{"v": uint64(math.MaxUint64)}})
		require.NoError(t, err)
		rows := cdcRows(t, exec, cursor)
		require.Len(t, rows, 1)
		require.Equal(t, uint64(math.MaxUint64), stateOf(rows[0], "after")["properties"].(map[string]interface{})["v"])
	})
	t.Run("numeric kind change remains observable", func(t *testing.T) {
		exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
		_, err := exec.Execute(context.Background(), "CREATE (:NumericCDC {v:2})", nil)
		require.NoError(t, err)
		cursor := cdcID(t, exec, "db.cdc.current")
		_, err = exec.Execute(context.Background(), "MATCH (n:NumericCDC) SET n.v=$value", map[string]interface{}{"value": float64(2)})
		require.NoError(t, err)
		rows := cdcRows(t, exec, cursor, map[string]interface{}{"select": "n", "changesTo": []interface{}{"v"}})
		require.Len(t, rows, 1)
		require.Equal(t, int64(2), stateOf(rows[0], "before")["properties"].(map[string]interface{})["v"])
		require.Equal(t, float64(2), stateOf(rows[0], "after")["properties"].(map[string]interface{})["v"])
	})
}
