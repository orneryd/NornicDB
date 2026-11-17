package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestMaterializedScalarRowsAllocateNothing(t *testing.T) {
	rows := [][]any{{int64(7), "value", true}}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if materializedRows(nil, rows) {
			panic("scalar recognized as entity")
		}
	}))
}

func TestMaterializedRowsTrackDeepCyclesAndRepeatedAliases(t *testing.T) {
	for _, depth := range []int{0, 6, 7, 8, 9, 20} {
		t.Run(strconv.Itoa(depth), func(t *testing.T) {
			node := &storage.Node{ID: "retained"}
			cycle := map[string]any{}
			cycle["self"] = cycle
			cycle["node"] = node
			var nested any = cycle
			for i := 0; i < depth; i++ {
				nested = []any{nested}
			}
			recorder := &recordingAccessRecorder{}
			require.True(t, materializedRows(recorder, [][]any{{nested, nested}}))
			require.Equal(t, []string{"retained", "retained"}, recorder.ids)
			require.True(t, materializedRows(nil, [][]any{{nested}}))
		})
	}
	require.False(t, materializedRows(nil, [][]any{{map[string]any{"list": []any{int64(7)}}}}))
}

func TestMaterializedRowsReuseCycleStateAcrossRows(t *testing.T) {
	node := &storage.Node{ID: "retained"}
	cycle := map[string]any{"node": node}
	cycle["self"] = cycle
	var deep any = cycle
	for i := 0; i < 20; i++ {
		deep = []any{deep}
	}
	recorder := &recordingAccessRecorder{}
	require.True(t, materializedRows(recorder, [][]any{{deep}, {cycle}, {deep}, {node}}))
	require.Equal(t, []string{"retained", "retained", "retained", "retained"}, recorder.ids)
}
