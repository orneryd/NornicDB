package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPendingTemporalIndexIncludesEveryOverlap(t *testing.T) {
	day := func(year, day int) time.Time { return time.Date(year, time.January, day, 0, 0, 0, 0, time.UTC) }
	bounds := [][2]any{
		{nil, day(0, 2)},
		{day(0, 1), day(0, 3)},
		{day(0, 3), day(0, 4)},
		{day(2026, 1), day(2026, 3)},
		{day(2026, 2), day(2026, 6)},
		{day(2026, 3), day(2026, 4)},
		{day(2026, 6), nil},
	}
	tx := &BadgerTransaction{pendingEdges: make(map[EdgeID]*Edge)}
	for i, pair := range bounds {
		id := EdgeID(fmt.Sprintf("db:%d", i))
		tx.pendingEdges[id] = &Edge{
			ID: id, Type: "LINK", Properties: map[string]any{"from": pair[0], "to": pair[1]},
		}
	}
	var read temporalValidationRead
	index, err := read.pendingTemporalEdges(t.Context(), tx, pendingTemporalScope{
		namespace: "db", edgeType: "LINK", start: "from", end: "to",
	})
	require.NoError(t, err)

	for _, pair := range bounds {
		start, end, hasEnd, valid := ReadTemporalBounds(pair[0], pair[1])
		require.True(t, valid)
		query := temporalInterval{start: start, end: end, hasEnd: hasEnd}
		var got, want []EdgeID
		for _, entry := range index.overlapping(query) {
			if intervalsOverlap(query, entry.interval) {
				got = append(got, entry.edge.ID)
			}
		}
		for id, edge := range tx.pendingEdges {
			start, end, hasEnd, valid := ReadTemporalBounds(edge.Properties["from"], edge.Properties["to"])
			require.True(t, valid)
			if intervalsOverlap(query, temporalInterval{start: start, end: end, hasEnd: hasEnd}) {
				want = append(want, id)
			}
		}
		require.ElementsMatch(t, want, got, "bounds %v", pair)
	}
}
