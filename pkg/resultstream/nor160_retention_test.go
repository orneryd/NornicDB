// NOR-160 lane (Claude), 30 October 2026: replay pages are caller-owned.
package resultstream

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func nor160ReplayRows() [][]any {
	rows := make([][]any, 2, 4)
	rows[0] = []any{map[string]any{"payload": map[string]any{"value": "original"}}, []any{"a"}}
	rows[1] = []any{map[string]any{"payload": map[string]any{"value": "second"}}, []any{"b"}}
	return rows
}

// A replayable stream keeps its rows: no page may change a later page, at
// the same or another position, through cells, rows or spare capacity.
func TestNOR160RetentionReplayPages(t *testing.T) {
	stream, err := NewProgressive(nor160ReplayRows(), true, 2, nil)
	require.NoError(t, err)
	ctx := context.Background()
	want := readownership.Capture(nor160ReplayRows())
	for pass := 0; pass < 3; pass++ {
		first, err := stream.Pull(ctx, 0, 1)
		require.NoError(t, err)
		require.Len(t, first.Rows, 1)
		readownership.Mutate(first.Rows)
		first.Rows[0] = []any{"replaced"}
		_ = append(first.Rows, []any{"appended"})

		all, err := stream.Pull(ctx, 0, 2)
		require.NoError(t, err)
		require.Equal(t, want, readownership.Capture(all.Rows), "pass %d", pass)
	}
}
