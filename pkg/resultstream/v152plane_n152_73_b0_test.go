package resultstream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These are observation tests for NOR-308, not approval of mutable delivery.
// Page currently documents rows as a read-only immutable view. The report asks
// for detached mutable deliveries; deciding that contract precedes a fix.
// All mutations below are sequential and confined to this fixture's values.
func TestNOR308ReportedReplayAliasing(t *testing.T) {
	for _, registered := range []bool{false, true} {
		name := "progressive"
		if registered {
			name = "registry"
		}
		for _, mutation := range []string{"row cell", "row header", "nested value", "outside page capacity"} {
			t.Run(name+"/"+mutation, func(t *testing.T) {
				rows := [][]any{{"zero"}, {"one", map[string]any{"items": []any{"kept"}, "bytes": []byte{7}}}, {"two"}, {"three"}}
				stream, err := NewProgressive(rows, true, len(rows), nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, stream.Close()) })
				pull := func(n int) (*Page, error) { return stream.Pull(t.Context(), 1, n) }
				if registered {
					registry, err := NewRegistry(Config{})
					require.NoError(t, err)
					t.Cleanup(registry.Close)
					scope := Scope{Owner: "nor308-owner", Database: "nor308-database"}
					first, err := registry.Start(t.Context(), scope, stream, 1)
					require.NoError(t, err)
					require.Equal(t, [][]any{{"zero"}}, first.Rows)
					require.NotEmpty(t, first.QID)
					qid := first.QID
					pull = func(n int) (*Page, error) { return registry.Pull(t.Context(), scope, qid, n) }
				}
				page, err := pull(1)
				require.NoError(t, err)
				require.Len(t, page.Rows, 1)
				require.Equal(t, uint64(1), page.Position)
				require.Equal(t, uint64(2), page.Next)
				require.True(t, page.HasMore)
				require.Equal(t, 3, cap(page.Rows), "the reported retained suffix is reachable by Go reslicing")
				switch mutation {
				case "row cell":
					page.Rows[0][0] = "changed"
				case "row header":
					page.Rows[0] = []any{"replacement"}
				case "nested value":
					value := page.Rows[0][1].(map[string]any)
					value["items"].([]any)[0] = "changed"
					value["bytes"].([]byte)[0] = 9
				case "outside page capacity":
					page.Rows[:2][1][0] = "changed later"
				}
				replay, err := pull(2)
				require.NoError(t, err)
				require.Equal(t, uint64(1), replay.Position)
				require.Equal(t, uint64(3), replay.Next)
				require.Len(t, replay.Rows, 2)
				require.True(t, replay.HasMore)
				switch mutation {
				case "row cell":
					require.Equal(t, "changed", replay.Rows[0][0])
				case "row header":
					require.Equal(t, []any{"replacement"}, replay.Rows[0])
				case "nested value":
					value := replay.Rows[0][1].(map[string]any)
					require.Equal(t, []any{"changed"}, value["items"])
					require.Equal(t, []byte{9}, value["bytes"])
				case "outside page capacity":
					require.Equal(t, "changed later", replay.Rows[1][0])
				}
				require.Equal(t, rows[1:3], replay.Rows, "the reported changes reach the retained source as well as replay")
			})
		}
	}
}

func TestNOR308ReadOnlyReplayControl(t *testing.T) {
	stream, err := NewProgressive([][]any{{"zero"}, {"one"}, {"two"}}, true, 3, nil)
	require.NoError(t, err)
	defer stream.Close()
	for range 3 {
		page, err := stream.Pull(t.Context(), 0, 2)
		require.NoError(t, err)
		require.Equal(t, [][]any{{"zero"}, {"one"}}, page.Rows)
		require.Equal(t, uint64(0), page.Position)
		require.Equal(t, uint64(2), page.Next)
		require.True(t, page.HasMore)
		require.NotNil(t, page.Total)
		require.Equal(t, uint64(3), *page.Total)
	}
	require.NoError(t, stream.Close())
	_, err = stream.Pull(t.Context(), 0, 1)
	require.ErrorIs(t, err, ErrClosed)
}
