// NOR-160 lane (Claude), 30 October 2026: trigger definitions are caller copies.
package trigger

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func TestNOR160RetentionTriggerRegistry(t *testing.T) {
	require.NoError(t, RemoveAll())
	t.Cleanup(func() { _ = RemoveAll() })
	selector := map[string]interface{}{"labels": []interface{}{"Held"}, "where": map[string]interface{}{"k": "v"}}
	require.NoError(t, Add("nor160", "RETURN 1", selector))
	want := readownership.Capture(map[string]interface{}{"labels": []interface{}{"Held"}, "where": map[string]interface{}{"k": "v"}})
	readownership.Mutate(selector)
	for pass := 0; pass < 3; pass++ {
		shown, err := Show("nor160")
		require.NoError(t, err)
		require.Equal(t, want, readownership.Capture(shown.Selector), "pass %d", pass)
		listed := List()
		require.Len(t, listed, 1)
		readownership.Mutate(shown)
		readownership.Mutate(listed)
	}

	imported := []*Trigger{{Name: "imported", Selector: map[string]interface{}{"k": []interface{}{"v"}}}}
	require.NoError(t, Import(imported))
	readownership.Mutate(imported)
	shown, err := Show("imported")
	require.NoError(t, err)
	require.Equal(t, readownership.Capture(map[string]interface{}{"k": []interface{}{"v"}}), readownership.Capture(shown.Selector))
}
