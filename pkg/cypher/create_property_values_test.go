// Author: Claude R17 implementation agent, 30 October 2026.
package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCreatePropertyValuesReadOnceMatchesTheMapReader: the one CREATE
// planner reads a property map once per text and evaluates it per row with
// the same values materializePropertyMap gives, and a map the reader rejects
// still gets that reader's error on every row.
func TestCreatePropertyValuesReadOnceMatchesTheMapReader(t *testing.T) {
	exec := &StorageExecutor{}
	for _, text := range []string{"{id: id, name: name}", "{w: id + 1}", "{id: id, missing: null}", "{}"} {
		for _, row := range []pipelineRow{{"id": int64(1), "name": "a"}, {"id": int64(2), "name": "b"}} {
			want, wantErr := exec.materializePropertyMap(t.Context(), text, row, false, false)
			got, err := exec.createPropertyValues(t.Context(), text, row)
			require.Equal(t, wantErr, err, text)
			require.Equal(t, want, got, text)
		}
	}
	for range 2 {
		_, wantErr := exec.materializePropertyMap(t.Context(), "{id: }", pipelineRow{}, false, false)
		_, err := exec.createPropertyValues(t.Context(), "{id: }", pipelineRow{})
		require.Error(t, err)
		require.Equal(t, wantErr.Error(), err.Error())
	}
}
