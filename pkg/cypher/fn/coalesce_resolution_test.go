package fn

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoalescePreservesResolution(t *testing.T) {
	for _, tc := range []struct {
		name         string
		value        interface{}
		resolved     bool
		err          error
		want         interface{}
		wantResolved bool
		calls        int
	}{
		{"unresolved", map[string]interface{}{"incidental": 1}, false, nil, nil, false, 1},
		{"error", map[string]interface{}{"incidental": 1}, true, fmt.Errorf("evaluation failed"), nil, false, 1},
		{"null", nil, true, nil, int64(7), true, 2},
		{"typed-null", map[string]interface{}(nil), true, nil, int64(7), true, 2},
		{"empty-map", map[string]interface{}{}, true, nil, map[string]interface{}{}, true, 1},
		{"false", false, true, nil, false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			value, resolved, err := Coalesce(2, func(i int) (interface{}, bool, error) {
				calls++
				if i == 0 {
					return tc.value, tc.resolved, tc.err
				}
				return int64(7), true, nil
			})
			require.Equal(t, tc.want, value)
			require.Equal(t, tc.wantResolved, resolved)
			require.Equal(t, tc.calls, calls)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
