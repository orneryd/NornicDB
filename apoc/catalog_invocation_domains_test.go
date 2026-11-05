// Author: Codex NOR-184 lane. Whole-catalog result domains; no producer execution.
package apoc

import (
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

func TestAPOCCatalogInvocationDomainsNOR184(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	counts := map[string]int{}
	for _, name := range List() {
		t.Run(name, func(t *testing.T) {
			info, found := GetInfo(name)
			require.True(t, found)
			require.NotNil(t, info.Handler)
			signature := info.Handler.Signature()
			require.NotNil(t, signature)
			require.Equal(t, reflect.Func, signature.Kind())
			errors := 0
			for i := 0; i < signature.NumOut(); i++ {
				if signature.Out(i).Implements(errorType) {
					errors++
					require.Equal(t, signature.NumOut()-1, i)
					require.LessOrEqual(t, signature.NumOut(), 2)
				}
			}
			switch {
			case errors > 0:
				counts["checked_error"]++
				require.Equal(t, 1, errors)
			case signature.NumOut() > 1:
				counts["tuple_metadata"]++
			case signature.NumOut() == 0:
				counts["void"]++
			default:
				if signature.Out(0).Kind() == reflect.Func {
					counts["capability"]++
				} else {
					counts["value"]++
				}
			}
		})
	}
	for _, role := range []string{"checked_error", "tuple_metadata", "void", "capability", "value"} {
		require.Positive(t, counts[role], role)
	}
	t.Logf("catalog result domains: %v", counts)
}
