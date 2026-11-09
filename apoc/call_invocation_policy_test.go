// Root Call keeps its argument admission: it invokes the registered handler
// with the registry's APOC argument policy, so a declaration without a root
// APOC plan stays uncallable and a declared scalar still binds its arguments.
package apoc

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

func TestAPOCCallDelegatesToRegisteredHandlerPolicy(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))

	value, err := Call("apoc.math.ceil", []interface{}{2.5})
	require.NoError(t, err)
	require.Equal(t, float64(3), value)

	// func(int64) int64 has no root APOC argument plan, so the root surface
	// stays closed even though the handler itself is admitted.
	_, err = Call("apoc.math.factorial", []interface{}{int64(3)})
	require.ErrorIs(t, err, registry.ErrUnsupportedSignature)

	// Declared scalar binding is unchanged: a string cannot become an int64.
	_, err = Call("apoc.math.gcd", []interface{}{"12", "8"})
	require.ErrorContains(t, err, "cannot be converted")
}
