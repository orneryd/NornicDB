package fn

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A function registers only with a name, an implementation and a valid
// privilege contract; its contract is listed under its lower-case name.
func TestRegisterRequiresAContract(t *testing.T) {
	impl := func(Context, []string) (interface{}, error) { return nil, nil }
	require.PanicsWithValue(t, "fn.Register: empty name", func() { Register(" ", PrivilegeNone, impl) })
	require.PanicsWithValue(t, "fn.Register: nil function", func() { Register("contract.test", PrivilegeNone, nil) })
	require.PanicsWithValue(t, "fn.Register: explicit privilege class required", func() { Register("contract.test", 0, impl) })

	Register(" Contract.Test ", PrivilegeRead, impl)
	t.Cleanup(func() {
		mu.Lock()
		delete(registry, "contract.test")
		delete(classes, "contract.test")
		mu.Unlock()
	})
	class, found := Class("CONTRACT.test")
	require.True(t, found)
	require.Equal(t, PrivilegeRead, class)
	require.Equal(t, PrivilegeRead, Classes()["contract.test"])
	_, found = Class("contract.missing")
	require.False(t, found)
}
