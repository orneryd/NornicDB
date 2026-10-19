package fabric

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFragmentSelectionAuthorization(t *testing.T) {
	for _, location := range []Location{
		&LocationLocal{DBName: "physical"},
		&LocationRemote{DBName: "physical", URI: "bolt://unused:7687"},
	} {
		catalog := NewCatalog()
		catalog.Register("composite.source", location)
		executor := newTestFabricExecutor(catalog, nil, nil)
		refused := errors.New("selection refused")
		executor.SetGraphAuthorizer(func(_ context.Context, graph, query string) error {
			require.Equal(t, "composite.source", graph)
			require.Equal(t, "RETURN 1 AS one", query)
			return refused
		})
		fragment := &FragmentExec{GraphName: "composite.source", Query: "RETURN 1 AS one"}
		_, _, _, err := executor.prepareExecDispatch(context.Background(), nil, fragment, nil)
		require.ErrorIs(t, err, refused)
	}
}
