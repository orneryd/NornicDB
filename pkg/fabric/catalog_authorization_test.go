package fabric

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFragmentAuthorizesBeforeCatalogLookup(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("hidden", &LocationLocal{DBName: "hidden"})
	denied := errors.New("selection denied")
	exec := NewFabricExecutor(catalog, nil, nil)
	for _, name := range []string{"hidden", "absent"} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			exec.SetGraphAuthorizer(func(ctx context.Context, database, query string) error {
				calls++
				require.Equal(t, name, database)
				require.Equal(t, "RETURN 1 AS n", query)
				return denied
			})
			result, err := exec.Execute(context.Background(), nil, &FragmentExec{Input: &FragmentInit{}, GraphName: name, Query: "RETURN 1 AS n", Columns: []string{"n"}}, nil, "")
			require.ErrorIs(t, err, denied)
			require.Nil(t, result)
			require.Equal(t, 1, calls)
		})
	}
}

func TestElementIDResolutionUsesCatalogVisibility(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("comp.hidden", &LocationLocal{DBName: "hidden"})
	exec := NewFabricExecutor(catalog, nil, nil)
	exec.SetGraphVisibility(func(context.Context, string) bool { return false })
	for _, name := range []string{"hidden", "absent"} {
		_, err := exec.constituentForElementID(context.Background(), "4:"+name+":node", "comp")
		var missing *GraphNotFoundError
		require.ErrorAs(t, err, &missing)
	}
	exec.SetGraphVisibility(func(context.Context, string) bool { return true })
	name, err := exec.constituentForElementID(context.Background(), "4:hidden:node", "comp")
	require.NoError(t, err)
	require.Equal(t, "comp.hidden", name)
}

func TestGraphReferenceRecordingFollowsSuccessfulResolution(t *testing.T) {
	exec := NewFabricExecutor(NewCatalog(), nil, nil)
	authorized, resolved := 0, 0
	exec.SetGraphAuthorizer(func(context.Context, string, string) error { authorized++; return nil })
	exec.SetGraphResolved(func(context.Context, string) error { resolved++; return nil })
	_, _, _, err := exec.prepareExecDispatch(context.Background(), nil, &FragmentExec{GraphName: "absent", Query: "RETURN 1"}, nil)
	require.Error(t, err)
	require.Equal(t, 1, authorized)
	require.Zero(t, resolved)
	exec.catalog.Register("present", &LocationLocal{DBName: "present"})
	_, _, _, err = exec.prepareExecDispatch(context.Background(), nil, &FragmentExec{GraphName: "present", Query: "RETURN 1"}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, authorized)
	require.Equal(t, 1, resolved)
}
