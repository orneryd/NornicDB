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
