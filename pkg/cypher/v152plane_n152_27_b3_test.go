package cypher

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeRootReadRejectsImplicitFanout(t *testing.T) {
	for _, query := range []string{
		"MATCH (n) RETURN count(n)",
		"MATCH (n:Evidence) RETURN n.value",
		"MATCH (n) RETURN n.value",
	} {
		t.Run(query, func(t *testing.T) {
			first := storage.NewMemoryEngine()
			second := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, first.Close()); require.NoError(t, second.Close()) })
			_, err := first.CreateNode(t.Context(), &storage.Node{ID: "first:one", Labels: []string{"Evidence"}, Properties: map[string]interface{}{"value": "first"}})
			require.NoError(t, err)
			_, err = second.CreateNode(t.Context(), &storage.Node{ID: "second:two", Labels: []string{"Evidence"}, Properties: map[string]interface{}{"value": "second"}})
			require.NoError(t, err)
			root := storage.NewCompositeEngine(map[string]storage.Engine{"first": first, "second": second}, nil, map[string]string{"first": "read", "second": "read"})
			accessErr := errors.New("second participant could not open")
			view := root.WithConstituentAccess(nil, func(alias string, engine storage.Engine, _ bool) (storage.Engine, error) {
				if alias == "second" {
					return nil, accessErr
				}
				return engine, nil
			})
			result, err := NewStorageExecutor(view).Execute(t.Context(), query, nil)
			require.ErrorContains(t, err, "Neo.ClientError.Statement.NotAllowed")
			require.Nil(t, result)
		})
	}
}

// Inject the participant lookup failure at the public database-manager boundary.
type failingCompositeAccessManager struct {
	*testDatabaseManagerAdapter
	failure error
}

func (m *failingCompositeAccessManager) GetStorageForUse(name, token string) (interface{}, error) {
	if name == "two" || name == "comp.two" {
		return nil, m.failure
	}
	return m.testDatabaseManagerAdapter.GetStorageForUse(name, token)
}
func TestCompositeExplicitReadReportsAccessFailure(t *testing.T) {
	f := newFabricAtomicityFixture(t)
	for _, name := range []string{"one", "two"} {
		engine, err := f.mgr.GetStorage(name)
		require.NoError(t, err)
		_, err = NewStorageExecutor(engine).Execute(t.Context(), "CREATE (:Evidence {value: 'present'})", nil)
		require.NoError(t, err)
	}
	for _, query := range []string{
		"CALL { USE comp.one MATCH (n:Evidence) RETURN n.value AS value UNION ALL USE comp.two MATCH (n:Evidence) RETURN n.value AS value } RETURN value",
		"CALL { USE comp.one MATCH (n:Evidence) RETURN count(n) AS a } CALL { USE comp.two MATCH (n:Evidence) RETURN count(n) AS b } RETURN a + b AS total",
	} {
		t.Run(query, func(t *testing.T) {
			healthy := newCompositeTestExecutor(t, f.mgr, "comp", &testDatabaseManagerAdapter{manager: f.mgr})
			result, err := healthy.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			require.NotEmpty(t, result.Rows)
			accessErr := errors.New("second participant could not open")
			adapter := &failingCompositeAccessManager{testDatabaseManagerAdapter: &testDatabaseManagerAdapter{manager: f.mgr}, failure: accessErr}
			exec := newCompositeTestExecutor(t, f.mgr, "comp", adapter)
			result, err = exec.Execute(t.Context(), query, nil)
			require.ErrorContains(t, err, accessErr.Error())
			require.Nil(t, result, "do not return partial rows as success")
		})
	}
}
