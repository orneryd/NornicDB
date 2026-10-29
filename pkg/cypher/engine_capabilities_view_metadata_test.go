package cypher

import (
	"context"
	"reflect"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type unavailableDataDecorator struct{ storage.Engine }

func (e *unavailableDataDecorator) ReadCapabilitySource() (storage.Engine, bool) {
	return e.Engine, true
}
func (*unavailableDataDecorator) ReadCapabilityAvailable(reflect.Type) bool { return false }

// Disabling optional graph reads cannot change the database's identity. Both
// the bound entry guard and the session/Fabric consumers need the same owner.
func TestEngineDecoratorsUnavailableDataKeepsViewMetadata(t *testing.T) {
	fixture := newCompositeFixture(t)
	for name, engine := range engineCapabilityStacks(&unavailableDataDecorator{fixture.storage}) {
		t.Run(name, func(t *testing.T) {
			executor := NewStorageExecutor(engine)
			executor.SetDatabaseManager(&decoratedCompositeManager{DatabaseManagerInterface: fixture.dbManager, root: engine})
			graphs, composite := executor.CompositeGraphs()
			require.True(t, composite, "data availability must not hide composite identity")
			require.Equal(t, []string{"pcomp.other", "pcomp.third"}, graphs)
			result, err := executor.Execute(context.Background(), "RETURN graph.names() AS g", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{[]interface{}{"pcomp.other", "pcomp.third"}}}, result.Rows)
			_, err = executor.Execute(context.Background(), "MATCH (n:EI) RETURN n.v", nil)
			require.Error(t, err)
			code, _ := nerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.NotAllowed", code)
			result, err = executor.Execute(context.Background(), "USE graph.byName('pcomp.other') MATCH (n:EI) RETURN n.v", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"o"}}, result.Rows)
		})
	}
}
