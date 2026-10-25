package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReplicatedCypherKeepsInstalledConstraintOwner(t *testing.T) {
	fixture := setupTestAdapter(t, adapterFixtureOptions{standalone: true})
	executor := cypher.NewStorageExecutor(fixture.engine)
	require.NoError(t, fixture.engine.GetSchema().AddConstraint(storage.Constraint{
		Name: "required", Type: storage.ConstraintNodeKey, Label: "K", Properties: []string{"required"},
	}))
	apply := func(query string) error {
		_, err := executor.Execute(context.Background(), query, nil)
		return err
	}
	err := apply("CREATE (:K {replacement:2})")
	require.Error(t, err)
	var violation *storage.ConstraintViolationError
	require.True(t, errors.As(err, &violation))
	require.Equal(t, []string{"required"}, violation.Properties)
	violation.Properties[0] = "replacement"
	err = apply("CREATE (:K {replacement:2})")
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", code)
	require.NoError(t, apply("CREATE (:K {required:2})"))
	nodes, err := fixture.engine.GetNodesByLabel("K")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
}
