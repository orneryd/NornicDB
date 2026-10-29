package multidb

import (
	"context"
	"errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestInstalledConstraintOwnerThroughDatabaseWrites(t *testing.T) {
	for _, route := range []string{"create", "update", "bulk", "commit"} {
		t.Run(route, func(t *testing.T) {
			manager, database := setupTestManager(t)
			engine, err := manager.GetStorage(database)
			require.NoError(t, err)
			require.NoError(t, engine.GetSchema().AddConstraint(storage.Constraint{
				Name: "required", Type: storage.ConstraintNodeKey, Label: "K", Properties: []string{"required"},
			}))
			if route == "update" {
				_, err = engine.CreateNode(&storage.Node{ID: "subject", Labels: []string{"K"}, Properties: map[string]any{"required": int64(1)}})
				require.NoError(t, err)
			}
			write := func(id string, properties map[string]any) error {
				node := &storage.Node{ID: storage.NodeID(id), Labels: []string{"K"}, Properties: properties}
				switch route {
				case "create":
					_, err := engine.CreateNode(node)
					return err
				case "update":
					node.ID = "subject"
					return engine.UpdateNode(node)
				case "bulk":
					return engine.BulkCreateNodes([]*storage.Node{node})
				default:
					tx, err := manager.inner.(*storage.MemoryEngine).BeginTransaction()
					if err != nil {
						return err
					}
					defer tx.Rollback()
					if err = tx.SetNamespace(database); err != nil {
						return err
					}
					node.ID = storage.NodeID(database + ":" + id)
					if _, err = tx.CreateNode(node); err != nil {
						return err
					}
					return engine.(*sizeTrackingEngine).CommitTransaction(context.Background(), tx)
				}
			}
			err = write("first", map[string]any{"replacement": int64(2)})
			require.Error(t, err)
			var violation *storage.ConstraintViolationError
			require.True(t, errors.As(err, &violation))
			require.Equal(t, []string{"required"}, violation.Properties)
			violation.Properties[0] = "replacement"
			err = write("second", map[string]any{"replacement": int64(2)})
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", code)
			require.NoError(t, write("valid", map[string]any{"required": int64(2)}))
			nodes, err := engine.GetNodesByLabel("K")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			require.Equal(t, []string{"required"}, engine.GetSchema().GetAllConstraints()[0].Properties)
		})
	}
}
