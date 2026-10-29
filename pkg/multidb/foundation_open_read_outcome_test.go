// Author: Lane #1644 (Codex). Opening consumes the persisted migration state;
// an unknown state cannot be replaced by an absent marker or a successful ACK.
package multidb_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type foundationMigrationFault struct {
	storage.Engine
	operation string
	cause     error
	visits    int
}

// GetInnerEngine exposes only physical preparation/publication ownership for
// the manager's receipt-based catalog bootstrap. Reads and marker writes still
// execute on this fault view, so the public oracle must observe their failure.
// Author: Lane #1644 (Codex), reconciling the existing fixture with TJ's owner.
func (v *foundationMigrationFault) GetInnerEngine() storage.Engine { return v.Engine }

func (v *foundationMigrationFault) fail(id storage.NodeID, operation string) error {
	if strings.HasSuffix(string(id), "migration:legacy_data") && v.operation == operation {
		v.visits++
		return v.cause
	}
	return nil
}
func (v *foundationMigrationFault) GetNode(id storage.NodeID) (*storage.Node, error) {
	if err := v.fail(id, "read"); err != nil {
		return nil, err
	}
	return v.Engine.GetNode(id)
}
func (v *foundationMigrationFault) UpdateNode(node *storage.Node) error {
	if err := v.fail(node.ID, "write"); err != nil {
		return err
	}
	return v.Engine.UpdateNode(node)
}
func (v *foundationMigrationFault) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if err := v.fail(node.ID, "write"); err != nil {
		return "", err
	}
	return v.Engine.CreateNode(node)
}

func TestFoundationPublicOpenReadOutcome(t *testing.T) {
	for _, operation := range []string{"healthy", "read", "write"} {
		t.Run(operation, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			cause := errors.New("required migration marker operation failed")
			view := &foundationMigrationFault{Engine: base, operation: operation, cause: cause}
			manager, err := multidb.NewDatabaseManager(t.Context(), view, nil)
			if manager != nil {
				t.Cleanup(func() { _ = manager.Close() })
			}
			if operation == "healthy" {
				require.NoError(t, err)
				require.NotNil(t, manager)
				return
			}
			require.Positive(t, view.visits, "public opening must reach the failed marker operation")
			require.ErrorIs(t, err, cause)
			require.Nil(t, manager)
		})
	}
}
