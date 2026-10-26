// Author: Lane #1644 (Codex). Public DELETE completion must retain the
// issued mutation interceptor's failure instead of reporting zero success.
package cypher_test

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type foundationDeleteFault struct {
	storage.Engine
	cause  error
	visits int
}

func (v *foundationDeleteFault) Namespace() string { return storage.DatabaseOf(v.Engine) }
func (v *foundationDeleteFault) DeleteNode(id storage.NodeID) error {
	v.visits++
	if v.cause != nil {
		return v.cause
	}
	return v.Engine.DeleteNode(id)
}
func TestFoundationPublicDeleteOutcome(t *testing.T) {
	for _, fault := range []bool{false, true} {
		name := "healthy"
		if fault {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			tenant := storage.NewNamespacedEngine(base, "tenant")
			_, err := tenant.CreateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}})
			require.NoError(t, err)
			cause := errors.New("issued deletion failed")
			view := &foundationDeleteFault{Engine: tenant}
			if fault {
				view.cause = cause
			}
			result, err := cypher.NewStorageExecutor(view).Execute(context.Background(), "MATCH (n:Doc) WITH n LIMIT 1 DETACH DELETE n", nil)
			require.Positive(t, view.visits, "public DELETE must consume the issued interceptor")
			if fault {
				require.ErrorIs(t, err, cause, "failed exact mutation cannot become successful zero deletion: %#v", result)
				_, err = tenant.GetNode("n")
				require.NoError(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, result.Stats.NodesDeleted)
			}
		})
	}
}
