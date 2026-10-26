package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteEntityPropertyReadRoutes(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		t.Run(kind, func(t *testing.T) {
			entity := storage.RemoteEntityValue{"elementId": "remote:1", "properties": map[string]interface{}{"name": "Remote"}}
			if kind == "node" {
				entity["labels"] = []interface{}{"Doc"}
			} else {
				entity["type"] = "LINKS"
			}
			exec := NewStorageExecutor(newTestMemoryEngine(t))
			for _, expression := range []string{"n.name", "n['name']", "n{.*}", "n{.name}"} {
				t.Run(expression, func(t *testing.T) {
					result, err := exec.Execute(context.Background(), "WITH $entity AS n RETURN "+expression+" AS v", map[string]interface{}{"entity": entity})
					require.NoError(t, err)
					var want interface{} = "Remote"
					if expression == "n{.*}" || expression == "n{.name}" {
						want = map[string]interface{}{"name": "Remote"}
					}
					require.Equal(t, [][]interface{}{{want}}, result.Rows)
				})
			}
		})
	}
}

func TestContextComprehensionRejectsOrdinaryEntityShapedMap(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, expression := range []string{"[x IN [{_edgeId:'e',type:'M'}] | id(x)]", "[x IN [{_edgeId:'e',type:'M'}] | type(x)]"} {
		ctx := context.WithValue(context.Background(), expressionFailureKey{}, &expressionFailure{})
		_ = exec.evaluateExpressionWithContextFull(ctx, originalBraceView(expression), nil, nil, nil, nil, nil, 0)
		require.Error(t, getExpressionFailure(ctx), expression)
	}
}
