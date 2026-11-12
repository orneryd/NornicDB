package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type rowEndpointFailureStore struct {
	storage.Engine
	operation string
	failure   error
	calls     int
}

func (s *rowEndpointFailureStore) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if s.operation == "node" {
		s.calls++
		if s.failure != nil {
			return nil, s.failure
		}
	}
	return s.Engine.GetNode(ctx, id)
}

func (s *rowEndpointFailureStore) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	if s.operation == "edge" {
		s.calls++
		if s.failure != nil {
			return nil, s.failure
		}
	}
	return s.Engine.GetEdge(ctx, id)
}

func TestRowEndpointPublicReadErrorAndRetry(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, programs := range []bool{false, true} {
				t.Run(fmt.Sprintf("programs=%t", programs), func(t *testing.T) {
					previous := rowValueProgramsDisabled.Swap(!programs)
					t.Cleanup(func() { rowValueProgramsDisabled.Store(previous) })
					for _, expression := range []string{"startNode(r).name", "endNode(r).name", "startNode(r)", "endNode(r)", "apoc.rel.startNode(r)", "apoc.rel.endNode(r)"} {
						for _, operation := range []string{"node", "edge"} {
							t.Run(expression+"/"+operation, func(t *testing.T) {
								store := &rowEndpointFailureStore{Engine: newTestMemoryEngine(t), operation: operation}
								native := storage.NewNamespacedEngine(store, "endpoint_failure")
								_, err := native.CreateNode(t.Context(), &storage.Node{ID: "n", Properties: map[string]any{"name": "kept"}})
								require.NoError(t, err)
								edge := &storage.Edge{ID: "r", StartNode: "n", EndNode: "n", Type: "R"}
								require.NoError(t, native.CreateEdge(t.Context(), edge))
								e := NewStorageExecutor(native)
								query := "WITH $r AS r RETURN [" + expression + "] AS value"
								// Repeated statements qualify both the initial text route and cached programs.
								for range 3 {
									failure := errors.New("endpoint " + operation + " failure")
									store.failure = failure
									store.calls = 0
									result, err := e.Execute(t.Context(), query, map[string]any{"r": edge})
									require.ErrorIs(t, err, failure)
									if result != nil {
										require.Empty(t, result.Rows, "a read error must not produce successful rows")
									}
									require.Equal(t, 1, store.calls, "failed reads must not retry through a different adapter")
								}
								store.failure = nil
								store.calls = 0
								result, err := e.Execute(t.Context(), query, map[string]any{"r": edge})
								require.NoError(t, err)
								require.Greater(t, store.calls, 0)
								require.Len(t, result.Rows, 1)
								require.Len(t, result.Rows[0], 1)
								items, ok := result.Rows[0][0].([]any)
								require.True(t, ok)
								require.Len(t, items, 1)
								if expression == "startNode(r).name" || expression == "endNode(r).name" {
									require.Equal(t, "kept", items[0])
								} else {
									node, ok := items[0].(*storage.Node)
									require.True(t, ok)
									require.Equal(t, "kept", node.Properties["name"])
								}
							})
						}
					}
				})
			}
		})
	}
}

func TestRowEndpointOperationCancellationWithMetadata(t *testing.T) {
	for _, compiled := range []bool{false, true} {
		for _, expression := range []string{"[startNode(r).name]", "[endNode(r).name]", "[startNode(r)]", "[endNode(r)]"} {
			t.Run(fmt.Sprintf("%s/compiled=%t", expression, compiled), func(t *testing.T) {
				store := &callerContextReadStore{Engine: newTestMemoryEngine(t)}
				native := storage.NewNamespacedEngine(store, "endpoint_metadata")
				_, err := native.CreateNode(t.Context(), &storage.Node{ID: "n", Properties: map[string]any{"name": "kept"}})
				require.NoError(t, err)
				edge := &storage.Edge{ID: "r", StartNode: "n", EndNode: "n", Type: "R"}
				require.NoError(t, native.CreateEdge(t.Context(), edge))
				e := NewStorageExecutor(native)
				metadata := context.WithValue(t.Context(), ctxKeyUseDatabase, "endpoint_metadata")
				values := map[string]any{"r": edge, rowStatementContextKey: metadata}
				request, cancel := context.WithCancel(context.WithValue(t.Context(), callerStorageContextKey{}, "operation"))
				t.Cleanup(cancel)
				operation := withExpressionFailureSlot(request)
				store.cancel = cancel
				var value any
				var resolved bool
				if compiled {
					root, _ := (&rowValueCompiler{}).compile(t.Context(), expressionPart(expression))
					value, resolved, err = root.eval(operation, e, values, nil)
				} else {
					value, resolved, err = e.evaluateRowValueText(operation, expression, values)
				}

				require.Nil(t, value)
				require.False(t, resolved)
				require.ErrorIs(t, err, context.Canceled)
				require.NotNil(t, store.seen)
				require.Equal(t, "operation", store.seen.Value(callerStorageContextKey{}))
				require.NoError(t, metadata.Err())
				require.Same(t, metadata, values[rowStatementContextKey])
			})
		}
	}
}

func TestRowEndpointPublicCancellationAndRetry(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, programs := range []bool{false, true} {
				t.Run(fmt.Sprintf("programs=%t", programs), func(t *testing.T) {
					previous := rowValueProgramsDisabled.Swap(!programs)
					t.Cleanup(func() { rowValueProgramsDisabled.Store(previous) })
					for _, expression := range []string{"startNode(r).name", "endNode(r).name", "startNode(r)", "endNode(r)"} {
						t.Run(expression, func(t *testing.T) {
							store := &callerContextReadStore{Engine: newTestMemoryEngine(t)}
							native := storage.NewNamespacedEngine(store, "endpoint_cancel")
							_, err := native.CreateNode(t.Context(), &storage.Node{ID: "n", Properties: map[string]any{"name": "kept"}})
							require.NoError(t, err)
							edge := &storage.Edge{ID: "r", StartNode: "n", EndNode: "n", Type: "R"}
							require.NoError(t, native.CreateEdge(t.Context(), edge))
							e := NewStorageExecutor(native)
							query := "WITH $r AS r RETURN [" + expression + "] AS value"
							for range 3 {
								request, cancel := context.WithCancel(context.WithValue(t.Context(), callerStorageContextKey{}, "request"))
								t.Cleanup(cancel)
								store.cancel = cancel
								store.seen = nil
								result, err := e.Execute(request, query, map[string]any{"r": edge})
								require.ErrorIs(t, err, context.Canceled)
								require.Nil(t, result)
								require.NotNil(t, store.seen, "canceled statements must not bypass the read through cached null")
								require.Equal(t, "request", store.seen.Value(callerStorageContextKey{}))
							}
							reads := store.reads
							result, err := e.Execute(t.Context(), query, map[string]any{"r": edge})
							require.NoError(t, err)
							require.Greater(t, store.reads, reads)
							require.Len(t, result.Rows, 1)
							items, ok := result.Rows[0][0].([]any)
							require.True(t, ok)
							require.Len(t, items, 1)
							if expression == "startNode(r).name" || expression == "endNode(r).name" {
								require.Equal(t, "kept", items[0])
							} else {
								node, ok := items[0].(*storage.Node)
								require.True(t, ok)
								require.Equal(t, "kept", node.Properties["name"])
							}
						})
					}
				})
			}
		})
	}
}
