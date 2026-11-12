package cypher

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Fail the first read only: a retry through another evaluation route would
// succeed, hiding the original storage error from the statement's caller.
type subqueryLookupFailureEngine struct {
	storage.Engine
	operation string
	failure   error
	calls     atomic.Int64
}

func (s *subqueryLookupFailureEngine) Namespace() string { return "subquery_failure" }

// singleShotLookup owns the operation-selected first-call substitution. Unlike
// a nonnil-error gate, selection also returns a nil/nil pair when configured.
func singleShotLookup[T any](s *subqueryLookupFailureEngine, ctx context.Context, selected bool, read func(context.Context) (T, error)) (T, error) {
	if selected && s.calls.Add(1) == 1 {
		var zero T
		return zero, s.failure
	}
	return read(ctx)
}

// The subquery's node pattern carries the row's key as a property, so the
// pipeline reads its label through the options scan; the fixture fails that
// read as well as the plain label lookup, for whichever read the pipeline
// performs.
func (s *subqueryLookupFailureEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if opts.Label == "Source" {
		_, err := singleShotLookup(s, ctx, s.operation == "node", func(ctx context.Context) (struct{}, error) {
			return struct{}{}, s.Engine.StreamNodesWithOptions(ctx, opts, visit)
		})
		return err
	}
	return s.Engine.StreamNodesWithOptions(ctx, opts, visit)
}

func (s *subqueryLookupFailureEngine) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	return singleShotLookup(s, ctx, s.operation == "node" && label == "Source", func(ctx context.Context) ([]*storage.Node, error) { return s.Engine.GetNodesByLabel(ctx, label) })
}
func (s *subqueryLookupFailureEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if request.Direction == storage.AdjacentIncoming {
		return singleShotLookup(s, ctx, s.operation == "incoming", func(ctx context.Context) ([]*storage.Edge, error) {
			return s.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: request.Node})
		})
	}
	if request.Direction == storage.AdjacentBoth {
		incoming, err := s.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: request.Node, Types: request.Types, Headers: request.Headers})
		if err != nil {
			return nil, err
		}
		outgoing, err := s.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentOutgoing, Node: request.Node, Types: request.Types, Headers: request.Headers})
		if err != nil {
			return nil, err
		}
		combined := make([]*storage.Edge, 0, len(outgoing)+len(incoming))
		combined = append(combined, outgoing...)
		for _, edge := range incoming {
			if edge.StartNode == edge.EndNode {
				continue
			}
			combined = append(combined, edge)
		}
		return combined, nil
	}
	return singleShotLookup(s, ctx, s.operation == "outgoing", func(ctx context.Context) ([]*storage.Edge, error) {
		return s.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: request.Node})
	})
}
func (s *subqueryLookupFailureEngine) GetIncomingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return singleShotLookup(s, ctx, s.operation == "incoming", func(ctx context.Context) ([]*storage.Edge, error) {
		return s.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: id})
	})
}

func TestSubqueryLookupErrorsFailStatement(t *testing.T) {
	for _, shape := range []struct{ name, operation, prefix, body string }{
		{"node", "node", "UNWIND ['present'] AS key", "MATCH (s:Source {id:key})"},
		{"outgoing_degree", "outgoing", "MATCH (n:Start)", "MATCH (n)-[:REL]->()"},
		{"incoming_degree", "incoming", "MATCH (n:Source)", "MATCH (n)<-[:REL]-()"},
		{"chain", "outgoing", "MATCH (n:Start)", "MATCH (n)-[:REL]->(s:Source)"},
	} {
		for _, kind := range []string{"EXISTS", "COUNT"} {
			for _, position := range []string{"RETURN", "WHERE"} {
				t.Run(shape.name+"/"+kind+"/"+position, func(t *testing.T) {
					base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "subquery_failure")
					_, err := NewStorageExecutor(base).Execute(context.Background(), "CREATE (:Start)-[:REL]->(:Source {id:'present'})", nil)
					require.NoError(t, err)
					failure := errors.New("injected subquery storage lookup failure")
					store := &subqueryLookupFailureEngine{Engine: base, operation: shape.operation, failure: failure}
					e := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
					expression := kind + " { " + shape.body + " }"
					query := shape.prefix + " RETURN " + expression + " AS value"
					if position == "WHERE" {
						if kind == "COUNT" {
							expression += " > 0"
						}
						query = shape.prefix + " WITH * WHERE " + expression + " RETURN true AS value"
					}
					_, err = e.Execute(context.Background(), query, nil)
					require.ErrorIs(t, err, failure, "a failed lookup must fail the statement")
					require.Equal(t, int64(1), store.calls.Load(), "the failed read must not be retried")
					result, err := e.Execute(context.Background(), query, nil)
					require.NoError(t, err, "a later statement can perform a successful read")
					var want any = true
					if kind == "COUNT" && position == "RETURN" {
						want = int64(1)
					}
					require.Equal(t, [][]any{{want}}, result.Rows)
				})
			}
		}
	}
}

func TestSubqueryLookupPropertyErrorsFailStatement(t *testing.T) {
	for _, kind := range []string{"EXISTS", "COUNT"} {
		t.Run(kind, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subquery_property_failure"))
			query := fmt.Sprintf("UNWIND [0] AS divisor RETURN %s { MATCH (s:Source {id:1 / divisor}) }", kind)
			_, err := e.Execute(context.Background(), query, nil)
			require.Error(t, err, "a property expression error must fail even on an empty graph")
		})
	}
}
