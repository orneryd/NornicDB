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

func (s *subqueryLookupFailureEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if s.operation == "node" && label == "Source" && s.calls.Add(1) == 1 {
		return nil, s.failure
	}
	return s.Engine.GetNodesByLabel(label)
}

func (s *subqueryLookupFailureEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if s.operation == "outgoing" && s.calls.Add(1) == 1 {
		return nil, s.failure
	}
	return s.Engine.GetOutgoingEdges(id)
}

func (s *subqueryLookupFailureEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if s.operation == "incoming" && s.calls.Add(1) == 1 {
		return nil, s.failure
	}
	return s.Engine.GetIncomingEdges(id)
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
					e := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
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
