package tck

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cucumber/godog"
)

type contractBackend struct {
	snapshot   GraphSnapshot
	procedures map[string][][]string
	closed     bool
}

func (b *contractBackend) Reset(context.Context) error {
	b.snapshot = GraphSnapshot{}
	b.procedures = map[string][][]string{}
	return nil
}

func (b *contractBackend) LoadNamedGraph(_ context.Context, name string) error {
	if name != "binary-tree-1" {
		return errors.New("unknown named graph")
	}
	b.snapshot.Nodes = []NodeValue{{Identity: "root", Labels: []string{"Root"}}}
	return nil
}

func (b *contractBackend) Execute(_ context.Context, query string, params map[string]any) (QueryResult, error) {
	switch strings.TrimSpace(query) {
	case "CREATE FIXTURE":
		b.snapshot.Nodes = []NodeValue{{
			Identity: "fixture", Labels: []string{"Fixture"}, Properties: map[string]any{"name": "old"},
		}}
		return QueryResult{}, nil
	case "RETURN PARAMETER":
		return QueryResult{Columns: []string{"value"}, Rows: [][]any{{params["value"]}, {int64(2)}}}, nil
	case "REPLACE PROPERTY":
		b.snapshot.Nodes[0].Properties["name"] = "new"
		return QueryResult{}, nil
	case "CREATE THEN DELETE":
		return QueryResult{}, nil
	case "FAIL CLASSIFIED":
		return QueryResult{}, &QueryError{Type: "TypeError", Phase: "runtime", Detail: "InvalidArgumentValue"}
	case "FAIL UNCLASSIFIED":
		return QueryResult{}, errors.New("raw backend error")
	case "RETURN INTEGER":
		return QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}}}, nil
	default:
		return QueryResult{}, errors.New("unexpected query")
	}
}

func (b *contractBackend) Snapshot(context.Context) (GraphSnapshot, error) {
	return cloneSnapshot(b.snapshot), nil
}

func (b *contractBackend) RegisterProcedure(_ context.Context, signature string, rows [][]string) error {
	b.procedures[signature] = rows
	return nil
}

func (b *contractBackend) Close(context.Context) error {
	b.closed = true
	return nil
}

func cloneSnapshot(input GraphSnapshot) GraphSnapshot {
	result := GraphSnapshot{
		Nodes:         make([]NodeValue, len(input.Nodes)),
		Relationships: make([]RelationshipValue, len(input.Relationships)),
	}
	for i, node := range input.Nodes {
		result.Nodes[i] = node
		result.Nodes[i].Labels = append([]string(nil), node.Labels...)
		result.Nodes[i].Properties = cloneMap(node.Properties)
	}
	for i, relationship := range input.Relationships {
		result.Relationships[i] = relationship
		result.Relationships[i].Properties = cloneMap(relationship.Properties)
	}
	return result
}

func TestBindingsExecuteTypedResultsAndObservableChecks(t *testing.T) {
	feature := `Feature: adapter contract
  Scenario: parameters and unordered rows retain their types
    Given an empty graph
    And parameters are:
      | value | 1 |
    When executing query:
      """
      RETURN PARAMETER
      """
    Then the result should be, in any order:
      | value |
      | 2     |
      | 1     |
    And no side effects

  Scenario: property replacement is observable in both directions
    Given an empty graph
    And having executed:
      """
      CREATE FIXTURE
      """
    When executing query:
      """
      REPLACE PROPERTY
      """
    Then the result should be empty
    And the side effects should be:
      | +properties | 1 |
      | -properties | 1 |

  Scenario: classified errors imply rollback
    Given an empty graph
    When executing query:
      """
      FAIL CLASSIFIED
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue

  Scenario: procedure fixtures are delegated to the backend
    Given any graph
    And there exists a procedure test.values() :: (value :: INTEGER?):
      | value |
      | 1     |
    When executing query:
      """
      CREATE THEN DELETE
      """
    Then the result should be empty
    And no side effects
`
	var backends []*contractBackend
	status, output := runHarnessContract(feature, func(ctx *godog.ScenarioContext) {
		RegisterSteps(ctx, func(context.Context) (Backend, error) {
			backend := &contractBackend{}
			backends = append(backends, backend)
			return backend, nil
		})
	})
	if status != 0 {
		t.Fatalf("bindings failed with status %d:\n%s", status, output)
	}
	if len(backends) != 4 {
		t.Fatalf("created %d backends, want one per scenario", len(backends))
	}
	for _, backend := range backends {
		if !backend.closed {
			t.Fatal("scenario backend was not closed")
		}
	}
	if _, ok := backends[3].procedures["test.values() :: (value :: INTEGER?)"]; !ok {
		t.Fatal("procedure fixture was not registered")
	}
}

func TestBindingsRejectTypedResultMismatch(t *testing.T) {
	feature := `Feature: adapter negative control
  Scenario: integers do not equal floats
    Given an empty graph
    When executing query:
      """
      RETURN INTEGER
      """
    Then the result should be, in order:
      | value |
      | 1.0   |
`
	status, _ := runHarnessContract(feature, func(ctx *godog.ScenarioContext) {
		RegisterSteps(ctx, func(context.Context) (Backend, error) { return &contractBackend{}, nil })
	})
	if status == 0 {
		t.Fatal("bindings accepted a typed result mismatch")
	}
}

func TestBindingsRejectUnclassifiedErrors(t *testing.T) {
	feature := `Feature: adapter negative control
  Scenario: raw errors cannot satisfy a classified expectation
    Given an empty graph
    When executing query:
      """
      FAIL UNCLASSIFIED
      """
    Then an Error should be raised at any time: *
`
	status, _ := runHarnessContract(feature, func(ctx *godog.ScenarioContext) {
		RegisterSteps(ctx, func(context.Context) (Backend, error) { return &contractBackend{}, nil })
	})
	if status == 0 {
		t.Fatal("bindings accepted an unclassified backend error")
	}
}
