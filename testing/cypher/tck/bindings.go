package tck

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cucumber/godog"
)

const (
	resetGraphPattern          = `^(?:an empty|any) graph$`
	loadNamedGraphPattern      = `^the (.+) graph$`
	parametersPattern          = `^parameters are:$`
	executeSetupPattern        = `^having executed:$`
	executeQueryPattern        = `^executing (?:control )?query:$`
	emptyResultPattern         = `^the result should be empty$`
	unorderedResultPattern     = `^the result should be, in any order:$`
	orderedResultPattern       = `^the result should be, in order:$`
	unorderedListResultPattern = `^the result should be \(ignoring element order for lists\):$`
	orderedListResultPattern   = `^the result should be, in order \(ignoring element order for lists\):$`
	noSideEffectsPattern       = `^no side effects$`
	sideEffectsPattern         = `^the side effects should be:$`
	expectedErrorPattern       = `^an? (\S+) should be raised at (compile time|runtime|any time): (\S+)$`
	procedurePattern           = `^there exists a procedure (.+):$`
)

var bindingPatterns = []string{
	resetGraphPattern,
	loadNamedGraphPattern,
	parametersPattern,
	executeSetupPattern,
	executeQueryPattern,
	emptyResultPattern,
	unorderedResultPattern,
	orderedResultPattern,
	unorderedListResultPattern,
	orderedListResultPattern,
	noSideEffectsPattern,
	sideEffectsPattern,
	expectedErrorPattern,
	procedurePattern,
}

type scenarioState struct {
	backend Backend
	params  map[string]any
	result  QueryResult
	err     error
	before  GraphSnapshot
	after   GraphSnapshot
}

// RegisterSteps installs all step forms used by the pinned openCypher corpus.
// Each scenario receives a fresh backend from factory.
func RegisterSteps(ctx *godog.ScenarioContext, factory BackendFactory) {
	state := &scenarioState{}
	ctx.Before(func(runCtx context.Context, _ *godog.Scenario) (context.Context, error) {
		backend, err := factory(runCtx)
		if err != nil {
			return runCtx, fmt.Errorf("create scenario backend: %w", err)
		}
		*state = scenarioState{backend: backend, params: map[string]any{}}
		return runCtx, nil
	})
	ctx.After(func(runCtx context.Context, _ *godog.Scenario, scenarioErr error) (context.Context, error) {
		if closer, ok := state.backend.(BackendCloser); ok {
			if err := closer.Close(runCtx); err != nil {
				return runCtx, errors.Join(scenarioErr, fmt.Errorf("close scenario backend: %w", err))
			}
		}
		return runCtx, nil
	})

	ctx.Step(resetGraphPattern, state.resetGraph)
	ctx.Step(loadNamedGraphPattern, state.loadNamedGraph)
	ctx.Step(parametersPattern, state.setParameters)
	ctx.Step(executeSetupPattern, state.executeSetup)
	ctx.Step(executeQueryPattern, state.executeQuery)
	ctx.Step(emptyResultPattern, state.expectEmptyResult)
	ctx.Step(unorderedResultPattern, func(table *godog.Table) error {
		return state.expectResult(table, false, false)
	})
	ctx.Step(orderedResultPattern, func(table *godog.Table) error {
		return state.expectResult(table, true, false)
	})
	ctx.Step(unorderedListResultPattern, func(table *godog.Table) error {
		return state.expectResult(table, false, true)
	})
	ctx.Step(orderedListResultPattern, func(table *godog.Table) error {
		return state.expectResult(table, true, true)
	})
	ctx.Step(noSideEffectsPattern, state.expectNoSideEffects)
	ctx.Step(sideEffectsPattern, state.expectSideEffects)
	ctx.Step(expectedErrorPattern, state.expectError)
	ctx.Step(procedurePattern, state.registerProcedure)
}

func (s *scenarioState) resetGraph(ctx context.Context) error {
	s.params = map[string]any{}
	s.result = QueryResult{}
	s.err = nil
	return s.backend.Reset(ctx)
}

func (s *scenarioState) loadNamedGraph(ctx context.Context, name string) error {
	if err := s.resetGraph(ctx); err != nil {
		return err
	}
	return s.backend.LoadNamedGraph(ctx, name)
}

func (s *scenarioState) setParameters(table *godog.Table) error {
	params := make(map[string]any, len(table.Rows))
	for rowIndex, row := range table.Rows {
		if len(row.Cells) != 2 {
			return fmt.Errorf("parameter row %d has %d cells, want 2", rowIndex, len(row.Cells))
		}
		value, err := ParseValue(row.Cells[1].Value)
		if err != nil {
			return fmt.Errorf("parse parameter %q: %w", row.Cells[0].Value, err)
		}
		params[row.Cells[0].Value] = value
	}
	s.params = params
	return nil
}

func (s *scenarioState) executeSetup(ctx context.Context, query *godog.DocString) error {
	_, err := s.backend.Execute(ctx, query.Content, cloneMap(s.params))
	if err != nil {
		return fmt.Errorf("execute fixture query: %w", err)
	}
	return nil
}

func (s *scenarioState) executeQuery(ctx context.Context, query *godog.DocString) error {
	before, err := s.backend.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot before query: %w", err)
	}
	s.result, s.err = s.backend.Execute(ctx, query.Content, cloneMap(s.params))
	after, snapshotErr := s.backend.Snapshot(ctx)
	if snapshotErr != nil {
		return fmt.Errorf("snapshot after query: %w", snapshotErr)
	}
	s.before = before
	s.after = after
	return nil
}

func (s *scenarioState) expectEmptyResult() error {
	if err := s.requireSuccess(); err != nil {
		return err
	}
	if len(s.result.Rows) != 0 {
		return fmt.Errorf("got %d result rows, want none", len(s.result.Rows))
	}
	return nil
}

func (s *scenarioState) expectResult(table *godog.Table, ordered, ignoreListOrder bool) error {
	if err := s.requireSuccess(); err != nil {
		return err
	}
	expected, err := resultFromTable(table)
	if err != nil {
		return err
	}
	return CompareResults(s.result, expected, ordered, ignoreListOrder)
}

func (s *scenarioState) requireSuccess() error {
	if s.err != nil {
		return fmt.Errorf("query failed unexpectedly: %w", s.err)
	}
	return nil
}

func (s *scenarioState) expectNoSideEffects() error {
	return s.compareSideEffects(SideEffects{})
}

func (s *scenarioState) expectSideEffects(table *godog.Table) error {
	expected := SideEffects{}
	for rowIndex, row := range table.Rows {
		if len(row.Cells) != 2 {
			return fmt.Errorf("side-effect row %d has %d cells, want 2", rowIndex, len(row.Cells))
		}
		count, err := strconv.Atoi(row.Cells[1].Value)
		if err != nil {
			return fmt.Errorf("parse side-effect count %q: %w", row.Cells[1].Value, err)
		}
		if err := assignSideEffect(&expected, row.Cells[0].Value, count); err != nil {
			return err
		}
	}
	return s.compareSideEffects(expected)
}

func (s *scenarioState) compareSideEffects(expected SideEffects) error {
	actual, err := ObserveSideEffects(s.before, s.after)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("side effects differ: got %+v, want %+v", actual, expected)
	}
	return nil
}

func (s *scenarioState) expectError(errorType, phase, detail string) error {
	if s.err == nil {
		return fmt.Errorf("query succeeded, want %s at %s: %s", errorType, phase, detail)
	}
	var queryErr *QueryError
	if !errors.As(s.err, &queryErr) {
		return fmt.Errorf("unclassified query error %T: %w", s.err, s.err)
	}
	if errorType != "Error" && errorType != "*" && queryErr.Type != errorType {
		return fmt.Errorf("error type differs: got %q, want %q", queryErr.Type, errorType)
	}
	if phase != "any time" && queryErr.Phase != phase {
		return fmt.Errorf("error phase differs: got %q, want %q", queryErr.Phase, phase)
	}
	if detail != "*" && queryErr.Detail != detail {
		return fmt.Errorf("error detail differs: got %q, want %q", queryErr.Detail, detail)
	}
	return s.expectNoSideEffects()
}

func (s *scenarioState) registerProcedure(ctx context.Context, signature string, table *godog.Table) error {
	registrar, ok := s.backend.(ProcedureRegistrar)
	if !ok {
		return fmt.Errorf("backend does not support procedure fixture %q", signature)
	}
	rows := make([][]string, len(table.Rows))
	for i, row := range table.Rows {
		rows[i] = make([]string, len(row.Cells))
		for j, cell := range row.Cells {
			rows[i][j] = cell.Value
		}
	}
	return registrar.RegisterProcedure(ctx, signature, rows)
}

func resultFromTable(table *godog.Table) (QueryResult, error) {
	if len(table.Rows) == 0 {
		return QueryResult{}, fmt.Errorf("result table has no header")
	}
	columns := make([]string, len(table.Rows[0].Cells))
	for i, cell := range table.Rows[0].Cells {
		columns[i] = cell.Value
	}
	result := QueryResult{Columns: columns, Rows: make([][]any, 0, len(table.Rows)-1)}
	for rowIndex, row := range table.Rows[1:] {
		if len(row.Cells) != len(columns) {
			return QueryResult{}, fmt.Errorf("result row %d has %d cells, want %d", rowIndex+1, len(row.Cells), len(columns))
		}
		values := make([]any, len(row.Cells))
		for columnIndex, cell := range row.Cells {
			value, err := ParseValue(cell.Value)
			if err != nil {
				return QueryResult{}, fmt.Errorf("parse result row %d column %q: %w", rowIndex+1, columns[columnIndex], err)
			}
			values[columnIndex] = value
		}
		result.Rows = append(result.Rows, values)
	}
	return result, nil
}

func assignSideEffect(target *SideEffects, name string, count int) error {
	switch strings.TrimSpace(name) {
	case "+nodes":
		target.AddedNodes = count
	case "-nodes":
		target.RemovedNodes = count
	case "+relationships":
		target.AddedRelationships = count
	case "-relationships":
		target.RemovedRelationships = count
	case "+properties":
		target.AddedProperties = count
	case "-properties":
		target.RemovedProperties = count
	case "+labels":
		target.AddedLabels = count
	case "-labels":
		target.RemovedLabels = count
	default:
		return fmt.Errorf("unknown side-effect metric %q", name)
	}
	return nil
}

func cloneMap(input map[string]any) map[string]any {
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
