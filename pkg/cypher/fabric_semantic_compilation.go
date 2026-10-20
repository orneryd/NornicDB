package cypher

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// scopeRecording preserves the shared semantic walk's facts for lowering. It
// never infers types or reconstructs scope from a physical fragment or a row.
type scopeRecording struct {
	queries  map[string][]*compiledScopeQuery
	owner    string
	source   string
	cypher25 bool
	names    *quotedVariableNames
	// Only complete root recordings are published. No row values are retained.
	lookupSource, planned string
	sealed                bool
	columnsOnce           sync.Once
	columns               []string
	columnsKnown          bool
	columnsErr            error
}

func newScopeRecording(source string, cypher25 bool) *scopeRecording {
	return &scopeRecording{queries: make(map[string][]*compiledScopeQuery), owner: semanticClauseKey(source), source: source, cypher25: cypher25}
}

// logicalScopeRecording prepares the same native source/name frame for admission,
// physical lowering and metadata. The semantic walk remains the only scope owner.
func (e *StorageExecutor) logicalScopeRecording(source string, cypher25 bool) (string, *scopeRecording) {
	if len(e.fabricRecordBindings) == 0 {
		if record := e.matchSemanticValidationCache.recording(semanticCacheKey(source, cypher25)); record != nil {
			return record.planned, record
		}
	}
	planned := fabric.PreserveNameRoles(source)
	admitted, names := canonicalizeQuotedVariables(fabric.LocalQuery(planned))
	record := newScopeRecording(admitted, cypher25)
	record.names = names
	record.lookupSource, record.planned = source, planned
	return planned, record
}

func (e *StorageExecutor) compileLogicalScopes(ctx context.Context, source string) (string, *scopeRecording, error) {
	planned, record := e.logicalScopeRecording(source, cypherVersionFromContext(ctx) == "25")
	admissionCtx := withQueryParams(ctx, record.names.parameterValues(getParamsFromContext(ctx)))
	if err := e.validateSemanticScopes(admissionCtx, fabric.SemanticQuery(record.source), record); err != nil {
		_, err = record.names.restore(nil, err, e.parseReturnItems)
		return "", nil, err
	}
	return planned, record, nil
}

// resultColumns consumes the first branch's recorded public projection, using
// exactly the frame resolver that supplies physical execution's columns. A unit
// CALL has already exported nothing in that scope; a star expands those facts.
func (record *scopeRecording) resultColumns() ([]string, bool, error) {
	record.columnsOnce.Do(func() {
		record.columns, record.columnsKnown, record.columnsErr = record.resolveResultColumns()
	})
	// Result objects and public metadata callers own their column slice.
	return slices.Clone(record.columns), record.columnsKnown, record.columnsErr
}

func (record *scopeRecording) resolveResultColumns() ([]string, bool, error) {
	projection := fabric.ReadQueryStructure(record.source).ResultClause()
	if projection == nil {
		return nil, false, nil
	}
	queries := record.queries[record.owner]
	if len(queries) == 0 {
		return nil, false, nil
	}
	key := semanticClauseKey(projection.Text)
	for _, step := range queries[0].steps {
		if step.clause != key {
			continue
		}
		inputs := typeScopeColumns(record.sourceTypes(step.before))
		frame, err := record.resolve(projection.Text, inputs, record.source)
		if err != nil {
			return nil, false, err
		}
		return frame.Columns, true, nil
	}
	return nil, false, nil
}

// Semantic compilation and physical pipelines consume the same native clause
// owner. USE and remote LOAD CSV keep their bindings in the logical walk
// without making the native physical pipeline execute those operators.
func semanticScopeClauses(text string) ([]pipelineClause, bool) {
	text, _ = stripUnionBranchFinishes(text)
	clauses, ok, _ := parsePipelineClauses(text, true)
	return clauses, ok
}

type compiledScopeQuery struct {
	globals typeScope
	steps   []compiledScopeStep
}
type compiledScopeStep struct {
	clause        string
	before, after typeScope
}

func firstScopeRecording(recordings []*scopeRecording) *scopeRecording {
	if len(recordings) > 0 {
		return recordings[0]
	}
	return nil
}
func semanticClauseKey(text string) string {
	return strings.TrimSpace(fabric.LocalQuery(fabric.SemanticQuery(text)))
}
func (record *scopeRecording) child(call string) *scopeRecording {
	if record == nil {
		return nil
	}
	return &scopeRecording{queries: record.queries, owner: semanticClauseKey(call), cypher25: record.cypher25, names: record.names}
}
func (record *scopeRecording) begin(globals typeScope) *compiledScopeQuery {
	if record == nil {
		return nil
	}
	query := &compiledScopeQuery{globals: maps.Clone(globals)}
	record.queries[record.owner] = append(record.queries[record.owner], query)
	return query
}
func (query *compiledScopeQuery) add(clause pipelineClause, before, after typeScope) {
	if query != nil {
		key := semanticClauseKey(clause.text)
		if clause.kind == pipelineClauseUse {
			key = ""
		}
		query.steps = append(query.steps, compiledScopeStep{key, before, maps.Clone(after)})
	}
}
func typeScopeColumns(types typeScope) []string {
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// resolve consumes recorded clause transitions. Generated RETURNs use the
// native projection plan over those facts. No semantic checker runs here.
func (record *scopeRecording) resolve(query string, inputs []string, owner string) (*fabric.LogicalFrame, error) {
	query, _ = canonicalizeQuotedVariablesWithBindings(fabric.LocalQuery(query), nil, record.names)
	owner, _ = canonicalizeQuotedVariablesWithBindings(fabric.LocalQuery(owner), nil, record.names)
	clauses, ok := semanticScopeClauses(fabric.SemanticQuery(query))
	if ok {
		inputNames := slices.Clone(inputs)
		if record.names != nil {
			for i, name := range inputNames {
				for internal, original := range record.names.internal {
					if _, parameter := record.names.parameters[internal]; !parameter && original.name == name {
						inputNames[i] = internal
					}
				}
			}
		}
		slices.Sort(inputNames)
		for _, logical := range record.queries[semanticClauseKey(owner)] {
			entry := true
			for start, first := range logical.steps {
				atEntry := entry
				if first.clause != "" {
					entry = false
				}
				names := typeScopeColumns(first.before)
				compatible := slices.Equal(names, inputNames)
				if atEntry {
					compatible = true
					for _, name := range names {
						compatible = compatible && slices.Contains(inputNames, name)
					}
				}
				if !compatible || first.clause != semanticClauseKey(clauses[0].text) {
					continue
				}
				current := first.before
				position := start
				matched := true
				terminalProjection := false
				var columns []string
				output := make(typeScope)
				for _, clause := range clauses {
					key := semanticClauseKey(clause.text)
					// Routing transitions preserve scope without becoming a
					// physical clause in a constituent statement.
					for position < len(logical.steps) && logical.steps[position].clause == "" {
						current = logical.steps[position].after
						position++
					}
					if position < len(logical.steps) && logical.steps[position].clause == key {
						terminalProjection = clause.kind == pipelineClauseReturn
						current = logical.steps[position].after
						position++
					} else if clause.kind != pipelineClauseReturn {
						matched = false
						break
					}
					if clause.kind == pipelineClauseReturn {
						plan := returnProjectionPlanFor(clause.text)
						if plan.star {
							plan = plan.withStarExpanded(typeScopeColumns(current))
						}
						columns = append([]string{}, plan.columns...)
						view := current.static(record.cypher25)
						for _, projection := range plan.projections {
							output[projection.alias] = projectedOperand(current, view, projection.expr)
						}
					}
				}
				if matched {
					if columns == nil {
						columns = typeScopeColumns(current)
						output = maps.Clone(current)
					}
					// Imports survive intermediate physical projections. A recorded
					// logical RETURN exports only its declared columns; hidden
					// transport bindings must not replace the caller's row.
					if !terminalProjection {
						for _, name := range typeScopeColumns(logical.globals) {
							if !slices.Contains(columns, name) {
								columns = append(columns, name)
							}
							if _, exists := output[name]; !exists {
								if operand, bound := current[name]; bound {
									output[name] = operand
								} else {
									output[name] = logical.globals[name]
								}
							}
						}
					}
					inputTypes := record.sourceTypes(first.before)
					globalTypes := record.sourceTypes(logical.globals)
					if record.names != nil {
						for i, name := range columns {
							columns[i] = record.names.restoreColumn(name)
						}
					}
					return &fabric.LogicalFrame{Inputs: typeScopeColumns(inputTypes), Globals: typeScopeColumns(globalTypes), Columns: columns, InputTypes: inputTypes, OutputTypes: record.sourceTypes(output), ReturnProjection: terminalProjection, Cypher25: record.cypher25}, nil
				}
			}
		}
	}
	return nil, localizedError(localization.CypherCommandRoutingFabricPreparedContextInvalid(), nil)
}

func (record *scopeRecording) sourceTypes(types typeScope) typeScope {
	if record.names == nil {
		return maps.Clone(types)
	}
	translated := make(typeScope, len(types))
	for name, operand := range types {
		translated[record.names.restoreColumn(name)] = operand
	}
	return translated
}
