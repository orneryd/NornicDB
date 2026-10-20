package cypher

import (
	"maps"
	"slices"
	"strings"

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
}

func newScopeRecording(source string, cypher25 bool) *scopeRecording {
	return &scopeRecording{queries: make(map[string][]*compiledScopeQuery), owner: semanticClauseKey(source), source: source, cypher25: cypher25}
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
					globalNames := typeScopeColumns(logical.globals)
					for _, name := range globalNames {
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
					inputTypes := record.sourceTypes(first.before)
					globalTypes := record.sourceTypes(logical.globals)
					if record.names != nil {
						for i, name := range columns {
							columns[i] = record.names.restoreColumn(name)
						}
					}
					return &fabric.LogicalFrame{Inputs: typeScopeColumns(inputTypes), Globals: typeScopeColumns(globalTypes), Columns: columns, InputTypes: inputTypes, OutputTypes: record.sourceTypes(output)}, nil
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
