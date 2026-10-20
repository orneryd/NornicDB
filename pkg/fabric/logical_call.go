package fabric

import (
	"context"
	"sort"
	"strings"
)

// A lowered CALL retains the grammar's complete clause. Scope, optionality and
// unit cardinality must not be inferred again from a physical fragment's text.
type callSubqueryBlock = QueryClause

func LogicalFrameFromContext(ctx context.Context) *LogicalFrame {
	return executionValuesFromContext(ctx).frame
}

// CallScope is the logical CALL frame shared by every fragment of one body.
// Globals are scoped imports; Records are the bindings currently in that frame.
type CallScope struct {
	Clause  *QueryClause
	Globals map[string]interface{}
}

func CallScopeFromContext(ctx context.Context) CallScope {
	return executionValuesFromContext(ctx).scope
}

func withCallScope(ctx context.Context, clause *QueryClause, records map[string]interface{}) (context.Context, map[string]interface{}) {
	// Capture the incoming row; the compiler's LogicalFrame alone selects
	// branch inputs and retained imports at dispatch. Reinterpreting CALL
	// scope here would create a second invocation/import decision.
	values := *executionValuesFromContext(ctx)
	values.scope = CallScope{Clause: clause, Globals: records}
	values.records = records
	return withExecutionValues(ctx, values), records
}

func (f *FragmentApply) optional() bool { return f.Call != nil && f.Call.Optional }
func (f *FragmentApply) unit() bool     { return f.Call != nil && f.Call.Nested.ResultClause() == nil }

// Record-only rewrites implement ordinary/optional joins with legacy imports.
// Scoped globals and unit cardinality require the native CALL frame; admitting
// them to a rewrite which has no such frame would discard the logical contract.
func (f *FragmentApply) supportsRecordRewrites() bool {
	if inner, ok := f.Inner.(*FragmentExec); ok && inner.Frame != nil && len(inner.Frame.Globals) > 0 {
		return false
	}
	return f.Call == nil || !f.Call.Scoped && !f.unit()
}

func withLogicalFrame(ctx context.Context, frame *LogicalFrame) context.Context {
	values := *executionValuesFromContext(ctx)
	if frame == nil {
		return withExecutionValues(ctx, values)
	}
	records := values.records
	var selected map[string]interface{}
	if len(frame.Inputs)+len(frame.Globals) > 0 {
		selected = make(map[string]interface{}, len(frame.Inputs)+len(frame.Globals))
	}
	for _, name := range frame.Inputs {
		if value, exists := records[name]; exists {
			selected[name] = value
		}
	}
	scope := values.scope
	var globals map[string]interface{}
	if len(frame.Globals) > 0 {
		globals = make(map[string]interface{}, len(frame.Globals))
	}
	for _, name := range frame.Globals {
		value, exists := selected[name]
		if !exists {
			value = scope.Globals[name]
			selected[name] = value
		}
		globals[name] = value
	}
	scope.Globals = globals
	values.frame, values.scope, values.records = frame, scope, selected
	return withExecutionValues(ctx, values)
}

// RemoteRecordQuery adapts the compiled native frame to remote Cypher. Globals
// are scoped CALL imports; current local bindings are ordinary body projections
// and may be dropped by a later WITH. Local adapters pass the frame directly.
func RemoteRecordQuery(ctx context.Context, query string, records, params map[string]interface{}) (string, map[string]interface{}) {
	if len(records) == 0 {
		return query, params
	}
	parameter := "__fabric_frame_records"
	for {
		if _, exists := params[parameter]; !exists {
			break
		}
		parameter += "_"
	}
	boundParams := make(map[string]interface{}, len(params)+1)
	for name, value := range params {
		boundParams[name] = value
	}
	boundParams[parameter] = records
	globals := CallScopeFromContext(ctx).Globals
	var names, locals []string
	for name := range globals {
		names = append(names, name)
	}
	for name := range records {
		if _, global := globals[name]; !global {
			locals = append(locals, name)
		}
	}
	sort.Strings(names)
	sort.Strings(locals)
	projection := func(bindings []string) string {
		items := make([]string, len(bindings))
		for i, name := range bindings {
			quoted := quoteCypherIdentifier(name)
			items[i] = "$" + parameter + "." + quoted + " AS " + quoted
		}
		if len(items) == 0 {
			return ""
		}
		return "WITH " + strings.Join(items, ", ") + " "
	}
	query = projection(locals) + query
	if len(names) > 0 {
		quoted := make([]string, len(names))
		for i, name := range names {
			quoted[i] = quoteCypherIdentifier(name)
		}
		query = projection(names) + "CALL (" + strings.Join(quoted, ", ") + ") { " + query + " } RETURN *"
	}
	return query, boundParams
}

// SemanticQuery removes graph selectors using their grammar-owned spans. Graph
// resolution is a routing concern; the entire logical scope is validated before
// lowering, caching or executing any physical row.
func SemanticQuery(text string) string {
	q := ReadQueryStructure(text)
	if q.Err != nil {
		return text
	}
	var edits []QueryTextEdit
	var visit func(*QueryStructure)
	visit = func(query *QueryStructure) {
		for _, clause := range query.Clauses {
			if clause.Kind == "USE" {
				edits = append(edits, QueryTextEdit{clause.Start, clause.End, " "})
			}
			if clause.Nested != nil {
				visit(clause.Nested)
			}
		}
	}
	visit(q)
	sort.Slice(edits, func(i, j int) bool { return edits[i].Start < edits[j].Start })
	var out strings.Builder
	start := 0
	for _, edit := range edits {
		out.WriteString(text[start:edit.Start])
		out.WriteString(edit.Text)
		start = edit.End
	}
	out.WriteString(text[start:])
	return out.String()
}
