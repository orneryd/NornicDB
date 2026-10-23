package fabric

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

// Hand-built fragments declare imports on FragmentInit; planned fragments use
// the bound CALL. Neither route derives binding scope from query text.
func fragmentImportNames(fragment *FragmentExec) (names []string) {
	if fragment.Call != nil {
		names = fragment.Call.Imports
	} else if input, ok := fragment.Input.(*FragmentInit); ok {
		names = input.ImportColumns
		if names == nil {
			names = input.Columns
		}
	}
	return names
}

// serializeImportProjection preserves a bound identity projection while USE is
// removed. Quoting prevents the next clause from turning an imported keyword
// into an operator or modifier. Expressions, DISTINCT and modifiers belong to
// the statement owner; no import scope is inferred from the fragment text.
func serializeImportProjection(clause resultmetadata.Clause, call *resultmetadata.Call) (string, bool) {
	if clause.Kind != resultmetadata.ClauseWith || clause.Projection == nil || !clause.Projection.PassThrough || call == nil || len(call.Imports) == 0 {
		return "", false
	}
	projection := clause.Projection
	var names []string
	projection.Names.Visit(func(known []string) { names = known }, func() {})
	if len(names) == 0 {
		return "", false
	}
	quoted := make([]string, len(names))
	for i, name := range names {
		bound := false
		for _, imported := range call.Imports {
			bound = bound || imported == name
		}
		if !bound {
			return "", false
		}
		quoted[i] = quoteCypherIdentifier(name)
	}
	head := "WITH "
	if projection.Distinct {
		head += "DISTINCT "
	}
	query := head + strings.Join(quoted, ", ")
	if projection.Modifiers != "" {
		query += " " + projection.Modifiers
	}
	if projection.Predicate != "" {
		query += " WHERE " + projection.Predicate
	}
	return query, true
}

// lookupImportPrefix only recognizes the import already declared by the owner.
// It accepts its original simple spelling and the planner's quoted serialization;
// physical lookup recognition never reads an import declaration of its own.
func lookupImportPrefix(fragment *FragmentExec) (name, remaining string, ok bool) {
	names := fragmentImportNames(fragment)
	if len(names) != 1 {
		return "", "", false
	}
	query := strings.TrimSpace(fragment.Query)
	if !startsWithFold(query, "WITH ") {
		return "", "", false
	}
	rest := strings.TrimSpace(query[len("WITH "):])
	for _, written := range []string{names[0], quoteCypherIdentifier(names[0])} {
		if strings.HasPrefix(rest, written) && len(rest) > len(written) && isWhitespace(rest[len(written)]) {
			return names[0], strings.TrimSpace(rest[len(written):]), true
		}
	}
	return "", "", false
}

type retainedFragmentBindingsKey struct{}

func withRetainedFragmentBindings(ctx context.Context, call *resultmetadata.Call, record map[string]interface{}) context.Context {
	var retained map[string]interface{}
	if retainsImports(call) {
		call.Retained.Visit(func(names []string) {
			retained = make(map[string]interface{}, len(names))
			for _, name := range names {
				retained[name] = record[name]
			}
		}, func() {})
	}
	return context.WithValue(ctx, retainedFragmentBindingsKey{}, retained)
}

// RetainedFragmentBindings carries the owner's scoped CALL imports to local
// execution, which keeps them visible through WITH as it does in native CALL.
func RetainedFragmentBindings(ctx context.Context) map[string]interface{} {
	retained, _ := ctx.Value(retainedFragmentBindingsKey{}).(map[string]interface{})
	return retained
}

// factColumnNames adapts the complete owner's ordered columns to the existing
// stream representation. Header inference and transport remain separate owners.
func factColumnNames(columns resultmetadata.Columns) (names []string) {
	columns.Visit(func(known []string) { names = known }, func() {})
	return names
}

func retainsImports(call *resultmetadata.Call) bool {
	return call != nil && call.Mode != resultmetadata.ImportNone
}

// freeName conservatively avoids every occurrence in the source as well as
// supplied parameter keys. It serializes generated names without lexing source.
func freeName(base, source string, taken ...map[string]interface{}) string {
	for i := 0; ; i++ {
		name := base
		if i > 0 {
			name += "_" + strconv.Itoa(i)
		}
		used := strings.Contains(source, name)
		for _, names := range taken {
			if _, exists := names[name]; exists {
				used = true
			}
		}
		if !used {
			return name
		}
	}
}

// remoteImportStatement binds imports under generated parameter names. Original
// parameters and variable names are independent even when their spelling agrees.
func remoteImportStatement(f *FragmentExec, params, record map[string]interface{}) (string, map[string]interface{}) {
	names, err := scopeNames(f.In)
	if err != nil || len(names) == 0 {
		return f.Query, params
	}
	out := make(map[string]interface{}, len(params)+len(names))
	for key, value := range params {
		out[key] = value
	}
	quoted := make([]string, 0, len(names))
	assignments := make([]string, 0, len(names))
	for i, name := range names {
		parameter := freeName("__fabric_import_"+strconv.Itoa(i), f.Query, out)
		out[parameter] = record[name]
		identity := quoteCypherIdentifier(name)
		quoted = append(quoted, identity)
		assignments = append(assignments, "$"+parameter+" AS "+identity)
	}
	head := "WITH " + strings.Join(assignments, ", ")
	if !retainsImports(f.Call) {
		return head + " " + f.Query, out
	}
	query := head + " CALL (" + strings.Join(quoted, ", ") + ") { " + f.Query + " }"
	if len(f.Columns) > 0 {
		returned := make([]string, len(f.Columns))
		for i, name := range f.Columns {
			returned[i] = quoteCypherIdentifier(name)
		}
		query += " RETURN " + strings.Join(returned, ", ")
	}
	return query, out
}
