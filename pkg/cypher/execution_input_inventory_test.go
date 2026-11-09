package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/collection"
	"github.com/stretchr/testify/require"
)

// Reconcile live registry handlers against owned source, including spec factories
// and aliases. Casting an argument value back to executable text introduces a
// dependency even when the handler's mode already forbids result caching.
//
// The walk follows the handler the registry actually runs, so a typed
// declaration a factory closes over is part of the answer: a branch whose guard
// names a different constant than the registered declaration supplies cannot
// run, and the text interpretation behind it is not this procedure's
// dependency. Everything the source cannot prove stays an over-approximation.
func TestExecutionInputRegistryInventory(t *testing.T) {
	source, err := loadInventorySource(".")
	require.NoError(t, err)
	methods, literals := source.methods, source.literals

	// Source-derived string-value adapters. Minimum argument count separates
	// statistics collect/retrieve config from status/clear/stop and fixed calls.
	rawText := map[string]int{}
	for name, method := range methods {
		variables := map[string]int{}
		ast.Inspect(method.Body, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || len(assignment.Rhs) != 1 || len(assignment.Lhs) == 0 {
				return true
			}
			cast, ok := assignment.Rhs[0].(*ast.TypeAssertExpr)
			if !ok {
				return true
			}
			kind, ok := cast.Type.(*ast.Ident)
			variable, named := assignment.Lhs[0].(*ast.Ident)
			if !ok || kind.Name != "string" || !named {
				return true
			}
			minimum := 0
			if indexed, ok := cast.X.(*ast.IndexExpr); ok {
				if literal, ok := indexed.Index.(*ast.BasicLit); ok && literal.Kind == token.INT {
					if index, err := strconv.Atoi(literal.Value); err == nil {
						minimum = index + 1
					}
				}
			}
			variables[variable.Name] = minimum
			return true
		})
		ast.Inspect(method.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !(strings.HasPrefix(selector.Sel.Name, "evaluate") || selector.Sel.Name == "executeInternal" || selector.Sel.Name == "Eval") {
				return true
			}
			for _, argument := range call.Args {
				if variable, ok := argument.(*ast.Ident); ok {
					if minimum, valueText := variables[variable.Name]; valueText {
						rawText[name] = minimum
					}
				}
			}
			return true
		})
	}
	ensureBuiltInProceduresRegistered()
	var discovered []string
	for _, spec := range globalProcedureRegistry.ListBuiltIns() {
		globalProcedureRegistry.mu.RLock()
		procedure := globalProcedureRegistry.builtins[spec.Name]
		globalProcedureRegistry.mu.RUnlock()
		function := runtime.FuncForPC(reflect.ValueOf(procedure.Handler).Pointer())
		sourcePath, line := function.FileLine(function.Entry())
		var handler *ast.FuncLit
		for _, literal := range literals[filepath.Base(sourcePath)] {
			if source.positions.Position(literal.Pos()).Line <= line && source.positions.Position(literal.End()).Line >= line {
				if handler == nil || literal.End()-literal.Pos() < handler.End()-handler.Pos() {
					handler = literal
				}
			}
		}
		require.NotNil(t, handler, "%s: resolve actual registered handler at %s:%d", spec.Name, sourcePath, line)
		// A handler a factory builds closes over the declaration it was
		// registered with; that argument is the only way to tell the four
		// apoc.export.* procedures apart, since they share one literal.
		frame := source.registrationFrame(spec.Name, handler)
		for _, parameter := range handler.Type.Params.List {
			pointer, ok := parameter.Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			kind, ok := pointer.X.(*ast.Ident)
			if !ok || kind.Name != "StorageExecutor" || len(parameter.Names) == 0 {
				continue
			}
			for _, call := range source.calls(handler.Body, parameter.Names[0].Name, frame) {
				if !source.reachesText(call, spec.MaxArgs, rawText, map[string]bool{}) {
					continue
				}
				require.NotEmpty(t, spec.ResultCache.CypherInputs, "%s interprets text through %s", spec.Name, call.name)
				discovered = append(discovered, spec.Name)
				break
			}
		}
	}
	require.GreaterOrEqual(t, len(discovered), 19)
	t.Logf("source-derived executable-text procedures (%d): %v; string-value adapters: %v", len(discovered), discovered, rawText)
	// Installation defers interpretation until a later custom procedure CALL.
	for _, name := range []string{"apoc.custom.installProcedure", "apoc.custom.declareProcedure"} {
		procedure, ok := globalProcedureRegistry.Get(name)
		require.True(t, ok, name)
		require.NotEmpty(t, procedure.Spec.ResultCache.CypherInputs, name)
		require.False(t, procedure.Spec.ResultCache.Deterministic, name)
	}
}

// inventorySource indexes the package's own declarations: the executor methods
// and package functions the walk follows, the handler literals the registry
// points into, the package variables and constants a typed declaration owns, and
// the handler expression each procedure name was registered with.
type inventorySource struct {
	positions token.FileSet
	methods   map[string]*ast.FuncDecl
	functions map[string]*ast.FuncDecl
	literals  map[string][]*ast.FuncLit
	variables map[string]ast.Expr
	types     map[string]ast.Expr
	constants map[string]string
	handlers  map[string]ast.Expr
}

// inventoryFrame carries what the walk proves at one point of a handler: the
// argument expression a factory parameter was registered with, and the integer
// constants the current function's parameters provably hold.
type inventoryFrame struct {
	bindings  map[string]ast.Expr
	constants map[string]string
	unknown   map[string]bool
}

// inventoryCall is one call the analysed body makes on the executor, with the
// frame its enclosing conditions prove for that call.
type inventoryCall struct {
	name      string
	arguments []ast.Expr
	frame     inventoryFrame
}

// inventoryRegistrars publish one procedure as (name, ..., handler): their last
// argument is the handler expression the registry keeps.
var inventoryRegistrars = map[string]bool{
	"registerBuiltInProcedure":             true,
	"registerBuiltInProcedureLiteral":      true,
	"registerStatementProcedureLiteral":    true,
	"registerAdminBuiltInProcedure":        true,
	"registerAdminBuiltInProcedureLiteral": true,
}

func loadInventorySource(directory string) (*inventorySource, error) {
	source := &inventorySource{
		methods:   map[string]*ast.FuncDecl{},
		functions: map[string]*ast.FuncDecl{},
		literals:  map[string][]*ast.FuncLit{},
		variables: map[string]ast.Expr{},
		types:     map[string]ast.Expr{},
		constants: map[string]string{},
		handlers:  map[string]ast.Expr{},
	}
	packages, err := parser.ParseDir(&source.positions, directory, func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	if err != nil {
		return nil, err
	}
	for filename, file := range packages["cypher"].Files {
		ast.Inspect(file, func(node ast.Node) bool {
			if literal, ok := node.(*ast.FuncLit); ok {
				source.literals[filepath.Base(filename)] = append(source.literals[filepath.Base(filename)], literal)
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			registrar, ok := call.Fun.(*ast.Ident)
			if !ok || !inventoryRegistrars[registrar.Name] || len(call.Args) < 2 {
				return true
			}
			name, ok := call.Args[0].(*ast.BasicLit)
			if !ok || name.Kind != token.STRING {
				return true
			}
			if registered, err := strconv.Unquote(name.Value); err == nil {
				source.handlers[registered] = call.Args[len(call.Args)-1]
			}
			return true
		})
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				if typed.Recv == nil {
					source.functions[typed.Name.Name] = typed
					continue
				}
				receiver, ok := typed.Recv.List[0].Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				if name, ok := receiver.X.(*ast.Ident); ok && name.Name == "StorageExecutor" {
					source.methods[typed.Name.Name] = typed
				}
			case *ast.GenDecl:
				source.indexGenDecl(typed)
			}
		}
	}
	frame := inventoryFrame{bindings: source.variables}
	for _, file := range packages["cypher"].Files {
		for _, declaration := range file.Decls {
			switch value := declaration.(type) {
			case *ast.FuncDecl:
				if value.Body != nil {
					frame = source.dropWrittenBindings(frame, value.Body)
				}
			case *ast.GenDecl:
				for _, specification := range value.Specs {
					if values, ok := specification.(*ast.ValueSpec); ok {
						for _, expression := range values.Values {
							frame = source.dropWrittenBindings(frame, expression)
						}
					}
				}
			}
		}
	}
	source.variables = frame.bindings
	return source, nil
}

// indexGenDecl records the package variables a typed declaration is initialized
// with and the integer value each constant block assigns, including iota.
func (s *inventorySource) indexGenDecl(declaration *ast.GenDecl) {
	previous := []ast.Expr(nil)
	for index, specification := range declaration.Specs {
		if defined, ok := specification.(*ast.TypeSpec); ok {
			s.types[defined.Name.Name] = defined.Type
			continue
		}
		values, ok := specification.(*ast.ValueSpec)
		if !ok {
			continue
		}
		if declaration.Tok == token.VAR {
			for position, name := range values.Names {
				if position < len(values.Values) {
					s.variables[name.Name] = values.Values[position]
				}
			}
			continue
		}
		if declaration.Tok != token.CONST {
			continue
		}
		expressions := values.Values
		if len(expressions) == 0 {
			expressions = previous
		}
		previous = expressions
		for position, name := range values.Names {
			if len(expressions) == 0 {
				continue
			}
			expression := expressions[min(position, len(expressions)-1)]
			if iota, ok := expression.(*ast.Ident); ok && iota.Name == "iota" {
				s.constants[name.Name] = strconv.Itoa(index)
				continue
			}
			if literal, ok := expression.(*ast.BasicLit); ok && literal.Kind == token.INT {
				s.constants[name.Name] = literal.Value
			}
		}
	}
}

// registrationFrame binds the factory parameters of the function that built a
// registered handler to the arguments that registration passed, so the typed
// declaration the handler closed over is readable at its call sites. A handler
// registered directly has no factory and therefore no bindings.
func (s *inventorySource) registrationFrame(name string, handler *ast.FuncLit) inventoryFrame {
	expression, ok := s.handlers[name]
	if !ok {
		return inventoryFrame{}
	}
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return inventoryFrame{}
	}
	factory, ok := call.Fun.(*ast.Ident)
	if !ok {
		return inventoryFrame{}
	}
	declaration := s.functions[factory.Name]
	if declaration == nil || declaration.Body == nil {
		return inventoryFrame{}
	}
	found := false
	ast.Inspect(declaration.Body, func(node ast.Node) bool {
		if literal, ok := node.(*ast.FuncLit); ok && literal == handler {
			found = true
			return false
		}
		return true
	})
	if !found {
		return inventoryFrame{}
	}
	parameters := parameterNames(declaration.Type.Params)
	bindings := make(map[string]ast.Expr, len(parameters))
	unknown := make(map[string]bool, len(parameters))
	for index, parameter := range parameters {
		unknown[parameter] = true
		if index < len(call.Args) {
			if identifier, ok := call.Args[index].(*ast.Ident); ok && s.variables[identifier.Name] != nil {
				bindings[parameter] = call.Args[index]
				delete(unknown, parameter)
			}
		}
	}
	return s.dropWrittenBindings(inventoryFrame{bindings: bindings, unknown: unknown}, declaration.Body)
}

// constantValue reports the integer constant an expression provably holds in
// frame: a parameter's known constant, a named constant, or one field of the
// typed declaration a factory parameter was registered with. Anything else is
// unknown, which keeps the walk an over-approximation.
func (s *inventorySource) constantValue(expression ast.Expr, frame inventoryFrame) (string, bool) {
	switch value := expression.(type) {
	case *ast.BasicLit:
		if value.Kind == token.INT {
			return value.Value, true
		}
	case *ast.Ident:
		if frame.unknown[value.Name] {
			return "", false
		}
		if bound, ok := frame.bindings[value.Name]; ok {
			return s.constantValue(bound, inventoryFrame{})
		}
		if constant, ok := frame.constants[value.Name]; ok {
			return constant, true
		}
		if constant, ok := s.constants[value.Name]; ok {
			return constant, true
		}
	case *ast.SelectorExpr:
		receiver, ok := value.X.(*ast.Ident)
		if !ok {
			return "", false
		}
		bound, ok := frame.bindings[receiver.Name]
		if !ok {
			return "", false
		}
		literal, ok := s.compositeLiteral(bound)
		if !ok {
			return "", false
		}
		for _, element := range literal.Elts {
			entry, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := entry.Key.(*ast.Ident)
			if !ok || key.Name != value.Sel.Name {
				continue
			}
			return s.constantValue(entry.Value, inventoryFrame{})
		}
	}
	return "", false
}

// compositeLiteral follows a package variable to the composite literal it is
// initialized with.
func (s *inventorySource) compositeLiteral(expression ast.Expr) (*ast.CompositeLit, bool) {
	switch value := expression.(type) {
	case *ast.CompositeLit:
		return value, true
	case *ast.Ident:
		if initializer, ok := s.variables[value.Name]; ok {
			return s.compositeLiteral(initializer)
		}
	}
	return nil, false
}

// calleeFrame binds one call's arguments to the callee's parameters, keeping only
// the constants the caller's frame proves.
func (s *inventorySource) calleeFrame(parameters *ast.FieldList, call inventoryCall) inventoryFrame {
	names := parameterNames(parameters)
	constants := make(map[string]string, len(names))
	unknown := make(map[string]bool, len(names))
	for index, parameter := range names {
		unknown[parameter] = true
		if index >= len(call.arguments) {
			continue
		}
		if constant, ok := s.constantValue(call.arguments[index], call.frame); ok {
			constants[parameter] = constant
			delete(unknown, parameter)
		}
	}
	return inventoryFrame{constants: constants, unknown: unknown}
}

// calls reports every call the body makes on the receiver (or on a
// *StorageExecutor callback parameter), with the frame its enclosing branch
// conditions prove. A branch whose guard contradicts the constants the caller
// supplied cannot run, so its text interpretation is not this handler's.
func (s *inventorySource) calls(body *ast.BlockStmt, receiver string, frame inventoryFrame) []inventoryCall {
	frame = s.dropWrittenBindings(frame, body)
	var calls []inventoryCall
	receivers := map[string]bool{receiver: true}
	callbacks := map[*ast.FuncLit]bool{}
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		for _, argument := range call.Args {
			literal, ok := argument.(*ast.FuncLit)
			if !ok {
				continue
			}
			callbacks[literal] = true
			for _, field := range literal.Type.Params.List {
				pointer, ok := field.Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				if name, ok := pointer.X.(*ast.Ident); ok && name.Name == "StorageExecutor" {
					for _, name := range field.Names {
						receivers[name.Name] = true
					}
				}
			}
		}
		return true
	})
	collect := func(node ast.Node, proven inventoryFrame) {
		if node == nil {
			return
		}
		ast.Inspect(node, func(node ast.Node) bool {
			if literal, ok := node.(*ast.FuncLit); ok && !callbacks[literal] {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			object, ok := selector.X.(*ast.Ident)
			if !ok || !receivers[object.Name] {
				return true
			}
			calls = append(calls, inventoryCall{name: selector.Sel.Name, arguments: call.Args, frame: proven})
			return true
		})
	}
	var walk func(ast.Node, inventoryFrame)
	walk = func(node ast.Node, proven inventoryFrame) {
		switch statement := node.(type) {
		case nil:
			return
		case *ast.BlockStmt:
			for _, inner := range statement.List {
				walk(inner, proven)
			}
		case *ast.IfStmt:
			walk(statement.Init, proven)
			collect(statement.Cond, proven)
			if refined, reachable := s.refineGuard(proven, statement.Cond, false); reachable {
				walk(statement.Body, refined)
			}
			if refined, reachable := s.refineGuard(proven, statement.Cond, true); reachable {
				walk(statement.Else, refined)
			}
		case *ast.ForStmt:
			walk(statement.Init, proven)
			collect(statement.Cond, proven)
			walk(statement.Post, proven)
			walk(statement.Body, proven)
		case *ast.RangeStmt:
			collect(statement.Key, proven)
			collect(statement.Value, proven)
			walk(statement.Body, proven)
		case *ast.SwitchStmt:
			walk(statement.Init, proven)
			collect(statement.Tag, proven)
			walk(statement.Body, proven)
		case *ast.TypeSwitchStmt:
			walk(statement.Init, proven)
			walk(statement.Assign, proven)
			walk(statement.Body, proven)
		case *ast.SelectStmt:
			walk(statement.Body, proven)
		case *ast.LabeledStmt:
			walk(statement.Stmt, proven)
		case *ast.CaseClause:
			for _, inner := range statement.Body {
				walk(inner, proven)
			}
		case *ast.CommClause:
			walk(statement.Comm, proven)
			for _, inner := range statement.Body {
				walk(inner, proven)
			}
		default:
			collect(statement, proven)
		}
	}
	walk(body, frame)
	return calls
}

// refineGuard reports the parameters a condition proves inside one branch. A
// parameter the caller bound to a constant contradicts a guard naming a
// different constant, and that branch cannot run.
func (s *inventorySource) refineGuard(frame inventoryFrame, condition ast.Expr, negated bool) (inventoryFrame, bool) {
	binary, ok := condition.(*ast.BinaryExpr)
	if !ok || (binary.Op != token.EQL && binary.Op != token.NEQ) {
		return frame, true
	}
	if binary.Op == token.NEQ {
		negated = !negated
	}
	parameter, constant := guardOperands(binary.X, binary.Y)
	if parameter == nil {
		parameter, constant = guardOperands(binary.Y, binary.X)
	}
	if parameter == nil || frame.unknown[parameter.Name] {
		return frame, true
	}
	value, known := s.constantValue(constant, frame)
	if !known {
		return frame, true
	}
	if current, exists := frame.constants[parameter.Name]; exists {
		if (current == value) != negated {
			return frame, true
		}
		return inventoryFrame{bindings: frame.bindings, constants: frame.constants, unknown: frame.unknown}, false
	}
	if negated {
		return frame, true
	}
	constants := maps.Clone(frame.constants)
	if constants == nil {
		constants = map[string]string{}
	}
	constants[parameter.Name] = value
	return inventoryFrame{bindings: frame.bindings, constants: constants, unknown: frame.unknown}, true
}

// guardOperands splits one equality of a parameter and a constant.
func guardOperands(parameter, constant ast.Expr) (*ast.Ident, ast.Expr) {
	identifier, ok := parameter.(*ast.Ident)
	if !ok {
		return nil, nil
	}
	return identifier, constant
}

// parameterNames lists one declaration's parameter names in call order.
func parameterNames(parameters *ast.FieldList) []string {
	var names []string
	if parameters == nil {
		return names
	}
	for _, field := range parameters.List {
		for _, name := range field.Names {
			names = append(names, name.Name)
		}
	}
	return names
}

// firstParameterName is the name a method's receiver is read through.
func firstParameterName(method *ast.FuncDecl) (string, bool) {
	if method.Recv == nil || len(method.Recv.List) == 0 || len(method.Recv.List[0].Names) == 0 {
		return "", false
	}
	return method.Recv.List[0].Names[0].Name, true
}

// reachesText follows the same admitted call graph for both the live registry
// census and its negative fixtures. A shared helper can be reached with more
// than one typed argument binding in the same handler.
func (s *inventorySource) reachesText(call inventoryCall, maxArgs int, rawText map[string]int, visited map[string]bool) bool {
	methods := s.methods

	name := call.name
	if name == "executeUserStatement" || name == "compileNodeWhereFilter" || name == "compileRelationshipWhereFilter" {
		return true
	}
	// Generic engines consume syntax already present in the outer text.
	// Do not walk their downstream fanout as a handler dependency.
	if strings.HasPrefix(name, "evaluate") || strings.HasPrefix(name, "parse") || name == "executeInternal" || name == "executeWithoutTransaction" || name == "executeWithImplicitTransactionCallback" || name == "executeWithImplicitTransaction" || name == "executePeriodicBatch" {
		return false
	}
	if minimum, adapter := rawText[name]; adapter && (maxArgs < 0 || maxArgs >= minimum) {
		return true
	}
	method := methods[name]
	if method == nil {
		return false
	}
	receiver, ok := firstParameterName(method)
	if !ok {
		return false
	}
	frame := s.calleeFrame(method.Type.Params, call)
	key := name
	for _, parameter := range collection.SortedKeys(frame.constants) {
		key += "|" + parameter + "=" + frame.constants[parameter]
	}
	if visited[key] {
		return false
	}
	visited[key] = true
	for _, callee := range s.calls(method.Body, receiver, frame) {
		if s.reachesText(callee, maxArgs, rawText, visited) {
			return true
		}
	}
	return false

}

// dropWrittenBindings refuses constant propagation through a written,
// address-exposed or shadowed name. It deliberately loses precision across the
// whole body rather than treating a pre-write value as a post-write proof.
func (s *inventorySource) dropWrittenBindings(frame inventoryFrame, body ast.Node) inventoryFrame {
	frame.bindings = maps.Clone(frame.bindings)
	frame.constants = maps.Clone(frame.constants)
	frame.unknown = maps.Clone(frame.unknown)
	if frame.unknown == nil {
		frame.unknown = map[string]bool{}
	}
	var forget func(ast.Expr)
	forget = func(expression ast.Expr) {
		switch value := expression.(type) {
		case *ast.Ident:
			delete(frame.bindings, value.Name)
			delete(frame.constants, value.Name)
			frame.unknown[value.Name] = true
		case *ast.SelectorExpr:
			forget(value.X)
		case *ast.IndexExpr:
			forget(value.X)
		case *ast.StarExpr:
			forget(value.X)
		}
	}
	ast.Inspect(body, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.CallExpr:
			if selector, ok := value.Fun.(*ast.SelectorExpr); ok && !s.functionField(selector, frame) {
				forget(selector.X)
			}
		case *ast.AssignStmt:
			for _, left := range value.Lhs {
				forget(left)
			}
		case *ast.IncDecStmt:
			forget(value.X)
		case *ast.RangeStmt:
			forget(value.Key)
			forget(value.Value)
		case *ast.UnaryExpr:
			if value.Op == token.AND {
				forget(value.X)
			}
		case *ast.ValueSpec:
			for _, name := range value.Names {
				forget(name)
			}
		}
		return true
	})
	return frame
}

// functionField distinguishes calling a proven function-valued field from a
// method that may implicitly receive the owner's address. Unknown selectors
// discard a binding instead of assuming a receiver cannot mutate it.
func (s *inventorySource) functionField(selector *ast.SelectorExpr, frame inventoryFrame) bool {
	receiver, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	bound, ok := frame.bindings[receiver.Name]
	if !ok {
		return false
	}
	literal, ok := s.compositeLiteral(bound)
	if !ok {
		return false
	}
	name, ok := literal.Type.(*ast.Ident)
	if !ok {
		return false
	}
	structure, ok := s.types[name.Name].(*ast.StructType)
	if !ok {
		return false
	}
	for _, field := range structure.Fields.List {
		matched := false
		for _, name := range field.Names {
			if name.Name == selector.Sel.Name {
				matched = true
			}
		}
		if !matched {
			continue
		}
		expression := field.Type
		seen := map[string]bool{}
		for {
			if _, ok := expression.(*ast.FuncType); ok {
				return true
			}
			name, ok := expression.(*ast.Ident)
			if !ok || seen[name.Name] {
				return false
			}
			seen[name.Name] = true
			expression = s.types[name.Name]
		}
	}
	return false
}
