package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type relationshipBracketBranch struct {
	from, to      token.Pos
	source, index string
	proven        bool
}

// Byte switches and guarded loops are another producer of bracket interiors.
// Tracking only the named bracket helpers would let an independent reader
// bypass the boundary by spelling its own switch or if statement.
func relationshipManualBracketBranches(body *ast.BlockStmt) []relationshipBracketBranch {
	indices := map[string]*ast.IndexExpr{}
	relationshipContext := false
	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if name, ok := call.Fun.(*ast.Ident); ok {
				switch name.Name {
				case "findListComprehensionToken", "bindingDeclaration", "scanLabelChain":
					relationshipContext = true
				}
			}
		}
		if comparison, ok := n.(*ast.BinaryExpr); ok && comparison.Op == token.EQL {
			for _, pair := range [][2]ast.Expr{{comparison.X, comparison.Y}, {comparison.Y, comparison.X}} {
				index, indexed := pair[0].(*ast.IndexExpr)
				character, quoted := pair[1].(*ast.BasicLit)
				if indexed && quoted && character.Kind == token.CHAR {
					text, _ := strconv.Unquote(character.Value)
					if previous, ok := index.Index.(*ast.BinaryExpr); ok && previous.Op == token.SUB && text == "-" {
						relationshipContext = true
					}
				}
			}
		}
		if value, ok := n.(*ast.BasicLit); ok && value.Kind == token.STRING {
			text, _ := strconv.Unquote(value.Value)
			for _, marker := range []string{"-[", "->", "<-", "]-"} {
				relationshipContext = relationshipContext || strings.Contains(text, marker)
			}
		}
		if assignment, ok := n.(*ast.AssignStmt); ok && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 {
			if name, ok := assignment.Lhs[0].(*ast.Ident); ok {
				if index, ok := assignment.Rhs[0].(*ast.IndexExpr); ok {
					indices[name.Name] = index
				}
			}
		}
		return true
	})
	indexOf := func(e ast.Expr) *ast.IndexExpr {
		if index, ok := e.(*ast.IndexExpr); ok {
			return index
		}
		if name, ok := e.(*ast.Ident); ok {
			return indices[name.Name]
		}
		return nil
	}
	bracket := func(e ast.Expr) bool {
		literal, ok := e.(*ast.BasicLit)
		if !ok || literal.Kind != token.CHAR {
			return false
		}
		value, _ := strconv.Unquote(literal.Value)
		return value == "["
	}
	var branches []relationshipBracketBranch
	add := func(index *ast.IndexExpr, scope ast.Node) {
		if index == nil {
			return
		}
		source, sourceOK := index.X.(*ast.Ident)
		at, indexOK := index.Index.(*ast.Ident)
		if sourceOK && indexOK {
			branches = append(branches, relationshipBracketBranch{scope.Pos(), scope.End(), source.Name, at.Name, relationshipContext})
		}
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch statement := n.(type) {
		case *ast.SwitchStmt:
			for _, item := range statement.Body.List {
				clause := item.(*ast.CaseClause)
				for _, value := range clause.List {
					if bracket(value) {
						add(indexOf(statement.Tag), clause)
					}
				}
			}
		case *ast.IfStmt:
			ast.Inspect(statement.Cond, func(n ast.Node) bool {
				if comparison, ok := n.(*ast.BinaryExpr); ok && (comparison.Op == token.EQL || comparison.Op == token.NEQ) {
					scope := ast.Node(statement.Body)
					if comparison.Op == token.NEQ {
						scope = body
					}
					if bracket(comparison.Y) {
						add(indexOf(comparison.X), scope)
					}
					if bracket(comparison.X) {
						add(indexOf(comparison.Y), scope)
					}
				}
				return true
			})
		}
		return true
	})
	return branches
}

func relationshipBranchOffset(e ast.Expr, branch relationshipBracketBranch) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if offset, ok := n.(*ast.BinaryExpr); ok && offset.Op == token.ADD {
			if name, ok := offset.X.(*ast.Ident); ok && name.Name == branch.index {
				found = true
			}
			if name, ok := offset.Y.(*ast.Ident); ok && name.Name == branch.index {
				found = true
			}
		}
		return true
	})
	return found
}

func relationshipBranchSlice(e ast.Expr, branches []relationshipBracketBranch, provenOnly bool) bool {
	slice, ok := e.(*ast.SliceExpr)
	if !ok || slice.Low == nil {
		return false
	}
	source, ok := slice.X.(*ast.Ident)
	if !ok {
		return false
	}
	for _, branch := range branches {
		if provenOnly && !branch.proven {
			continue
		}
		if e.Pos() >= branch.from && e.End() <= branch.to && source.Name == branch.source && relationshipBranchOffset(slice.Low, branch) {
			return true
		}
	}
	return false
}

// A node-only scanner receives the owner-created nominal masked-text type.
// Follow its single-assignment string aliases without declaring a generic
// list parser to be a relationship reader.
func relationshipMaskedSources(declaration *ast.FuncDecl) map[string]bool {
	safe := map[string]bool{}
	for _, field := range declaration.Type.Params.List {
		if name, ok := field.Type.(*ast.Ident); ok && name.Name == "relationshipNodeText" {
			for _, parameter := range field.Names {
				safe[parameter.Name] = true
			}
		}
	}
	assignments := map[string]int{}
	ast.Inspect(declaration.Body, func(n ast.Node) bool {
		if assignment, ok := n.(*ast.AssignStmt); ok {
			for _, lhs := range assignment.Lhs {
				if name, ok := lhs.(*ast.Ident); ok {
					assignments[name.Name]++
				}
			}
		}
		return true
	})
	for changed := true; changed; {
		changed = false
		ast.Inspect(declaration.Body, func(n ast.Node) bool {
			assignment, ok := n.(*ast.AssignStmt)
			if !ok || len(assignment.Rhs) != 1 || len(assignment.Lhs) == 0 {
				return true
			}
			name, ok := assignment.Lhs[0].(*ast.Ident)
			if !ok || safe[name.Name] || assignments[name.Name] != 1 {
				return true
			}
			valueSafe := false
			switch value := assignment.Rhs[0].(type) {
			case *ast.Ident:
				valueSafe = safe[value.Name]
			case *ast.CallExpr:
				if function, ok := value.Fun.(*ast.Ident); ok {
					valueSafe = function.Name == "relationshipNodeReaderText" || function.Name == "relationshipExpressionReaderText"
					if function.Name == "string" && len(value.Args) == 1 {
						if source, ok := value.Args[0].(*ast.Ident); ok {
							valueSafe = safe[source.Name]
						}
					}
				}
			}
			if valueSafe {
				safe[name.Name] = true
				changed = true
			}
			return true
		})
	}
	return safe
}

// A counter-based prefilter can inspect an interior after leaving the '['
// switch arm. Catch that form as well as readers that slice inside the arm.
func relationshipUnownedWhereReads(body *ast.BlockStmt, branches []relationshipBracketBranch) []token.Pos {
	depths, sources := map[string]bool{}, map[string]bool{}
	for _, branch := range branches {
		sources[branch.source] = true
		ast.Inspect(body, func(n ast.Node) bool {
			if n == nil || n.Pos() < branch.from || n.End() > branch.to {
				return true
			}
			if change, ok := n.(*ast.IncDecStmt); ok && change.Tok == token.INC {
				if name, ok := change.X.(*ast.Ident); ok {
					depths[name.Name] = true
				}
			}
			return true
		})
	}
	var positions []token.Pos
	ast.Inspect(body, func(n ast.Node) bool {
		condition, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		inside := false
		ast.Inspect(condition.Cond, func(n ast.Node) bool {
			if comparison, ok := n.(*ast.BinaryExpr); ok && comparison.Op == token.GTR {
				if name, ok := comparison.X.(*ast.Ident); ok && depths[name.Name] {
					inside = true
				}
			}
			return true
		})
		if !inside {
			return true
		}
		ast.Inspect(condition, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) < 2 {
				return true
			}
			last, ok := call.Args[len(call.Args)-1].(*ast.BasicLit)
			if !ok || last.Kind != token.STRING {
				return true
			}
			word, _ := strconv.Unquote(last.Value)
			if !strings.EqualFold(word, "WHERE") {
				return true
			}
			fromSource := false
			ast.Inspect(call.Args[0], func(n ast.Node) bool {
				if name, ok := n.(*ast.Ident); ok && sources[name.Name] {
					fromSource = true
				}
				return true
			})
			if fromSource {
				positions = append(positions, call.Pos())
			}
			return true
		})
		return true
	})
	return positions
}

func relationshipUnownedArrowReads(body *ast.BlockStmt) []token.Pos {
	arrowContext, bracketContext := false, false
	ast.Inspect(body, func(n ast.Node) bool {
		if value, ok := n.(*ast.BasicLit); ok && value.Kind == token.STRING {
			text, _ := strconv.Unquote(value.Value)
			arrowContext = arrowContext || strings.Contains(text, "->") || strings.Contains(text, "<-")
		}
		if call, ok := n.(*ast.CallExpr); ok {
			if name, ok := call.Fun.(*ast.Ident); ok {
				switch name.Name {
				case "nextRelationshipBracket", "firstRelationshipBracket", "nextRelationshipBracketInExpressions":
					bracketContext = true
				}
			}
		}
		return true
	})
	var positions []token.Pos
	ast.Inspect(body, func(n ast.Node) bool {
		if comparison, ok := n.(*ast.BinaryExpr); ok && (comparison.Op == token.EQL || comparison.Op == token.NEQ) {
			for _, pair := range [][2]ast.Expr{{comparison.X, comparison.Y}, {comparison.Y, comparison.X}} {
				index, indexed := pair[0].(*ast.IndexExpr)
				character, quoted := pair[1].(*ast.BasicLit)
				if !indexed || !quoted || character.Kind != token.CHAR {
					continue
				}
				text, _ := strconv.Unquote(character.Value)
				offset, preceding := index.Index.(*ast.BinaryExpr)
				if arrowContext && preceding && offset.Op == token.SUB && text == "]" || bracketContext && (text == "-" || text == ">" || text == "<") {
					positions = append(positions, comparison.Pos())
				}
			}
		}
		return true
	})
	return positions
}

// This is a source boundary, not a list of approved consumers. Bracket
// interiors are tainted through assignments and local calls. Only the two
// declaration entry points consume them; scanners and regexes cannot become
// another reader. Parsed declaration fields are intentionally not raw text.
func relationshipOwnerViolations(sources map[string]string, trace ...func(string, ...interface{})) []string {
	type function struct {
		file             string
		decl             *ast.FuncDecl
		params           []string
		raw              map[string]bool
		intrinsic        map[string]bool
		bounds           map[string]bool
		returnsRaw       bool
		returnsIntrinsic bool
		branches         []relationshipBracketBranch
	}
	fset := token.NewFileSet()
	functions := map[string][]*function{}
	var violations []string
	origins := map[string]bool{}
	var files []*ast.File
	constants := map[string]ast.Expr{}
	for name, source := range sources {
		f, err := parser.ParseFile(fset, name, source, 0)
		if err != nil {
			violations = append(violations, err.Error())
			continue
		}
		files = append(files, f)
		for _, decl := range f.Decls {
			if group, ok := decl.(*ast.GenDecl); ok {
				for _, spec := range group.Specs {
					if value, ok := spec.(*ast.ValueSpec); ok {
						for i, n := range value.Names {
							if i < len(value.Values) {
								constants[n.Name] = value.Values[i]
							}
						}
					}
				}
			}
			if d, ok := decl.(*ast.FuncDecl); ok && d.Body != nil {
				fn := &function{file: name, decl: d, raw: map[string]bool{}, intrinsic: map[string]bool{}, bounds: map[string]bool{}}
				if name != "relationship_declaration.go" {
					masked := relationshipMaskedSources(d)
					for _, branch := range relationshipManualBracketBranches(d.Body) {
						if !masked[branch.source] {
							fn.branches = append(fn.branches, branch)
						}
					}
				}
				for _, field := range d.Type.Params.List {
					for _, n := range field.Names {
						fn.params = append(fn.params, n.Name)
					}
				}
				functions[d.Name.Name] = append(functions[d.Name.Name], fn)
			}
		}
	}
	nameOf := func(e ast.Expr) string {
		switch x := e.(type) {
		case *ast.Ident:
			return x.Name
		case *ast.SelectorExpr:
			return x.Sel.Name
		}
		return ""
	}
	var literal func(ast.Expr) string
	literal = func(e ast.Expr) string {
		switch x := e.(type) {
		case *ast.Ident:
			if value, ok := constants[x.Name]; ok {
				return literal(value)
			}
		case *ast.BasicLit:
			if x.Kind == token.STRING {
				s, _ := strconv.Unquote(x.Value)
				return s
			}
		case *ast.BinaryExpr:
			if x.Op == token.ADD {
				return literal(x.X) + literal(x.Y)
			}
		}
		return ""
	}
	// A relationship regex is an independent grammar even if its result never
	// flows through a bracket helper (the old degree and lookup shortcuts).
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || (nameOf(call.Fun) != "MustCompile" && nameOf(call.Fun) != "Compile") || len(call.Args) == 0 {
				return true
			}
			pattern := literal(call.Args[0])
			if strings.Contains(pattern, `\[`) && (strings.Contains(pattern, "-") || strings.Contains(pattern, ":") || strings.Contains(pattern, `\*`)) {
				violations = append(violations, fset.Position(call.Pos()).String()+": independent relationship regex")
			}
			return true
		})
	}
	// Decoded-name ownership is checked with compiler identities in
	// TestRelationshipOwnerTypedProvenanceBoundary; this pass tracks raw text.
	changed := true
	for changed {
		changed = false
		for _, group := range functions {
			for _, fn := range group {
				var rawIn func(ast.Expr, bool) bool
				raw := func(e ast.Expr) bool { return rawIn(e, false) }
				intrinsic := func(e ast.Expr) bool { return rawIn(e, true) }
				rawIn = func(e ast.Expr, sourceOnly bool) bool {
					switch x := e.(type) {
					case *ast.Ident:
						if sourceOnly {
							return fn.intrinsic[x.Name]
						}
						return fn.raw[x.Name]
					case *ast.ParenExpr:
						return rawIn(x.X, sourceOnly)
					case *ast.BinaryExpr:
						return rawIn(x.X, sourceOnly) || rawIn(x.Y, sourceOnly)
					case *ast.SliceExpr:
						if relationshipBranchSlice(x, fn.branches, true) {
							origins[fset.Position(x.Pos()).String()+" "+fn.decl.Name.Name] = true
							return true
						}
						if rawIn(x.X, sourceOnly) {
							return true
						}
						if lo, ok := x.Low.(*ast.BinaryExpr); ok && lo.Op == token.ADD {
							if id, ok := lo.X.(*ast.Ident); ok && fn.bounds[id.Name] {
								origins[fset.Position(x.Pos()).String()+" "+fn.decl.Name.Name] = true
								return true
							}
						}
					case *ast.CallExpr:
						if nameOf(x.Fun) == "relationshipDeclarationOf" || nameOf(x.Fun) == "relationshipDeclarationHead" {
							return false
						}
						if nameOf(x.Fun) == "TrimSpace" && len(x.Args) > 0 {
							return rawIn(x.Args[0], sourceOnly)
						}
						for _, to := range functions[nameOf(x.Fun)] {
							if to.returnsIntrinsic {
								return true
							}
							if to.returnsRaw {
								for _, argument := range x.Args {
									if rawIn(argument, sourceOnly) {
										return true
									}
								}
							}
						}
					}
					return false
				}
				mark := func(table map[string]bool, name string) {
					if name != "_" && !table[name] {
						table[name] = true
						changed = true
					}
				}
				ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
					switch x := n.(type) {
					case *ast.ValueSpec:
						if len(x.Values) == 1 && len(x.Names) > 0 {
							if call, ok := x.Values[0].(*ast.CallExpr); ok {
								switch nameOf(call.Fun) {
								case "nextRelationshipBracket", "firstRelationshipBracket", "scanRelationshipBracket", "nextRelationshipBracketInExpressions", "scanRelationshipBracketScope":
									mark(fn.bounds, x.Names[0].Name)
								}
							}
						}
						for i, value := range x.Values {
							if i < len(x.Names) && intrinsic(value) {
								mark(fn.intrinsic, x.Names[i].Name)
							}
							if i < len(x.Names) && raw(value) {
								mark(fn.raw, x.Names[i].Name)
							}
						}
					case *ast.AssignStmt:
						if len(x.Rhs) == 1 && len(x.Lhs) > 0 {
							if c, ok := x.Rhs[0].(*ast.CallExpr); ok {
								bracket := false
								switch nameOf(c.Fun) {
								case "nextRelationshipBracket", "firstRelationshipBracket", "scanRelationshipBracket", "nextRelationshipBracketInExpressions", "scanRelationshipBracketScope":
									bracket = true
								case "Index", "IndexByte":
									for _, arg := range c.Args {
										bracket = bracket || strings.Contains(literal(arg), "-[")
									}
								}
								if bracket {
									if id, ok := x.Lhs[0].(*ast.Ident); ok {
										mark(fn.bounds, id.Name)
									}
								}
							}
						}
						for i, rhs := range x.Rhs {
							if i < len(x.Lhs) && intrinsic(rhs) {
								if id, ok := x.Lhs[i].(*ast.Ident); ok {
									mark(fn.intrinsic, id.Name)
								}
							}
							if i < len(x.Lhs) && raw(rhs) {
								if id, ok := x.Lhs[i].(*ast.Ident); ok {
									mark(fn.raw, id.Name)
								}
							}
						}
					case *ast.CallExpr:
						callee := nameOf(x.Fun)
						if callee == "relationshipDeclarationOf" || callee == "relationshipDeclarationHead" {
							return true
						}
						for i, arg := range x.Args {
							if raw(arg) {
								for _, to := range functions[callee] {
									if i < len(to.params) {
										mark(to.raw, to.params[i])
									}
								}
							}
						}
					case *ast.ReturnStmt:
						for _, result := range x.Results {
							if intrinsic(result) && !fn.returnsIntrinsic {
								fn.returnsIntrinsic = true
								changed = true
							}
							if raw(result) && !fn.returnsRaw {
								fn.returnsRaw = true
								changed = true
							}
						}
					}
					return true
				})
			}
		}
	}
	for _, group := range functions {
		for _, fn := range group {
			if fn.file != "relationship_declaration.go" {
				for _, position := range relationshipUnownedArrowReads(fn.decl.Body) {
					violations = append(violations, fset.Position(position).String()+": relationship arrow admission must consume owner frames")
				}
				for _, position := range relationshipUnownedWhereReads(fn.decl.Body, fn.branches) {
					violations = append(violations, fset.Position(position).String()+": relationship WHERE prefilter must consume owner facts or masked text")
				}
				if fn.decl.Name.Name == "eachPatternElement" || fn.decl.Name.Name == "scanQuotedTokens" || fn.decl.Name.Name == "classifyQuotedToken" {
					violations = append(violations, "only the declaration owner may implement semantic element iteration")
				}
				if fn.decl.Type.Params != nil {
					ast.Inspect(fn.decl.Type.Params, func(n ast.Node) bool {
						if pointer, ok := n.(*ast.StarExpr); ok && nameOf(pointer.X) == "quotedToken" {
							violations = append(violations, fset.Position(pointer.Pos()).String()+": quoted-token role classification belongs to the declaration owner")
						}
						return true
					})
				}
				ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
					if value, ok := n.(*ast.ValueSpec); ok && nameOf(value.Type) == "quotedToken" {
						violations = append(violations, fset.Position(value.Pos()).String()+": only the declaration owner may create quoted-token roles")
					}
					if call, ok := n.(*ast.CallExpr); ok && nameOf(call.Fun) == "scanLabelChain" && len(call.Args) > 1 {
						if mode, ok := call.Args[1].(*ast.Ident); !ok || mode.Name != "false" {
							violations = append(violations, fset.Position(call.Pos()).String()+": relationship type grammar is available only through the declaration owner")
						}
					}
					if assignment, ok := n.(*ast.AssignStmt); ok {
						for _, left := range assignment.Lhs {
							if field, ok := left.(*ast.SelectorExpr); ok && (field.Sel.Name == "binding" || field.Sel.Name == "procColumn" || field.Sel.Name == "relationshipBinding") {
								violations = append(violations, fset.Position(field.Pos()).String()+": quoted-token binding roles are read-only outside the declaration owner")
							}
						}
					}
					if value, ok := n.(*ast.CompositeLit); ok && (nameOf(value.Type) == "patternElement" || nameOf(value.Type) == "traversalPart" || nameOf(value.Type) == "quotedToken" || nameOf(value.Type) == "relationshipFrame" || nameOf(value.Type) == "relationshipHopFrame" || nameOf(value.Type) == "relationshipHop" || nameOf(value.Type) == "traversalRelationship" || nameOf(value.Type) == "cachedRelationshipDeclaration" || nameOf(value.Type) == "relationshipDeclarationEntry") {
						violations = append(violations, fset.Position(value.Pos()).String()+": only the declaration owner may construct an element")
					}
					if value, ok := n.(*ast.CompositeLit); ok && (nameOf(value.Type) == "relationshipDeclaration" || nameOf(value.Type) == "relationshipDeclarationCache") && len(value.Elts) != 0 {
						violations = append(violations, fset.Position(value.Pos()).String()+": only the declaration owner may populate a declaration")
					}
					if call, ok := n.(*ast.CallExpr); ok && (nameOf(call.Fun) == "relationshipNodeText" || nameOf(call.Fun) == "expressionListText") {
						violations = append(violations, fset.Position(call.Pos()).String()+": only the declaration owner may construct relationship-free prefilter text")
					}
					if name, ok := n.(*ast.Ident); ok && name.Name == "relationshipDeclarationFacts" {
						violations = append(violations, fset.Position(name.Pos()).String()+": only the declaration owner may populate its immutable head cache")
					}
					if field, ok := n.(*ast.SelectorExpr); ok && (field.Sel.Name == "typeText" || field.Sel.Name == "rangeText" || field.Sel.Name == "detailOpen" || field.Sel.Name == "detailClose" || field.Sel.Name == "sourceInterior" || field.Sel.Name == "declarationOverflow" || field.Sel.Name == "declarationFirst" || field.Sel.Name == "declarationPresent" || field.Sel.Name == "declarationRest" || field.Sel.Name == "repetitionKnown" || field.Sel.Name == "repetitionPossible") {
						violations = append(violations, fset.Position(field.Pos()).String()+": relationship type/range text is private to the owner; consume its parsed facts")
					}
					return true
				})
			}
			// Reuse the final fixed point to recognize raw identifiers inside a
			// scanner call or byte loop, including in a newly introduced helper.
			var isRaw func(ast.Expr) bool
			isRaw = func(e ast.Expr) bool {
				found := false
				ast.Inspect(e, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						if nameOf(call.Fun) == "relationshipDeclarationOf" || nameOf(call.Fun) == "relationshipDeclarationHead" {
							return false
						}
						for _, to := range functions[nameOf(call.Fun)] {
							if to.returnsIntrinsic {
								found = true
							}
							if to.returnsRaw {
								for _, arg := range call.Args {
									found = found || isRaw(arg)
								}
							}
						}
					}
					if id, ok := n.(*ast.Ident); ok && fn.raw[id.Name] {
						found = true
					}
					if s, ok := n.(*ast.SliceExpr); ok {
						if relationshipBranchSlice(s, fn.branches, true) {
							found = true
						}
						if lo, ok := s.Low.(*ast.BinaryExpr); ok && lo.Op == token.ADD {
							if id, ok := lo.X.(*ast.Ident); ok && fn.bounds[id.Name] {
								found = true
							}
						}
					}
					return true
				})
				return found
			}
			ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
				bad := false
				if fn.decl.Name.Name == "relationshipNodeReaderText" {
					if call, ok := n.(*ast.CallExpr); ok && (nameOf(call.Fun) == "nextRelationshipBracket" || nameOf(call.Fun) == "scanRelationshipBracket") {
						violations = append(violations, fset.Position(call.Pos()).String()+": masked-text owner must include expression/subquery scopes")
					}
				}
				switch x := n.(type) {
				case *ast.IndexExpr:
					bad = isRaw(x.X)
					for _, branch := range fn.branches {
						if !branch.proven {
							continue
						}
						if x.Pos() >= branch.from && x.End() <= branch.to {
							if source, ok := x.X.(*ast.Ident); ok && source.Name == branch.source && relationshipBranchOffset(x.Index, branch) {
								bad = true
							}
						}
					}
				case *ast.RangeStmt:
					bad = isRaw(x.X)
				case *ast.CallExpr:
					callee := nameOf(x.Fun)
					if callee == "relationshipDeclarationOf" || callee == "relationshipDeclarationHead" {
						return false
					}
					if callee == "eachPatternElement" && len(x.Args) >= 2 {
						if callback, ok := x.Args[1].(*ast.FuncLit); ok {
							fields := callback.Type.Params.List
							if len(fields) != 1 || nameOf(fields[0].Type) != "patternElement" {
								bad = true
							}
						}
					}
					if callee == "eachExpressionList" && len(x.Args) >= 2 {
						if callback, ok := x.Args[1].(*ast.FuncLit); ok {
							fields := callback.Type.Params.List
							if len(fields) != 1 || nameOf(fields[0].Type) != "expressionListText" {
								bad = true
							}
						}
					}
					for _, branch := range fn.branches {
						if x.Pos() < branch.from || x.End() > branch.to || len(x.Args) < 2 {
							continue
						}
						if source, ok := x.Args[0].(*ast.Ident); ok && source.Name == branch.source {
							switch callee {
							case "scanSymbolicName", "scanIdentifierToken", "parseIdentifierToken", "scanLabelChain":
								for _, argument := range x.Args[1:] {
									bad = bad || relationshipBranchOffset(argument, branch)
								}
							}
						}
					}
					// Mixed node/list branches are not by themselves proof that
					// a slice is a relationship. Reject their direct header
					// decoders, rather than tainting every generic list parser.
					for _, argument := range x.Args {
						if !relationshipBranchSlice(argument, fn.branches, false) {
							continue
						}
						semantic := callee == "scanSymbolicName" || callee == "scanIdentifierToken" || callee == "parseIdentifierToken" || callee == "scanLabelChain"
						for _, delimiter := range x.Args {
							if value, ok := delimiter.(*ast.BasicLit); ok && (value.Kind == token.STRING || value.Kind == token.CHAR) {
								text, _ := strconv.Unquote(value.Value)
								semantic = semantic || text == "WHERE" || text == ":" || text == "|" || text == "*" || text == "{" || text == "}"
							}
						}
						bad = bad || semantic
					}
					if bad {
						break
					}
					if _, local := functions[callee]; local {
						return true
					}
					if callee == "TrimSpace" || callee == "len" {
						return true
					}
					if selector, ok := x.Fun.(*ast.SelectorExpr); ok {
						if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "fmt" {
							for _, arg := range x.Args {
								bad = bad || isRaw(arg)
							}
						}
					}
				}
				if bad {
					violations = append(violations, fset.Position(n.Pos()).String()+": raw relationship interior scanned outside declaration owner")
				}
				return true
			})
		}
	}
	if len(violations) > 0 && len(trace) > 0 {
		for origin := range origins {
			trace[0]("RELATIONSHIP_ORIGIN %s", origin)
		}
	}
	return violations
}

func TestRelationshipDeclarationOwnerBoundary(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string]string{}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") && !strings.HasSuffix(entry.Name(), "_test.go") {
			data, err := os.ReadFile(filepath.Clean(entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			sources[entry.Name()] = string(data)
		}
	}
	for _, failure := range relationshipOwnerViolations(sources, t.Logf) {
		t.Error(failure)
	}
}

func TestRelationshipDeclarationOwnerGuardMutations(t *testing.T) {
	for _, body := range []string{
		`func bad(q string){scanLabelChain(q,true)}`,
		`func bad(q string,relationship bool){scanLabelChain(q,relationship)}`,
		`func bad(c cachedRelationshipDeclaration){strings.Split(c.sourceInterior,"|")}`,
		`func bad(c relationshipDeclarationCache){for q:=range c.declarationOverflow{strings.Split(q,"|")}}`,
		`func bad(c relationshipDeclarationCache){c.repetitionKnown=true}`,
		`func bad(){relationshipDeclarationFacts.put("x",relationshipDeclarationEntry{})}`,
		`func bad(q string){consume(cachedRelationshipDeclaration{sourceInterior:q})}`,
		`func bad(q string){for i:=range q{if q[i]!='['{continue};c:=findMatchingBracket(q,i);findListComprehensionToken(q[i+1:c],"|")}}`,
		`func bad(q string){eachExpressionList(q,func(inner string)error{return nil})}`,
		`func bad(q string){consume(expressionListText(q))}`,
		`func bad(q string,t *quotedToken){prev,_:=previousSignificant(q,t.start);if prev=='['{t.binding=true}}`,
		`func bad(){consume(quotedToken{binding:true})}`,
		`func bad(){var token quotedToken;consume(token)}`,
		`func bad(token quotedToken,q string){token.binding=strings.Contains(q,"-[")}`,
		`func bad(token quotedToken,q string){token.relationshipBinding=strings.Contains(q,"-[")}`,
		`func bad(){consume(traversalPart{})}`,
		`func bad(q string){f,_:=readRelationshipFrame(q,0,nil);strings.Split(q[f.detailOpen+1:f.detailClose],"|")}`,
		`func bad(q string){_,close:=nextRelationshipBracket(q,0);end:=close+1;for end<len(q)&&(q[end]=='-'||q[end]=='>'){end++}}`,
		`func bad(q string){for i:=range q{switch c:=q[i];c{case '(','[':scanSymbolicName(q,skipASCIISpaces(q,i+1,len(q)))}}}`,
		`func bad(q string){if !strings.Contains(q,"->"){return};for i:=1;i<len(q);i++{if q[i-1]==']'{return}}}`,
		`func bad(q string){depth:=0;for i:=range q{switch q[i]{case '(','[':depth++;case ')',']':depth--;case 'W':if depth>0&&matchKeywordAt(q,i,"WHERE"){return}}}}`,
		`func bad(q string){for i:=range q{if strings.HasPrefix(q,"-[")&&q[i]=='['{strings.Split(q[i+1:],"|")}}}`,
		`func extract(q string)string{o,c:=nextRelationshipBracket(q,0);return q[o+1:c]};func bad(q string){strings.Split(extract(q),"|")}`,
		`func bad(q string){for i:=range q{if i>0&&q[i-1]=='-'&&q[i]=='['{scanSymbolicName(q,i+1)}}}`,
		`func bad(q string){if !strings.HasPrefix(q,"-["){return};for i:=range q{if q[i]!='['{continue};strings.Split(q[i+1:],"|")}}`,
		`func bad(q string){if !strings.HasPrefix(q,"-["){return};for i:=range q{switch q[i]{case '[':if q[i+1]==':'{return}}}}`,
		`func bad(q string){eachPatternElement(q,func(opener byte,element string){strings.Split(element[1:len(element)-1],"|")})}`,
		`func bad(){consume(patternElement{})}`,
		`func bad(q string){consume(relationshipDeclaration{typeText:q})}`,
		`func relationshipNodeReaderText(q string){nextRelationshipBracket(q,0)}`,
		`func bad(q string){o,c:=nextRelationshipBracketInExpressions(q,0);inside:=q[o+1:c];strings.Split(inside,"|")}`,
		`func bad(q string){o,c:=nextRelationshipBracket(q,0);var inside=q[o+1:c];strings.Split(inside,"|")}`,
		"const prefix=`-\\[`;var shortcut=regexp.MustCompile(prefix+`([^:]*):([^]]*)\\]`)",
		`func bad(s string){mayUseNodeLabelExpressions(relationshipNodeText(s))}`,
		`func bad(d relationshipDeclaration){strings.Split(d.typeText,"|")}`,
		`func bad(q string){o,c:=nextRelationshipBracket(q,0); inside:=q[o+1:c]; strings.Split(inside,"|")}`,
		`func bad(q string){o,c:=nextRelationshipBracket(q,0); helper(q[o+1:c])};func helper(value string){for i:=0;i<len(value);i++{if value[i]==':'{return}}}`,
		"var shortcut=regexp.MustCompile(`-\\[\\s*(.*)\\]->`)",
		"var shortcut=regexp.MustCompile(`-\\[([^:]*):([^]]*)\\]`)",
	} {
		source := "package cypher; import(\"strings\";\"regexp\");" + body
		if len(relationshipOwnerViolations(map[string]string{"new_reader.go": source})) == 0 {
			t.Errorf("missed independent reader: %s", body)
		}
	}
	for _, good := range []string{
		`package cypher;func node(q string){scanLabelChain(q,false)}`,
		`package cypher;func good(q string){o,c:=nextRelationshipBracket(q,0);d:=relationshipDeclarationOf(q[o+1:c]);consume(d.types)}`,
		`package cypher;func list(q string){if len(q)>0&&q[0]=='['{strings.Split(q[1:len(q)-1],",")}}`,
		`package cypher;func list(q string){for i:=range q{if q[i]!='['||relationshipBracketAt(q,0,i){continue};strings.Split(q[i+1:],",")}}`,
		`package cypher;func good(q string){eachPatternElement(q,func(element patternElement){consume(element.relationship.types)})}`,
		`package cypher;func node(text relationshipNodeText){q:=string(text);for i:=range q{switch q[i]{case '(','[':scanSymbolicName(q,i+1)}}}`,
	} {
		if failures := relationshipOwnerViolations(map[string]string{"consumer.go": good}); len(failures) != 0 {
			t.Fatal(failures)
		}
	}
}
