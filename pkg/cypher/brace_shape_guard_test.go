package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Extract shapes, never a census of approved Cypher functions. A new private
// reader fails without anyone first having to know its name. The scoped guard
// below retains reviewed lexical adapters by exact body digest; punctuation
// handling alone is not a second brace-role or map-entry grammar.
func independentBraceReaders(file *ast.File) []string {
	imports := map[string]bool{}
	for _, item := range file.Imports {
		path, _ := strconv.Unquote(item.Path.Value)
		if path == "strings" || path == "bytes" {
			name := path
			if item.Name != nil {
				name = item.Name.Name
			}
			imports[name] = true
		}
	}
	constants := map[string]string{}
	ast.Inspect(file, func(node ast.Node) bool {
		if values, ok := node.(*ast.ValueSpec); ok {
			for index, value := range values.Values {
				if literal, ok := value.(*ast.BasicLit); ok && index < len(values.Names) {
					constants[values.Names[index].Name], _ = strconv.Unquote(literal.Value)
				}
			}
		}
		return true
	})
	var brace func(ast.Node) bool
	brace = func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.ParenExpr:
			return brace(value.X)
		case *ast.CallExpr:
			if name, ok := value.Fun.(*ast.Ident); ok && len(value.Args) == 1 {
				switch name.Name {
				case "byte", "rune", "uint8", "int32", "int":
					return brace(value.Args[0])
				}
			}
		}
		if name, ok := node.(*ast.Ident); ok {
			return constants[name.Name] == "{" || constants[name.Name] == "}"
		}
		literal, ok := node.(*ast.BasicLit)
		if !ok {
			return false
		}
		value, err := strconv.Unquote(literal.Value)
		return err == nil && (value == "{" || value == "}")
	}
	type functionBody struct {
		name string
		body *ast.BlockStmt
	}
	var functions []functionBody
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			// A package variable can own a function literal too. Nested
			// literals are already included in their enclosing function.
			ast.Inspect(declaration, func(node ast.Node) bool {
				if values, ok := node.(*ast.ValueSpec); ok {
					for index, value := range values.Values {
						if index >= len(values.Names) {
							continue
						}
						ast.Inspect(value, func(node ast.Node) bool {
							if literal, ok := node.(*ast.FuncLit); ok {
								functions = append(functions, functionBody{values.Names[index].Name + "$literal", literal.Body})
								return false
							}
							return true
						})
					}
				}
				return true
			})
			continue
		}
		if function.Body == nil {
			continue
		}
		name := function.Name.Name
		if function.Recv != nil {
			var receiver strings.Builder
			ast.Inspect(function.Recv.List[0].Type, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					receiver.WriteString(id.Name)
				}
				return true
			})
			name = receiver.String() + "." + name
		}
		functions = append(functions, functionBody{name, function.Body})
	}
	var hits []string
	for _, function := range functions {
		name := function.name
		braceContext := false
		ast.Inspect(function.body, func(n ast.Node) bool {
			if brace(n) {
				braceContext = true
			}
			if id, ok := n.(*ast.Ident); ok && strings.Contains(strings.ToLower(id.Name), "brace") {
				braceContext = true
			}
			return true
		})
		found := false
		privateDepth := func(condition ast.Node, body ast.Node) bool {
			braceEvent := false
			ast.Inspect(condition, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
						braceEvent = braceEvent || selector.Sel.Name == "IsOpenBrace" || selector.Sel.Name == "IsCloseBrace"
					}
				}
				return true
			})
			if !braceEvent {
				return false
			}
			mutation := false
			ast.Inspect(body, func(node ast.Node) bool {
				if increment, ok := node.(*ast.IncDecStmt); ok {
					if name, ok := increment.X.(*ast.Ident); ok {
						lower := strings.ToLower(name.Name)
						mutation = mutation || strings.Contains(lower, "depth") || strings.Contains(lower, "nest") || strings.Contains(lower, "count")
					}
				}
				return true
			})
			return mutation
		}
		ast.Inspect(function.body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.BinaryExpr:
				switch n.Op {
				case token.EQL, token.NEQ, token.LSS, token.GTR, token.LEQ, token.GEQ:
					found = found || brace(n.X) || brace(n.Y)
				}
			case *ast.CaseClause:
				for _, value := range n.List {
					found = found || brace(value)
					found = found || privateDepth(value, &ast.BlockStmt{List: n.Body})
				}
			case *ast.IfStmt:
				found = found || privateDepth(n.Cond, n.Body)
			case *ast.CallExpr:
				selector, ok := n.Fun.(*ast.SelectorExpr)
				if !ok {
					break
				}
				pkg, ok := selector.X.(*ast.Ident)
				if !ok || !imports[pkg.Name] {
					break
				}
				switch selector.Sel.Name {
				case "Index", "IndexByte", "IndexRune", "IndexAny", "LastIndex", "LastIndexByte", "Contains", "ContainsRune", "ContainsAny", "Split", "SplitN", "Cut", "HasPrefix", "HasSuffix":
					for index, arg := range n.Args {
						ast.Inspect(arg, func(node ast.Node) bool {
							if literal, ok := node.(*ast.BasicLit); ok {
								value, err := strconv.Unquote(literal.Value)
								if err == nil && (strings.ContainsAny(value, "{}") || index > 0 && braceContext && value == ",") {
									found = true
								}
							}
							if brace(node) {
								found = true
							}
							return true
						})
					}
				}
			case *ast.IncDecStmt:
				if id, ok := n.X.(*ast.Ident); ok && strings.Contains(strings.ToLower(id.Name), "brace") {
					found = true
				}
			case *ast.AssignStmt:
				if n.Tok == token.ADD_ASSIGN || n.Tok == token.SUB_ASSIGN {
					for _, lhs := range n.Lhs {
						if id, ok := lhs.(*ast.Ident); ok && strings.Contains(strings.ToLower(id.Name), "brace") {
							found = true
						}
					}
				}
			}
			return true
		})
		if found {
			hits = append(hits, name)
		}
	}
	return hits
}

func TestBraceReaderShapeGuard(t *testing.T) {
	// The pattern context and role readers moved from cypher/map_pattern_context.go
	// and cypher/brace_syntax.go into the antlr owner; brace_view.go holds the
	// view's accessors over that product.
	owners := map[string]bool{"cypher/brace_syntax.go": true, "cypher/brace_text.go": true, "cypher/brace_view.go": true,
		"cypher/antlr/brace_pattern_context.go": true, "cypher/antlr/brace_roles.go": true}
	// These parse other languages. Lexical Cypher dispositions are separate
	// below, with exact body digests; no grammar decision gets an exclusion.
	exceptions := map[string]string{
		"cypher/fulltext_query.go:ftTokenize":                  "Lucene fulltext query grammar",
		"cypher/functions_cypher25.go:javaReplacementTemplate": "Java regex replacement template grammar",
		"cypher/temporal_pattern.go:compileTemporalPattern":    "Java temporal formatting pattern grammar",
	}
	// The lexical foundation and this layer have separate responsibilities.
	// These reviewed readers locate lexical/contextual boundaries, while the
	// brace owner admits roles and entries. Seal each exact body so a later
	// grammar change cannot silently inherit the lexical disposition.
	lexicalAdapters := map[string]struct{ reason, sha256 string }{
		"cypher/ast_builder.go:ASTBuilder.patternFields":                                    {"Pattern name/label and property-span extraction; the property brace extent comes from braceView.close.", "46842404deb3286afd45350ba9844c79002155f308de87cd8965139c87b29d98"},
		"cypher/brace_record_consumers.go:braceView.patternProperty":                        {"Pattern property map located through ProjectBrace and braceView.close; the byte compare only confirms the projected opening.", "e5026d99315cbcb2f48b793b929fa7209b2ac0cd1bd19be7946843ab7872f2dd"},
		"cypher/call.go:parseCallTailDelimited":                                             {"CALL tail delimiter dispatch; a brace extent comes from braceView.close.", "c125a0ccf0ee8f7e7c7b84ab8e5013fbbfa5f748ee9668653d5effdf2e00f693"},
		"cypher/call_scope_reader.go:malformedCallScopeError":                               {"CALL scope diagnostic; the body extent comes from the shared delimiter matcher.", "61582e8361d92099baa15387681a20869acee0ac1d2c290d02e45a049e90c965"},
		"cypher/case_expression.go:caseOperandEndsAt":                                       {"CASE operand boundary punctuation; no brace role or entry verdict.", "f76d24846357d76e1cdc9517679b24f8a139bb7f581a488ee3b9f650c4147562"},
		"cypher/cypher25_expression_rewrite.go:startsProjectedExpression":                   {"Operand-start punctuation class; no brace role or entry verdict.", "9505b1617e64e238f66182ed181cb86765fef8c5d5cb8d8b9291bedc887f8978"},
		"cypher/executor.go:firstTopLevelCallSubquery":                                      {"Comment-aware lexical nesting; CALL admission delegates to callSubqueryAt.", "0bcc9752755e66c217b81603bfb1011a753cb6318bfd381c3a0d34c1c2b5272f"},
		"cypher/executor_mutations.go:StorageExecutor.tryExecuteDeleteWithWithLimitHotPath": {"DELETE target-list screen (more than one target leaves the hot path); no brace role or entry verdict.", "19c4d8ce6622cd974f9d0f35260795f6dfb88e597a81ce9f6b58ee5399070058"},
		"cypher/executor_show.go:StorageExecutor.executeAlterDatabase":                      {"ALTER DATABASE SET LIMIT scalar assignment list; no map or brace grammar.", "5147f3b6809761bee426a10cffa255ec4fd16fca1cf702b74012290593c771e0"},
		"cypher/expression_validation.go:staticListOperand":                                 {"List operand extent; a map operand's extent comes from braceView.close.", "7171228d15f4c8c81d6efaf3154a0bab671c52e5e387501c6548efa20f537cb3"},
		"cypher/expression_validation.go:wholeListOperand":                                  {"List operand termination punctuation; static typing remains in its own owner.", "7cfd73f7f7efef3b57178a4e56f306181f0bc2bfa4aeccb57e537ab94f2618f5"},
		"cypher/group_by_rewrite.go:projectionWords":                                        {"Projection word extents and nesting; no brace role or entry verdict.", "6d74e85bb6e5e2075ed5bae3ca7359ddf01a3352448fb81bf25257ff4ac4e75e"},
		"cypher/keyword_scan.go:queryMayNeedCanonicalRewrite":                               {"Canonical-spacing screen: a closing delimiter directly before AS; no brace role.", "5683d5bc61716d602f22e7fe81f75d9c649cf383cffcf47f45949bc41285ceea"},
		"cypher/keyword_scan.go:readCanonicalQueryText":                                     {"Canonical spacing: a closing delimiter directly before AS gets its space; no brace role.", "dc8dd1ffabe739508471474523dffb41f288d203f859d195b0df3fefadcf9b32"},
		"cypher/label_expression_rewrite.go:labelExpressionRewriter.expression":             {"Rewrite walk; a brace extent comes from the shared delimiter matcher and its body role from opensSubquery.", "a7543d68f30c46edf107b9ecd53778a676f6abae61997336cc095d515b3e2cd4"},
		"cypher/label_expression_rewrite.go:labelExpressionRewriter.labelTest":              {"Label-chain termination punctuation; label grammar is independent of map entry grammar.", "081cc676e41b1492617eb2f4075dc85fa9cbca8a4af192d3d424a99033c53cda"},
		"cypher/label_expression_rewrite.go:mayUsePatternPredicate":                         {"Conservative predicate eligibility screen; no map or body admission.", "54ccf6f4771dce46c2368c7a599d209b54bad883e6fd4d686111cd0ea868caa7"},
		"cypher/node_helpers.go:StorageExecutor.extractVarName":                             {"Variable-name termination punctuation using inherited whitespace classification.", "3e493fef6813dd403c5e13a6327e1569378dad2bf87ff2aa0079e71fb7376ed5"},
		"cypher/optional_match_traversal_agg.go:subqueryExpressionEnd":                      {"Candidate enclosure lookup; body eligibility delegates to subqueryExpressionKeywordAt.", "5a0e76f0b1834b0bc1879034a109f98cb9310d96e2ae197cb73b82e4d23137db"},
		"cypher/parser.go:tokenizeSource":                                                   {"Inherited lexical token/source-span emission; common primitives decide name, number and opaque extents.", "fe0d40aadd64b25698819e80315f277aa5d7f946fe0d83d04684b34cc03cd52d"},
		"cypher/property_access_semantic_validation.go:scanPropertyAccesses":                {"Property access after a typed name; the brace role comes from braces.at.", "7034e81ba9b3f5bf730c502c777ab2fb0494cdef910908c0964f20de5be5857f"},
		"cypher/query_structure_rewrite.go:topLevelWords":                                   {"Top-level word extents; nested groups are skipped by the shared delimiter matcher.", "985802539d211b99f6dd54206e819d0b58bc0cf4e403125b5184fbd871db4c31"},
		"cypher/result_columns_projection.go:tryFlatReturnProjection":                       {"Conservative complexity screen; no brace role or entry verdict.", "9b584786e56a87d39a49c69e4ac7597511e9970f8880f7f16064dd6fbf66fbc5"},
		"cypher/row_subscript_validation.go:rowSubscriptReceiverStart":                      {"Expression receiver extent; a brace receiver's extent comes from braceEndingAt.", "51b2a2789e1b77e98003ca5b25497ff6be9063be44dbcd0af377dc691f6e1cf2"},
		"cypher/schema_contracts.go:splitConstraintContractEntries":                         {"REQUIRE block entries split at line breaks and semicolons; nested braces are skipped by braceView.close.", "a7a1e075e1fd8c7778a6b2e24c08b4dc200102286558ff70072163e2620779f6"},
		"cypher/semantic_validation.go:StorageExecutor.validateSemanticScopes":              {"Trailing comma after a statement's last projection item; no brace role.", "36cc3b4a8ee539246594cea38ff0d683c42ebb16e0667ef0d83b90df29acda0c"},
		"cypher/standalone_order_rewrite.go:scanStandaloneOrderClauses":                     {"Clause walk; a brace extent comes from the shared delimiter matcher and its body role from braces.at.", "f2c7c8adda742d57096815b46be82751817a9755fba69b0ec09c410e5757fc23"},
		"cypher/standalone_order_rewrite.go:standaloneClausePosition":                       {"Standalone-clause operand boundary punctuation; no brace role or entry verdict.", "ee3c1961dc975442e47cafe4fb736ca6fa9f0fccf18ef3ef08ce9bbc37f48dd1"},
		"cypher/statement_framing.go:startsWithClauseKeyword":                               {"A leading { opens a query block (CYPHER 25 { ... } UNION ...); unchanged from release.", "f7db0f3515bfc22dade1a1db5c3afffff1ef3c59884591dffc4d5361d2655d97"},
		"fabric/planner_source.go:planningCursor.next":                                      {"Fabric fragment cursor; a brace extent comes from the view's DelimiterSpan.", "00955fe710b94015301eaa14f911a872d936fb02486d24fa2fcdb042e3090654"},
	}
	seen := map[string]bool{}
	var hits []string
	for _, root := range []string{"../cypher", "../fabric"} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, _ := filepath.Rel("..", path)
			relative = filepath.ToSlash(relative)
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			fs := token.NewFileSet()
			file, err := parser.ParseFile(fs, path, source, parser.ParseComments)
			if err != nil {
				return err
			}
			// The generated ANTLR grammar remains the independent oracle.
			if ast.IsGenerated(file) && strings.HasPrefix(relative, "cypher/antlr/") {
				return nil
			}
			if owners[relative] {
				return nil
			}
			digests := map[string]string{}
			for _, declaration := range file.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok {
					continue
				}
				name := function.Name.Name
				if function.Recv != nil {
					name = strings.TrimPrefix(types.ExprString(function.Recv.List[0].Type), "*") + "." + name
				}
				start, end := fs.Position(function.Pos()).Offset, fs.Position(function.End()).Offset
				digests[name] = sourceDecisionDigest(source[start:end])
			}
			for _, name := range independentBraceReaders(file) {
				key := relative + ":" + name
				if reason := exceptions[key]; reason != "" {
					seen[key] = true
					continue
				}
				if adapter, ok := lexicalAdapters[key]; ok {
					seen[key] = true
					if adapter.reason != "" && adapter.sha256 == digests[name] {
						continue
					}
				}
				hits = append(hits, key)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for key := range exceptions {
		if !seen[key] {
			t.Errorf("stale exclusion %s", key)
		}
	}
	for key := range lexicalAdapters {
		if !seen[key] {
			t.Errorf("stale lexical adapter %s", key)
		}
	}
	sort.Strings(hits)
	for _, hit := range hits {
		t.Error("independent brace reader: " + hit)
	}
}

func TestBraceReaderShapeGuardMutation(t *testing.T) {
	for _, body := range []string{
		`if text[0] == '{' { return }`,
		`switch text[0] { case '}': return }`,
		`_ = strings.IndexByte(text, '{')`,
		`braceDepth := 0; braceDepth++`,
		`braceBody := text; _ = strings.Split(braceBody, ",")`,
		`_ = bytes.Contains(text, "}")`,
		`_ = bytes.Index(text, []byte("{"))`,
		`const opening = '{'; if text[0] == opening { return }`,
		`if text[0] == byte('{') { return }`,
		`depth := 0; if cyphertext.IsOpenBrace(text[0]) { depth++ }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "mutation.go", "package mutation; import (\"strings\"; \"bytes\"); func privateReader(text []byte) {"+body+"}", 0)
		if err != nil {
			t.Fatal(err)
		}
		if hits := independentBraceReaders(file); len(hits) != 1 {
			t.Fatalf("mutation escaped: %s (%v)", body, hits)
		}
	}
	file, err := parser.ParseFile(token.NewFileSet(), "literal.go", `package mutation; var hidden = func(text []byte) { if text[0] == '}' { return } }`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hits := independentBraceReaders(file); len(hits) != 1 || hits[0] != "hidden$literal" {
		t.Fatalf("function-literal mutation escaped: %v", hits)
	}
}
