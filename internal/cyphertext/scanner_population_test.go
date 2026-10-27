package cyphertext

import (
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type scannerPopulationEntry struct {
	Path        string   `json:"path"`
	Function    string   `json:"function"`
	Mechanisms  []string `json:"mechanisms"`
	Shared      bool     `json:"shared"`
	Disposition string   `json:"disposition,omitempty"`
	Reason      string   `json:"reason,omitempty"`
	Tests       []string `json:"tests,omitempty"`
}

type populationFunctionNode struct {
	direct bool
	calls  []string
}

// This is a finite ownership/population check, not a provenance interpreter.
// It re-derives all current direct lexical/wrapper callers and conservative
// raw lexical/numeric candidates across every production Go package, unions
// the r73f minimum and complete configured-URL sink manifest population, then
// requires a reviewed disposition. Behavioral tests remain separate evidence.
// source_reader_selectors.json records mechanism selectors and classifications
// for #1667's internal/workcensus. This extractor is frozen until the approved
// B0 rebase: switch member extraction to that go/packages + go/ssa engine then,
// retaining this test as its client. Do not grow another census engine here.
func TestFoundationLexicalPopulation(t *testing.T) {
	root := filepath.Join("..", "..")
	read := func(path string, target any) {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(data, target))
	}
	minimum := map[string][]string{}
	read("testdata/scanner_minimum.json", &minimum)
	sinks := map[string]bool{}
	for _, path := range []string{"url_sink_inventory.json", "url_sink_non_egress.json"} {
		manifest := map[string]string{}
		read(filepath.Join(root, "pkg", "observability", "testdata", path), &manifest)
		for key := range manifest {
			function, _, _ := strings.Cut(key, " ")
			function, _, _ = strings.Cut(function, "#")
			sinks[function] = true
		}
	}
	wrappers := map[string]bool{}
	for _, name := range strings.Fields("scanIdentifierToken scanSymbolicName parseIdentifierToken isPatternVariableName skipCypherLexicalToken scanCypherQuotedText skipCypherQuotedText skipQuotedText skipQuotedSemanticText queryCommentEnd queryWhitespaceAt queryGapEnd readQueryGap findMatchingParen findMatchingBracket findMatchingBrace keywordIndex keywordIndexFrom firstKeywordIndexFromDefault matchKeywordAt keywordMatchAt keywordEndAt isParameterStart scanParameterReferences numericLiteralStartsAt groupedNumericLiteralEnd isASCIIDigit isDigitByte isCypherQuoteByte isCypherBacktickByte isCypherStringQuoteByte cypherLexicalByte cypherStringLexicalByte scanWord hasKeywordAt keywordAt skipSpacesAndComments") {
		wrappers[name] = true
	}
	outputCalls := map[string]bool{}
	for _, name := range strings.Fields("RedactURL RedactText RedactError RedactErrorDetails RedactClientError RedactLiterals redactLiteralsContext redactForLog emitSlowQueryLog startExecuteSpan redactAdminCommandError redactAdminCommandErrorSource DisplayValue DisplayArgs ClientCredentials ContextCredentials RemoteUserForDisplay") {
		outputCalls[name] = true
	}
	sourceMethods := map[string]bool{}
	for _, name := range strings.Fields("OriginalOffset OriginalSpan Window Rewrite PasswordBoundary RecordSource") {
		sourceMethods[name] = true
	}
	numericCalls := regexp.MustCompile(`(?i)(ScanNumber|NumberStartsAt|DigitRun|DigitForBase|IsDigit|numericLiteral|parseIntFast|parseFloatFast|kpScanNumber|kpScanInt|isASCIIDigit|isDigitByte)`)
	stateName := regexp.MustCompile(`(?i)^in(singlequote|doublequote|backtick|linecomment|blockcomment|string|quote|comment)$`)
	// Derive credential field candidates from the actual output policy table.
	// This deliberately over-approximates its word/boundary rules; it is a
	// census, not a second production sensitive-name classifier.
	credentialNames := map[string]bool{"pass": true, "passwd": true, "secretref": true, "clientsecret": true}
	normalizeField := func(name string) string {
		return strings.Map(func(character rune) rune {
			if character >= 'A' && character <= 'Z' {
				return character + ('a' - 'A')
			}
			if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
				return character
			}
			return -1
		}, name)
	}
	policyTree, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "pkg", "observability", "redaction.go"), nil, 0)
	require.NoError(t, err)
	policyKeys := 0
	for _, declaration := range policyTree.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range group.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, name := range value.Names {
				if name.Name == "DefaultRedactKeys" {
					for _, expression := range value.Values {
						ast.Inspect(expression, func(node ast.Node) bool {
							if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
								key, err := strconv.Unquote(literal.Value)
								require.NoError(t, err)
								credentialNames[normalizeField(key)] = true
								policyKeys++
							}
							return true
						})
					}
				}
			}
		}
	}
	require.Greater(t, policyKeys, 0, "canonical sensitive-key policy table absent")
	credentialName := func(name string) bool {
		normalized := normalizeField(name)
		for key := range credentialNames {
			if strings.Contains(normalized, key) {
				return true
			}
		}
		return false
	}
	outputName := regexp.MustCompile(`^(Printf|Println|Fprintf|Fatalf|Errorf|Warn|Warnf|Info|Infof|Debug|Debugf|Error|Encode|Write|WriteString|Send|Set|Add|SetHeader|WriteJSON|JSON|WriteError)$`)
	patternClass := regexp.MustCompile(`\\[dsw]|\[0-9|A-Za-z|a-zA-Z`)
	var actual []scannerPopulationEntry
	functions := map[string]populationFunctionNode{}
	testNames := map[string]bool{}
	seenMinimum := map[string]map[string]bool{}
	for path := range minimum {
		seenMinimum[path] = map[string]bool{}
	}
	for _, directory := range []string{"internal", "pkg", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == "vendor" || entry.Name() == "node_modules" || entry.Name() == ".git" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") {
				return nil
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			set := token.NewFileSet()
			tree, err := parser.ParseFile(set, path, nil, parser.ParseComments)
			if err != nil {
				return err
			}
			if strings.HasSuffix(path, "_test.go") {
				for _, declaration := range tree.Decls {
					if fn, ok := declaration.(*ast.FuncDecl); ok && strings.HasPrefix(fn.Name.Name, "Test") {
						testNames[fn.Name.Name] = true
					}
				}
				return nil
			}
			if ast.IsGenerated(tree) {
				actual = append(actual, scannerPopulationEntry{Path: relative, Function: "<generated>", Mechanisms: []string{"generated-oracle"}})
				return nil
			}
			imports := map[string]string{}
			for _, item := range tree.Imports {
				imported, err := strconv.Unquote(item.Path.Value)
				if err != nil {
					return err
				}
				name := filepath.Base(imported)
				if item.Name != nil {
					name = item.Name.Name
				}
				imports[name] = imported
			}
			regexGlobals := map[string]bool{}
			for _, declaration := range tree.Decls {
				group, ok := declaration.(*ast.GenDecl)
				if !ok {
					continue
				}
				for _, spec := range group.Specs {
					value, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					matched := false
					for _, expression := range value.Values {
						ast.Inspect(expression, func(node ast.Node) bool {
							call, ok := node.(*ast.CallExpr)
							if !ok {
								return true
							}
							target, ok := call.Fun.(*ast.SelectorExpr)
							if !ok {
								return true
							}
							receiver, ok := target.X.(*ast.Ident)
							if !ok || imports[receiver.Name] != "regexp" {
								return true
							}
							for _, argument := range call.Args {
								if literal, ok := argument.(*ast.BasicLit); ok && literal.Kind == token.STRING {
									pattern, err := strconv.Unquote(literal.Value)
									if err == nil && patternClass.MatchString(pattern) {
										matched = true
									}
								}
							}
							return true
						})
					}
					if matched {
						for _, name := range value.Names {
							regexGlobals[name.Name] = true
						}
					}
				}
			}
			for _, declaration := range tree.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				name := fn.Name.Name
				if fn.Recv != nil && len(fn.Recv.List) > 0 {
					name = testfixture.ReceiverName(fn.Recv.List[0].Type, testfixture.ReceiverWithPointers) + "." + name
				}
				mechanisms := map[string]bool{}
				forced := false
				for _, required := range minimum[relative] {
					if required == fn.Name.Name {
						forced = true
						seenMinimum[relative][required] = true
					}
				}
				if forced {
					mechanisms["review-minimum"] = true
				}
				if sinks[relative+":"+name] {
					mechanisms["credential-output"] = true
				}
				packagePath := filepath.ToSlash(filepath.Dir(relative))
				functionKey := packagePath + ":" + name
				var calls []string
				shared, loop, rawDigit, rawWhitespace, rawQuote := false, false, false, false, false
				credentialRead, output, returned := false, false, false
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					switch node := node.(type) {
					case *ast.ForStmt, *ast.RangeStmt:
						loop = true
					case *ast.Ident:
						if stateName.MatchString(node.Name) {
							mechanisms["opaque-state"] = true
						}
						if credentialName(node.Name) {
							credentialRead = true
						}
						if regexGlobals[node.Name] {
							mechanisms["constant-regexp-reader"] = true
						}
					case *ast.ReturnStmt:
						if fn.Type.Results != nil && len(fn.Type.Results.List) > 0 {
							returned = true
						}
					case *ast.BasicLit:
						if node.Kind == token.STRING {
							value, err := strconv.Unquote(node.Value)
							if err == nil && credentialName(value) {
								credentialRead = true
							}
						}
						if node.Kind == token.CHAR {
							value, err := strconv.Unquote(node.Value)
							if err == nil {
								if value == " " || value == "\t" {
									rawWhitespace = true
								}
								if value == "'" || value == "\"" || value == "`" {
									rawQuote = true
								}
							}
						}
					case *ast.BinaryExpr:
						for _, expr := range []ast.Expr{node.X, node.Y} {
							if literal, ok := expr.(*ast.BasicLit); ok && literal.Kind == token.CHAR && (literal.Value == "'0'" || literal.Value == "'9'") {
								rawDigit = true
							}
						}
					case *ast.CallExpr:
						call := ""
						switch target := node.Fun.(type) {
						case *ast.Ident:
							call = target.Name
							if target.Obj == nil || target.Obj.Kind == ast.Fun {
								calls = append(calls, packagePath+":"+call)
								if wrappers[call] {
									mechanisms["lexical-wrapper"] = true
								}
							}
						case *ast.SelectorExpr:
							call = target.Sel.Name
							if receiver, ok := target.X.(*ast.Ident); ok {
								imported := ""
								if receiver.Obj == nil || receiver.Obj.Kind == ast.Pkg {
									imported = imports[receiver.Name]
								}
								if strings.HasPrefix(imported, "github.com/orneryd/nornicdb/") {
									calls = append(calls, strings.TrimPrefix(imported, "github.com/orneryd/nornicdb/")+":"+call)
								}
								if fn.Recv != nil && len(fn.Recv.List) > 0 && len(fn.Recv.List[0].Names) > 0 && receiver.Obj == fn.Recv.List[0].Names[0].Obj {
									calls = append(calls, packagePath+":"+testfixture.ReceiverName(fn.Recv.List[0].Type, testfixture.ReceiverWithPointers)+"."+call)
								}
								if imported == "github.com/orneryd/nornicdb/internal/cyphertext" {
									mechanisms["shared-call"] = true
									shared = true
								}
								if imported == "unicode" && (call == "IsSpace" || call == "IsLetter" || call == "IsDigit") || imported == "unicode/utf8" && strings.HasPrefix(call, "Decode") {
									mechanisms["unicode-class"] = true
								}
								if imported == "strconv" && (call == "Atoi" || strings.HasPrefix(call, "ParseInt") || strings.HasPrefix(call, "ParseUint") || strings.HasPrefix(call, "ParseFloat")) {
									mechanisms["numeric-conversion"] = true
								}
								if imported == "regexp" {
									for _, argument := range node.Args {
										if literal, ok := argument.(*ast.BasicLit); ok && literal.Kind == token.STRING {
											pattern, err := strconv.Unquote(literal.Value)
											if err == nil && patternClass.MatchString(pattern) {
												mechanisms["constant-regexp-reader"] = true
											}
										}
									}
								}
							}
							if sourceMethods[call] && imports["cyphertext"] != "" {
								mechanisms["shared-source-view"] = true
							}
						}
						if numericCalls.MatchString(call) {
							mechanisms["numeric-reader"] = true
						}
						if outputCalls[call] {
							mechanisms["credential-output"] = true
						}
						if outputName.MatchString(call) {
							output = true
						}
					}
					return true
				})
				if loop && rawDigit {
					mechanisms["raw-numeric-loop"] = true
				}
				if loop && rawWhitespace {
					mechanisms["raw-whitespace-loop"] = true
				}
				if loop && rawQuote {
					mechanisms["raw-quote-loop"] = true
				}
				if credentialRead && (output || returned) {
					mechanisms["credential-field-output"] = true
				}
				if strings.HasPrefix(relative, "internal/cyphertext/") {
					mechanisms["primitive-owner"] = true
					shared = true
				}
				functions[functionKey] = populationFunctionNode{direct: shared, calls: calls}
				keys := make([]string, 0, len(mechanisms))
				for key := range mechanisms {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				actual = append(actual, scannerPopulationEntry{Path: relative, Function: name, Mechanisms: keys, Shared: shared})
			}
			return nil
		})
		require.NoError(t, err)
	}
	// Resolve actual package/function bindings rather than trusting a wrapper's
	// spelling in an unrelated package or a shadowed local variable. Only known
	// static calls and the current receiver are linked; interface/field dispatch
	// remains an explicit limit of this structural check.
	reachable := map[string]bool{}
	reverse := map[string][]string{}
	var queue []string
	for key, node := range functions {
		if node.direct {
			reachable[key] = true
			queue = append(queue, key)
		}
		for _, called := range node.calls {
			reverse[called] = append(reverse[called], key)
		}
	}
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		for _, caller := range reverse[key] {
			if !reachable[caller] {
				reachable[caller] = true
				queue = append(queue, caller)
			}
		}
	}
	complete := actual[:0]
	for index := range actual {
		row := actual[index]
		row.Shared = reachable[filepath.ToSlash(filepath.Dir(row.Path))+":"+row.Function]
		if row.Shared && len(row.Mechanisms) == 0 {
			row.Mechanisms = []string{"shared-wrapper"}
		}
		if len(row.Mechanisms) > 0 {
			complete = append(complete, row)
		}
	}
	actual = complete
	for path, names := range minimum {
		if !strings.HasSuffix(path, ".go") {
			_, err := os.Stat(filepath.Join(root, filepath.FromSlash(path)))
			require.NoError(t, err)
			actual = append(actual, scannerPopulationEntry{Path: path, Function: "<language-boundary>", Mechanisms: []string{"language-boundary"}})
			continue
		}
		for _, name := range names {
			require.True(t, seenMinimum[path][name], "minimum declaration absent: %s:%s", path, name)
		}
	}
	sort.Slice(actual, func(i, j int) bool {
		return actual[i].Path+":"+actual[i].Function < actual[j].Path+":"+actual[j].Function
	})
	if os.Getenv("NORNICDB_LEXICAL_POPULATION_DISCOVERY") == "1" {
		data, err := json.Marshal(actual)
		require.NoError(t, err)
		fmt.Println("FOUNDATION_POPULATION " + string(data))
		return
	}
	var reviewed []scannerPopulationEntry
	read("testdata/lexical_population.json", &reviewed)
	require.Len(t, reviewed, len(actual), "population changed; run explicit discovery and review every new/removed member")
	for index, row := range actual {
		expected := reviewed[index]
		require.Equal(t, row.Path+":"+row.Function, expected.Path+":"+expected.Function)
		require.Equal(t, row.Mechanisms, expected.Mechanisms, row.Path+":"+row.Function)
		require.Equal(t, row.Shared, expected.Shared, row.Path+":"+row.Function)
		require.NotEmpty(t, expected.Reason, row.Path+":"+row.Function)
		require.NotEmpty(t, expected.Tests, row.Path+":"+row.Function)
		for _, name := range expected.Tests {
			require.True(t, testNames[name], "missing evidence test %s for %s:%s", name, row.Path, row.Function)
		}
		switch expected.Disposition {
		case "shared-source":
			require.True(t, row.Shared, row.Path+":"+row.Function)
		case "primitive-owner":
			require.Contains(t, row.Mechanisms, "primitive-owner")
		case "generated-oracle":
			require.Contains(t, row.Mechanisms, "generated-oracle")
		case "language-peer":
			require.Contains(t, row.Mechanisms, "language-boundary")
		case "data-grammar", "contextual-grammar", "credential-output", "credential-runtime":
		default:
			t.Fatalf("unreviewed member %s:%s", row.Path, row.Function)
		}
	}
	t.Logf("checked %d complete scanner/numeric/output candidates against the reviewed population", len(actual))
}
