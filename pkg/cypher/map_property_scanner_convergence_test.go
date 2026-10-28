package cypher

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This is the mechanism matrix for every property/map reader converted in
// Rule 73 round 6. It deliberately reuses mapQuotationCases, the same
// 16-key x 5-value x 3-layout generator as the shared materializers, so a
// structural reader cannot quietly receive a smaller lexical sample.
func TestPropertyScannerQuotingMatrixPublicForms(t *testing.T) {
	for _, tc := range mapQuotationCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "scanner_"+strings.ReplaceAll(tc.name, "/", "_"))
			exec := NewStorageExecutor(store)
			_, err := exec.Execute(ctx, "CREATE (a:R6Start "+tc.text+")-[:R6 "+tc.text+"]->(b:R6End "+tc.text+")", nil)
			require.NoError(t, err)

			queries := []struct {
				name, text string
				want       int64
			}{
				{"shortest-relationship-positive", "MATCH p = SHORTEST 1 (a:R6Start)(()-[:R6 " + tc.text + "]->())+(b:R6End) RETURN count(*) AS c", 1},
				{"shortest-node-positive", "MATCH p = SHORTEST 1 (a:R6Start)(()-[:R6]->())+(b:R6End " + tc.text + ") RETURN count(*) AS c", 1},
				{"shortest-relationship-negative", "MATCH p = SHORTEST 1 (a:R6Start)(()-[:R6 {keep:8}]->())+(b:R6End) RETURN count(*) AS c", 0},
				{"shortest-node-negative", "MATCH p = SHORTEST 1 (a:R6Start)(()-[:R6]->())+(b:R6End {keep:8}) RETURN count(*) AS c", 0},
			}
			for _, query := range queries {
				t.Run(query.name, func(t *testing.T) {
					result, queryErr := exec.Execute(ctx, query.text, nil)
					require.NoError(t, queryErr, query.text)
					require.Equal(t, [][]interface{}{{query.want}}, result.Rows, query.text)
				})
			}

			chainCreate := "CREATE (ca:R6ChainStart)-[:R6Chain]->(cb:R6ChainMiddle " + tc.text + ")-[:R6Chain]->(cc:R6ChainEnd)"
			_, chainCreateErr := exec.Execute(ctx, chainCreate, nil)
			require.NoError(t, chainCreateErr, chainCreate)
			for _, chain := range []struct {
				name, properties string
				want             int64
			}{{"chain-positive", tc.text, 1}, {"chain-negative", "{keep:8}", 0}} {
				t.Run(chain.name, func(t *testing.T) {
					query := "MATCH (ca:R6ChainStart)-[:R6Chain]->(cb:R6ChainMiddle " + chain.properties + ")-[:R6Chain]->(cc:R6ChainEnd) RETURN count(*) AS c"
					got, chainErr := exec.Execute(ctx, query, nil)
					require.NoError(t, chainErr, query)
					require.Equal(t, [][]interface{}{{chain.want}}, got.Rows, query)
				})
			}

			before := propertyDurableSnapshot(t, store)
			quotedKey := "`" + strings.ReplaceAll(tc.key, "`", "``") + "`"
			invalidCreate := "CREATE (x:R6Bad {id:7})-[:R6Bad]->(y:R6Bad {" + quotedKey + ":properties(1)})"
			_, err = exec.Execute(ctx, invalidCreate, nil)
			require.Error(t, err, invalidCreate)
			require.Equal(t, before, propertyDurableSnapshot(t, store), invalidCreate)

			_, err = exec.Execute(ctx, ":param "+tc.text, nil)
			require.NoError(t, err, tc.text)
			params, err := exec.Execute(ctx, ":params", nil)
			require.NoError(t, err)
			found := false
			for _, row := range params.Rows {
				if len(row) == 2 && row[0] == tc.key {
					found = true
				}
			}
			require.True(t, found, "shell key %q", tc.key)
		})
	}
}

func TestPropertyScannerQuotingMatrixInternalReaders(t *testing.T) {
	for _, tc := range mapQuotationCases() {
		t.Run(tc.name, func(t *testing.T) {
			var keys []string
			require.True(t, visitStaticMapLiteralKeys(tc.text, func(key string) { keys = append(keys, key) }))
			require.Equal(t, []string{tc.key, "keep", "tail"}, keys)

			parts := splitTopLevelComma(strings.TrimSpace(tc.text[1 : len(tc.text)-1]))
			require.Len(t, parts, 3)
			require.Greater(t, findTopLevelMapKeyValueSeparator(parts[0]), 0)
			quotedKey := "`" + strings.ReplaceAll(tc.key, "`", "``") + "`"
			require.True(t, validMapLiteralKey(quotedKey))

			_, _, _, ok := parseCallTailSinglePropertyMap(tc.text + " tail")
			require.False(t, ok, "restricted single-property CALL fast path must decline a multi-entry map")
		})
	}
}
func TestPropertyScannerFamilyUsesCanonicalReaders(t *testing.T) {
	bannedDeclarations := map[string]bool{
		"splitPropertyPairs": true, "numericValidationSkipQuoted": true,
		"topLevelColonIndex": true, "findTopLevelByte": true,
		"splitProcedureTopLevelComma": true, "splitParamsCarefully": true,
		"splitCommaTopLevelLocal": true, "splitArgsRespectingArrays": true,
	}
	quoteMechanismAllowlist := map[string]string{
		"pkg/cypher/executor_spans.go:redactSpanErrorText":        "Finds quoted Cypher fragments inside free-form trace errors, then delegates escape handling and literal masking to the canonical Cypher readers.",
		"pkg/adminimport/importer.go:withDefaults":                "sets a CSV quote option; does not read Cypher source",
		"pkg/adminimport/importer.go:openCSVSource":               "validates a CSV quote option before encoding/csv parsing; does not read Cypher source",
		"pkg/adminimport/neo4j_csv.go:ExportNeo4jCSV":             "sets and validates the CSV output quote option; does not read Cypher source",
		"pkg/observability/logging.go:appendEscapedJSONString":    "encodes log attribute values as JSON, not Cypher source",
		"pkg/observability/logging.go:safeASCII":                  "classifies JSON output escaping, not Cypher source",
		"internal/rerankwire/decode.go:validateEnvelopeProse":     "validates JSON response-envelope prose, not Cypher source",
		"internal/rerankwire/decode.go:possibleJSONScalarStart":   "classifies JSON response-envelope scalar starts, not Cypher source",
		"testing/cypher/tck/value.go:parseValue":                  "parses TCK expected-result table notation, not executable Cypher text",
		"testing/cypher/tck/value.go:parseString":                 "decodes TCK expected-result table strings, not executable Cypher text",
		"testing/cypher/tck/value.go:parseKey":                    "parses TCK expected-result map keys, not executable Cypher text",
		"internal/cyphertext/scan.go:IsQuote":                     "canonical Cypher quote classification owner",
		"internal/cyphertext/scan.go:TrimQuoteWrapper":            "canonical matched text-wrapper owner; delegates delimiter classification to IsQuote while preserving internal URL apostrophes",
		"internal/cyphertext/scan.go:IsStringQuote":               "canonical Cypher string-quote classification owner",
		"internal/cyphertext/scan.go:IsSingleQuote":               "canonical Cypher single-string delimiter classification owner",
		"internal/cyphertext/scan.go:IsDoubleQuote":               "canonical Cypher double-string delimiter classification owner",
		"internal/cyphertext/scan.go:IsBacktick":                  "canonical quoted symbolic-name classification owner",
		"internal/cyphertext/scan.go:scanQuotedText":              "canonical Cypher string and symbolic-name quotation owner",
		"pkg/cypher/keyword_scan.go:nameContinuesExpression":      "canonical keyword scanner expression-start classification",
		"pkg/cypher/keyword_scan.go:isOperatorWith":               "canonical keyword scanner WITH-operator classification",
		"pkg/cypher/keyword_scan.go:queryMayNeedCanonicalRewrite": "canonical query-rewrite lexical admission reader",
		"pkg/cypher/keyword_scan.go:aliasKeywordAt":               "canonical alias-keyword boundary reader",
		"pkg/cypher/keyword_scan.go:readCanonicalQueryText":       "canonical query text, quotation and comment rewrite owner",
		"pkg/cypher/keyword_scan.go:inPatternClause":              "canonical clause-boundary reader",
		"pkg/cypher/keyword_scan.go:stripCommentText":             "canonical Cypher comment-removal traversal; optional edit sink retains AST source provenance",
		"internal/cyphertext/source_unicode.go:PrintIdentifier":   "canonical stage-aware identifier source printer; raw source escapes literal backslashes and unpaired units, accepted tokens retain identity; tested by TestUnicodeSourceLiteralAndStoredNamePolicies and TestStoredLiteralUnicodeNameSourceRoundtrip",
		"pkg/cypher/result_restoration.go:value":                  "canonical typed metadata/diagnostic restoration owner; prose quotation recovers source fragments and Source.OriginalPosition maps parser coordinates",
		"internal/cyphertext/structure.go:FindMatchingDelimiter":  "canonical quote/comment-aware delimiter reader",
		"internal/cyphertext/string.go:DecodeString":              "canonical Cypher string decoder",
		"pkg/cypher/quoted_variables.go:isBacktickQuotedName":     "canonical quoted symbolic-name validator",
		"pkg/cypher/fulltext_query.go:ftTokenize":                 "parses decoded Lucene fulltext query syntax, not Cypher text",
		"pkg/cypher/temporal_format.go:formatDurationPattern":     "parses a decoded Java duration format pattern, not Cypher text",
		"pkg/cypher/temporal_format.go:stripDurationQuotes":       "parses a decoded Java duration format pattern, not Cypher text",
		"pkg/cypher/temporal_pattern.go:compileTemporalPattern":   "parses a decoded Java temporal construction pattern, not Cypher text",
		"pkg/cypher/functions_text_locate.go:fnTextLocate":        "reads decoded function argument text, not Cypher source",
		"pkg/cypher/functions_text_locate.go:textLocatePattern":   "builds a regex from decoded function argument text, not Cypher source",
		"cmd/nornicdb-admin/main.go:runImportFull":                "reads a CSV quote command-line option, not Cypher text",
		"cmd/nornicdb-admin/main.go:runExportNeo4jCSV":            "reads a CSV quote command-line option, not Cypher text",
		"internal/jsontext/append_string.go:AppendString":         "canonical JSON string emitter, not Cypher quotation; TestJSONAppendStringExactUnits verifies exact UTF16 units and zero-allocation ASCII",
		"internal/jsontext/strings.go:projectJSONStrings":         "canonical JSON string lexical projection for raw, binding, opaque and wire stages, not Cypher quotation; codec tests preserve literal escapes, lone units, pairs and opaque RawMessage",
	}
	// These production helpers exist only on one maintained branch. They are
	// still explicit, reasoned exceptions when present, but absence on the
	// other branch cannot make that branch's guard stale.
	branchOptionalAllowlist := map[string]string{
		"pkg/multidb/remote_credentials.go:isCredentialFreeSimpleHTTPURL": "This URL fast path rejects userinfo, query, fragment, and ambiguous authority punctuation; it does not scan Cypher names or strings.",
		"pkg/cypher/json_stringify.go:writeString":                        "emits JSON string escapes and does not read Cypher text",
		"pkg/cypher/json_stringify.go:pythonBytesRepr":                    "emits a Python bytes representation and does not read Cypher text",
	}

	repoRoot, err := scannerGuardRepositoryRoot()
	require.NoError(t, err)
	seenAllowlist := make(map[string]bool, len(quoteMechanismAllowlist))
	var violations []string
	err = filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" || entry.Name() == "node_modules" || entry.Name() == ".git" || entry.Name() == ".verify" {
				// Verification checkouts are copies of other revisions, not
				// source files belonging to this revision's scanner family.
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		relative, relativeErr := filepath.Rel(repoRoot, path)
		if relativeErr != nil {
			return relativeErr
		}
		relative = filepath.ToSlash(relative)
		fileSet := token.NewFileSet()
		file, parseErr := parser.ParseFile(fileSet, path, nil, parser.ParseComments)
		if parseErr != nil {
			return parseErr
		}
		if ast.IsGenerated(file) {
			return nil
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			name := function.Name.Name
			if bannedDeclarations[name] {
				violations = append(violations, fmt.Sprintf("%s:%s reintroduced banned scanner declaration", relative, name))
			}
			mechanisms := scannerQuoteMechanisms(fileSet, function.Body)
			if len(mechanisms) == 0 {
				continue
			}
			key := relative + ":" + name
			allowKey := key
			reason, allowed := quoteMechanismAllowlist[allowKey]
			if !allowed {
				allowKey = key
				reason, allowed = branchOptionalAllowlist[allowKey]
			}
			if allowed {
				require.NotEmpty(t, strings.TrimSpace(reason), "allowlist reason for %s", key)
				if _, required := quoteMechanismAllowlist[allowKey]; required {
					seenAllowlist[allowKey] = true
				}
				continue
			}
			violations = append(violations, key+" compares or tracks Cypher quotation independently")
		}
		return nil
	})
	require.NoError(t, err)
	for key := range quoteMechanismAllowlist {
		if !seenAllowlist[key] {
			violations = append(violations, "stale canonical-reader allowlist entry "+key)
		}
	}
	if len(violations) != 0 {
		t.Fatalf("independent Cypher quotation mechanisms:\n%s", strings.Join(violations, "\n"))
	}
}

func scannerUsesCanonicalQuoteReader(relativePath string, body *ast.BlockStmt) bool {
	// A canonical call elsewhere in a function says nothing about a private
	// quote loop. Canonical owners are admitted only by the exact, reasoned
	// per-function allowlist in TestPropertyScannerFamilyUsesCanonicalReaders.
	return false
}

func scannerGuardRepositoryRoot() (string, error) {
	directory, err := filepath.Abs(".")
	if err != nil {
		return "", err
	}
	for {
		if _, statErr := os.Stat(filepath.Join(directory, "go.mod")); statErr == nil {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", fmt.Errorf("repository root not found from %s", directory)
		}
		directory = parent
	}
}

func scannerQuoteMechanisms(fileSet *token.FileSet, body *ast.BlockStmt) []string {
	seen := make(map[string]bool)
	ast.Inspect(body, func(node ast.Node) bool {
		switch expression := node.(type) {
		case *ast.BinaryExpr:
			if expression.Op != token.EQL && expression.Op != token.NEQ {
				return true
			}
			if scannerQuoteLiteral(expression.X) || scannerQuoteLiteral(expression.Y) {
				seen[fileSet.Position(expression.Pos()).String()] = true
			}
		case *ast.CaseClause:
			for _, candidate := range expression.List {
				if scannerQuoteLiteral(candidate) {
					seen[fileSet.Position(candidate.Pos()).String()] = true
				}
			}
		case *ast.AssignStmt:
			for _, target := range expression.Lhs {
				if identifier, ok := target.(*ast.Ident); ok && scannerQuoteStateIdentifier(identifier.Name) {
					seen[fileSet.Position(identifier.Pos()).String()] = true
				}
			}
			for _, value := range expression.Rhs {
				if scannerQuoteLiteral(value) {
					seen[fileSet.Position(value.Pos()).String()] = true
				}
			}
		case *ast.ValueSpec:
			for _, identifier := range expression.Names {
				if scannerQuoteStateIdentifier(identifier.Name) {
					seen[fileSet.Position(identifier.Pos()).String()] = true
				}
			}
			for _, value := range expression.Values {
				if scannerQuoteLiteral(value) {
					seen[fileSet.Position(value.Pos()).String()] = true
				}
			}
		}
		return true
	})
	mechanisms := make([]string, 0, len(seen))
	for position := range seen {
		mechanisms = append(mechanisms, position)
	}
	sort.Strings(mechanisms)
	return mechanisms
}

func scannerQuoteStateIdentifier(name string) bool {
	switch strings.ToLower(name) {
	case "quote", "quotechar", "quotedelimiter", "insingle", "indouble", "inbacktick":
		return true
	default:
		return false
	}
}

func scannerQuoteLiteral(expression ast.Expr) bool {
	literal, ok := expression.(*ast.BasicLit)
	if !ok || (literal.Kind != token.CHAR && literal.Kind != token.STRING) {
		return false
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		return false
	}
	runes := []rune(value)
	return len(runes) == 1 && strings.ContainsRune("'\"`", runes[0])
}
