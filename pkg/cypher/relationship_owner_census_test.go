package cypher

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const relationshipOwnerModule = "github.com/orneryd/nornicdb/"

// relationshipOwnerProducers read relationship or contract syntax, or produce
// compiled contract plans. Every production use of one (a call, a function or
// method value, an alias, a closure, a package-level initializer) is a ledger
// record classified by the lifetime of the work: once per statement shape,
// statement execution, row, checked entity, schema revision, DDL statement,
// cache invalidation, or inside the owner itself. Uses are resolved by type,
// so renaming a function value does not hide it. A new use fails until it is
// classified.
var relationshipOwnerProducers = map[string]bool{
	"cypher.relationshipDeclarationHead": true, "cypher.relationshipDeclarationOf": true,
	"cypher.relationshipDeclarationEntryOf": true, "cypher.readRelationshipDeclarationHead": true,
	"(*cypher.relationshipDeclarationCache).readDeclaration": true, "cypher.readRelationshipHop": true,
	"cypher.readRelationshipHopFrame": true, "cypher.readRelationshipHopPrefix": true,
	"cypher.readRelationshipFrame": true, "cypher.readRelationshipConnection": true,
	"cypher.relationshipConnectionOf": true, "cypher.relationshipFrameAfterNode": true,
	"cypher.relationshipPrefixAfterNode": true, "cyphertext.ReadConnectionFrame": true,
	"cyphertext.readConnectionFrame": true, "cyphertext.ReadConnectionPrefix": true,
	"cyphertext.ReadRelationshipTypes": true, "cypher.traversalPatternParts": true,
	"cypher.relationshipChainEnd": true, "cypher.relationshipNodeReaderText": true,
	"cypher.readRelationshipNodeText": true, "cypher.relationshipExpressionReaderText": true,
	"cypher.eachPatternElement": true, "cypher.statementSchemaFactsOf": true,
	"cypher.scanQuotedTokens":          true,
	"cypher.compileQuotedVariablePlan": true, "cypher.canonicalizeQuotedVariables": true,
	"cypher.parseBoundDegreeShapeText": true, "cypher.parseBoundDegreeShape": true,
	"cypher.relationshipLookupPattern": true,
	"cypher.boundOutgoingRelationship": true,
	"cypher.mayUsePatternPredicate":    true, "cypher.boundChainShapeFor": true,
	"cypher.parseBoundChainShape": true, "cyphertext.CompileConstraintPattern": true,
	"cyphertext.SplitConstraintEntries": true, "storage.parseConstraintPattern": true,
	"storage.compileNodeConstraintContractExpression": true, "storage.compileRelationshipConstraintContractExpression": true,
	"storage.compileConstraintContractEntryPlan": true, "storage.compileConstraintContractRevision": true,
	"storage.CompileConstraintContract": true,
}

// Lifetimes on which repeated syntax work multiplies with data. A record of
// one of these may only reach a producer that retains its result per shape.
var relationshipOwnerRepeatedLifetimes = map[string]bool{"row": true, "entity": true}

var relationshipOwnerRetainingProducers = map[string]bool{
	"cypher.relationshipDeclarationHead": true, "cypher.relationshipDeclarationOf": true,
	"(*cypher.relationshipDeclarationCache).readDeclaration": true, "cypher.canonicalizeQuotedVariables": true,
	"cypher.statementSchemaFactsOf": true,
	"cypher.boundChainShapeFor":     true,
	"cypher.parseBoundDegreeShape":  true,
}

var relationshipOwnerLifetimes = map[string]bool{
	"shape": true, "execution": true, "row": true, "entity": true, "revision": true,
	"ddl": true, "invalidation": true, "owner": true,
	// row-text reads text built for each row from row values (as the bases
	// do); there is no row-independent shape to retain. row-fallback is the
	// generic row-expression evaluator recognizing a pattern operand in the
	// expression it is given, the bases' per-call structural walk.
	"row-text": true, "row-fallback": true,
}

// relationshipOwnerQualified shortens a function's full name to its package
// name: (*github.com/orneryd/nornicdb/pkg/cypher.T).m becomes (*cypher.T).m.
func relationshipOwnerQualified(fn *types.Func) string {
	name := fn.FullName()
	for _, prefix := range []string{relationshipOwnerModule + "pkg/", relationshipOwnerModule + "internal/", relationshipOwnerModule + "cmd/"} {
		name = strings.ReplaceAll(name, prefix, "")
	}
	return name
}

// censusPackage is one module package type-checked for a census: its files,
// their repository-relative names and the resolved uses and types.
type censusPackage struct {
	files []*ast.File
	names map[*ast.File]string
	info  *types.Info
}

// typeCheckCensusPackages type-checks every module package with a file that
// mentions one of the identifiers wanted reports true for. overlay adds
// in-memory source files to a package by import path, for mutation tests.
func typeCheckCensusPackages(t *testing.T, overlay map[string][]string, wanted func(string) bool) []censusPackage {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "list", "-deps", "-export", "-json", "./pkg/...", "./internal/...", "./cmd/...")
	command.Dir = root
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, stderr.String())
	}
	type listed struct {
		ImportPath, Dir, Export string
		GoFiles, CgoFiles       []string
	}
	exports := map[string]string{}
	var population []listed
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var pkg listed
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		exports[pkg.ImportPath] = pkg.Export
		if strings.HasPrefix(pkg.ImportPath, relationshipOwnerModule) {
			population = append(population, pkg)
		}
	}
	fset := token.NewFileSet()
	lookup := func(path string) (io.ReadCloser, error) {
		if exports[path] == "" {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(exports[path])
	}
	var checked []censusPackage
	for _, pkg := range population {
		census := censusPackage{names: map[*ast.File]string{}}
		mentions := false
		mention := func(file *ast.File) {
			ast.Inspect(file, func(node ast.Node) bool {
				if ident, ok := node.(*ast.Ident); ok && wanted(ident.Name) {
					mentions = true
				}
				return !mentions
			})
		}
		add := func(file *ast.File, name string) {
			census.files = append(census.files, file)
			rel, _ := filepath.Rel(root, filepath.Join(pkg.Dir, name))
			census.names[file] = filepath.ToSlash(rel)
			mention(file)
		}
		for _, name := range append(append([]string{}, pkg.GoFiles...), pkg.CgoFiles...) {
			file, err := parser.ParseFile(fset, filepath.Join(pkg.Dir, name), nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			add(file, name)
		}
		for index, source := range overlay[pkg.ImportPath] {
			name := fmt.Sprintf("overlay_%d.go", index)
			file, err := parser.ParseFile(fset, name, source, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			add(file, name)
		}
		if !mentions {
			continue
		}
		census.info = &types.Info{Uses: map[*ast.Ident]types.Object{}, Defs: map[*ast.Ident]types.Object{}, Types: map[ast.Expr]types.TypeAndValue{}}
		config := types.Config{Importer: importer.ForCompiler(fset, "gc", lookup), FakeImportC: true, Sizes: types.SizesFor("gc", "amd64")}
		if _, err := config.Check(pkg.ImportPath, fset, census.files, census.info); err != nil {
			t.Fatalf("type-check %s: %v", pkg.ImportPath, err)
		}
		checked = append(checked, census)
	}
	return checked
}

// censusEnclosing names the function a declaration belongs to, or <package>.
func censusEnclosing(info *types.Info, decl ast.Decl) string {
	if function, ok := decl.(*ast.FuncDecl); ok {
		if object, ok := info.Defs[function.Name].(*types.Func); ok {
			return relationshipOwnerQualified(object)
		}
	}
	return "<package>"
}

// relationshipOwnerCensus type-checks every module package that can refer to
// a producer and records each resolved use of one. overlay adds in-memory
// source files to a package by import path, for mutation tests.
func relationshipOwnerCensus(t *testing.T, overlay map[string][]string) map[string]int {
	t.Helper()
	bare := map[string]bool{}
	for name := range relationshipOwnerProducers {
		bare[name[strings.LastIndexAny(name, ".)")+1:]] = true
	}
	records := map[string]int{}
	for _, pkg := range typeCheckCensusPackages(t, overlay, func(name string) bool { return bare[name] }) {
		for _, file := range pkg.files {
			for _, decl := range file.Decls {
				enclosing := censusEnclosing(pkg.info, decl)
				ast.Inspect(decl, func(node ast.Node) bool {
					ident, ok := node.(*ast.Ident)
					if !ok {
						return true
					}
					if function, ok := pkg.info.Uses[ident].(*types.Func); ok {
						if producer := relationshipOwnerQualified(function); relationshipOwnerProducers[producer] {
							records[pkg.names[file]+"|"+enclosing+"|"+producer]++
						}
					}
					return true
				})
			}
		}
	}
	return records
}

func relationshipOwnerLedger(t *testing.T) map[string]string {
	t.Helper()
	ledger, err := os.Open(filepath.Join("testdata", "relationship_owner_census.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	classified := map[string]string{}
	scanner := bufio.NewScanner(ledger)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "|")
		if len(fields) != 4 || !relationshipOwnerLifetimes[fields[3]] {
			t.Fatalf("malformed ledger line %q", line)
		}
		classified[strings.Join(fields[:3], "|")] = fields[3]
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return classified
}

// relationshipOwnerLedgerDiff returns the census uses missing from the
// ledger, ledger records no longer present, and repeated-lifetime records
// that reach a producer which does not retain its result.
func relationshipOwnerLedgerDiff(census map[string]int, classified map[string]string) (missing, stale, violations []string) {
	for record := range census {
		lifetime, ok := classified[record]
		if !ok {
			missing = append(missing, record+"|?")
			continue
		}
		producer := record[strings.LastIndexByte(record, '|')+1:]
		if relationshipOwnerRepeatedLifetimes[lifetime] && !relationshipOwnerRetainingProducers[producer] {
			violations = append(violations, record+"|"+lifetime)
		}
	}
	for record := range classified {
		if census[record] == 0 {
			stale = append(stale, record)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	sort.Strings(violations)
	return missing, stale, violations
}

func TestRelationshipOwnerCensusLedger(t *testing.T) {
	census := relationshipOwnerCensus(t, nil)
	missing, stale, violations := relationshipOwnerLedgerDiff(census, relationshipOwnerLedger(t))
	if len(missing)+len(stale)+len(violations) > 0 {
		t.Fatalf("relationship owner census differs from testdata/relationship_owner_census.txt\nunclassified (%d):\n%s\nno longer present (%d):\n%s\nrepeated syntax on a row/entity path (%d):\n%s",
			len(missing), strings.Join(missing, "\n"), len(stale), strings.Join(stale, "\n"), len(violations), strings.Join(violations, "\n"))
	}
	total := 0
	for _, count := range census {
		total += count
	}
	t.Logf("CENSUS records=%d uses=%d", len(census), total)
}

// A producer reached through a function value, an alias, a closure or a
// package-level initializer is still a new unclassified use.
func TestRelationshipOwnerCensusResolvesFunctionValues(t *testing.T) {
	const storagePath = relationshipOwnerModule + "pkg/storage"
	census := relationshipOwnerCensus(t, map[string][]string{storagePath: {`package storage

var aliasedCompiler = compileConstraintContractEntryPlan

func censusFunctionValue(entry ConstraintContractEntry) {
	compile := compileConstraintContractEntryPlan
	_, _ = compile(entry)
}

func censusClosure(entry ConstraintContractEntry) {
	run := func() { _ = compileConstraintContractRevision(ConstraintContract{Entries: []ConstraintContractEntry{entry}}) }
	run()
}
`}})
	missing, _, _ := relationshipOwnerLedgerDiff(census, relationshipOwnerLedger(t))
	for _, record := range []string{
		"pkg/storage/overlay_0.go|<package>|storage.compileConstraintContractEntryPlan|?",
		"pkg/storage/overlay_0.go|storage.censusClosure|storage.compileConstraintContractRevision|?",
		"pkg/storage/overlay_0.go|storage.censusFunctionValue|storage.compileConstraintContractEntryPlan|?",
	} {
		found := false
		for _, candidate := range missing {
			found = found || candidate == record
		}
		if !found {
			t.Fatalf("census did not report %s; unclassified: %v", record, missing)
		}
	}
}
