package cypher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

var relationshipProducerMetadata struct {
	once  sync.Once
	files []string
	err   error
}

// Use the compiler's actual production population, not a handpicked directory
// list. Fixtures, tests and inactive build-tag files are not production files.
func relationshipProducerFiles(t *testing.T) []string {
	t.Helper()
	relationshipProducerMetadata.once.Do(func() {
		output, err := exec.Command("go", "list", "-json", "../...", "../../internal/cyphertext").Output()
		if err != nil {
			relationshipProducerMetadata.err = err
			return
		}
		decoder := json.NewDecoder(bytes.NewReader(output))
		for {
			var pkg struct {
				Dir               string
				GoFiles, CgoFiles []string
			}
			if err := decoder.Decode(&pkg); err == io.EOF {
				break
			} else if err != nil {
				relationshipProducerMetadata.err = err
				return
			}
			for _, name := range append(pkg.GoFiles, pkg.CgoFiles...) {
				relationshipProducerMetadata.files = append(relationshipProducerMetadata.files, filepath.Join(pkg.Dir, name))
			}
		}
		if len(relationshipProducerMetadata.files) == 0 {
			relationshipProducerMetadata.err = fmt.Errorf("empty compiler producer population")
		}
	})
	if relationshipProducerMetadata.err != nil {
		t.Fatal(relationshipProducerMetadata.err)
	}
	return relationshipProducerMetadata.files
}

// Both parsers and storage consume the shared owner's phases. Canonical
// factories/representations cannot be imported by an arbitrary producer or
// used to erase provenance behind a nominal type or an aliased function.
func relationshipCrossPackageViolations(path, source string) []string {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, 0)
	if err != nil {
		return []string{err.Error()}
	}
	imports := map[string]string{}
	for _, value := range file.Imports {
		name, _ := strconv.Unquote(value.Path.Value)
		alias := filepath.Base(name)
		if value.Name != nil {
			alias = value.Name.Name
		}
		if alias == "." && (name == "github.com/orneryd/nornicdb/internal/cyphertext" || name == "github.com/orneryd/nornicdb/pkg/storage") {
			return []string{"dot import hides the schema-fact owner identity"}
		}
		imports[alias] = name
	}
	var failures []string
	for _, declaration := range file.Decls {
		function, _ := declaration.(*ast.FuncDecl)
		ast.Inspect(declaration, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			alias, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch imports[alias.Name] {
			case "github.com/orneryd/nornicdb/internal/cyphertext":
				if selector.Sel.Name == "RelationshipType" || selector.Sel.Name == "RelationshipTypes" || selector.Sel.Name == "ConnectionFrame" || selector.Sel.Name == "ConnectionPrefix" {
					ownerFile := filepath.ToSlash(path) == "cypher/relationship_declaration.go"
					if !ownerFile {
						failures = append(failures, fset.Position(selector.Pos()).String()+": shared relationship representation cannot erase consumer provenance")
					}
					return true
				}
				roles := map[string]map[string]string{
					"SplitConstraintEntries":     {"cypher/schema_contracts.go": "splitConstraintContractEntries"},
					"ReadRelationshipTypes":      {"cypher/relationship_declaration.go": "relationshipDeclarationOf"},
					"ReadConnectionFrame":        {"cypher/relationship_declaration.go": "readRelationshipFrame"},
					"ReadConnectionPrefix":       {"cypher/relationship_declaration.go": "relationshipPrefixAfterNode"},
					"CompileConstraintPattern":   {"cypher/antlr/constraint_pattern.go": "ParseConstraintPattern", "storage/constraint_contracts.go": "parseConstraintPattern"},
					"EachChainItem":              {"cypher/label_chain.go": "eachChainItem"},
					"SplitNodeHead":              {"cypher/label_chain.go": "splitNodeHead"},
					"StrictNodeHeadLabels":       {"cypher/label_chain.go": "strictNodeHeadLabels"},
					"ScanQuotedName":             {"cypher/label_chain.go": "scanQuotedName"},
					"IndexByteOutsideBackticks":  {"cypher/label_chain.go": "indexByteOutsideBackticks"},
					"IndexByteOutsideQuotedText": {"cypher/label_chain.go": "indexByteOutsideBackticks"},
				}
				if owners, factory := roles[selector.Sel.Name]; factory {
					role := owners[filepath.ToSlash(path)]
					if function == nil || role == "" || function.Name.Name != role {
						failures = append(failures, fset.Position(selector.Pos()).String()+": shared schema factory is reserved for its canonical adapter")
					}
				}
			case "github.com/orneryd/nornicdb/pkg/storage":
				// Product code has no alternate producer of storage routing hints.
				// External clients keep their explicit resolved-data protocol. A
				// new internal adapter must integrate an owned parsed plan first.
				if selector.Sel.Name == "QueryInfo" || selector.Sel.Name == "QueryAnalyzer" || selector.Sel.Name == "NewQueryAnalyzer" {
					failures = append(failures, fset.Position(selector.Pos()).String()+": internal routing metadata must consume an owned parsed plan")
				}
			}
			return true
		})
	}
	return failures
}

func TestRelationshipOwnerCrossPackageGuard(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range relationshipProducerFiles(t) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		for _, failure := range relationshipCrossPackageViolations(relative, string(data)) {
			t.Error(failure)
		}
	}
}

func TestRelationshipOwnerCrossPackageGuardMutations(t *testing.T) {
	for _, source := range []string{
		`package consumer; import s "github.com/orneryd/nornicdb/internal/cyphertext";var read=s.SplitConstraintEntries`,
		`package consumer; import s "github.com/orneryd/nornicdb/internal/cyphertext";func bad(q string){s.ReadRelationshipTypes(q)}`,
		`package consumer; import s "github.com/orneryd/nornicdb/internal/cyphertext";var read=s.ReadRelationshipTypes`,
		`package consumer; import s "github.com/orneryd/nornicdb/internal/cyphertext";var read=s.ReadConnectionFrame`,
		`package consumer; import s "github.com/orneryd/nornicdb/internal/cyphertext";var read=s.ReadConnectionPrefix`,
		`package consumer; import s "github.com/orneryd/nornicdb/internal/cyphertext";type erased=s.ConnectionPrefix`,
		`package consumer; import s "github.com/orneryd/nornicdb/internal/cyphertext";func bad(q string){s.CompileConstraintPattern(q)}`,
		`package consumer; import s "github.com/orneryd/nornicdb/internal/cyphertext";var read=s.EachChainItem`,
		`package consumer; import s "github.com/orneryd/nornicdb/internal/cyphertext";func erased(t relationshipTypeFacts){_ = s.RelationshipType(t).StorageName()}`,
		`package consumer; import . "github.com/orneryd/nornicdb/internal/cyphertext";func bad(q string){ReadRelationshipTypes(q)}`,
		`package consumer; import s "github.com/orneryd/nornicdb/pkg/storage";func renamed(q string)*s.QueryInfo{return &s.QueryInfo{Labels:[]string{q}}}`,
		`package consumer; import s "github.com/orneryd/nornicdb/pkg/storage";type erased = s.QueryInfo`,
		`package consumer; import s "github.com/orneryd/nornicdb/pkg/storage";func bad(q string){info:=s.NewQueryAnalyzer().AnalyzeQuery(q);info.Labels=[]string{q}}`,
	} {
		if failures := relationshipCrossPackageViolations("future/consumer.go", source); len(failures) == 0 {
			t.Fatalf("cross-package mutation escaped: %s", source)
		}
	}
}
