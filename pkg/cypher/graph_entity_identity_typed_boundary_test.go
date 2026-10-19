package cypher

import (
    "bytes"
    "context"
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
    "runtime"
    "sort"
    "testing"
    "time"
)

// The compiler supplies the consumer population, rather than procedure/file
// names or parameter spelling. New readers, aliases, inferred local values,
// embedded fields and function-valued references all use the same type graph.
// An unresolved import/type is an incomplete inventory and fails closed.
func TestGraphEntityIdentityTypedBoundary(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
    defer cancel()
    command := exec.CommandContext(ctx, "go", "list", "-export", "-deps", "-json", ".")
    var diagnostics bytes.Buffer
    command.Stderr = &diagnostics
    output, err := command.Output()
    if err != nil { t.Fatalf("identity type inventory: %v\n%s", err, diagnostics.String()) }
    type listedPackage struct {
        ImportPath, Dir, Export string
        GoFiles, CgoFiles []string
    }
    exports := map[string]string{}
    var current listedPackage
    decoder := json.NewDecoder(bytes.NewReader(output))
    directory, err := os.Getwd()
    if err != nil { t.Fatal(err) }
    for decoder.More() {
        var listed listedPackage
        if err := decoder.Decode(&listed); err != nil { t.Fatal(err) }
        exports[listed.ImportPath] = listed.Export
        if filepath.Clean(listed.Dir) == filepath.Clean(directory) { current = listed }
    }
    if current.ImportPath == "" || len(current.GoFiles) == 0 { t.Fatal("identity inventory omitted the current production package") }
    positions := token.NewFileSet()
    var files []*ast.File
    for _, name := range append(current.GoFiles, current.CgoFiles...) {
        file, err := parser.ParseFile(positions, name, nil, 0)
        if err != nil { t.Fatal(err) }
        files = append(files, file)
    }
    // A fresh reader with an unrelated name, an alias and inferred locals must
    // be caught by the same compiler graph. It exists only in this AST fixture,
    // never in production source or the executable test package.
    const negativeName = "identity-boundary-negative-control.generated"
    negative,err:=parser.ParseFile(positions,negativeName,
        "package cypher\nimport storage \"github.com/orneryd/nornicdb/pkg/storage\"\n"+
        "type identityBoundaryAlias = storage.Node\n"+
        "func unrelatedIdentityReader(input *identityBoundaryAlias, relationship *storage.Edge) []string {\n"+
        "candidate := input; inferred := relationship; candidate.Database = inferred.Database\n"+
        "return []string{string(candidate.ID), string(inferred.ID), string(inferred.StartNode), string(inferred.EndNode)}\n}\n",0)
    if err!=nil {t.Fatal(err)}
    files=append(files,negative)
    info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}}
    configuration := types.Config{
        Importer: importer.ForCompiler(positions, "gc", func(path string) (io.ReadCloser, error) {
            export := exports[path]
            if export == "" { return nil, fmt.Errorf("identity inventory lacks compiler export for %s", path) }
            return os.Open(export)
        }),
        Sizes: types.SizesFor("gc", runtime.GOARCH),
    }
    if _, err := configuration.Check(current.ImportPath, positions, files, info); err != nil {
        t.Fatalf("incomplete identity consumer type graph: %v", err)
    }
    native := func(typ types.Type) bool {
        typ = types.Unalias(typ)
        if pointer, ok := typ.(*types.Pointer); ok { typ = types.Unalias(pointer.Elem()) }
        named, ok := typ.(*types.Named)
        if !ok || named.Obj().Pkg() == nil { return false }
        return named.Obj().Pkg().Path() == "github.com/orneryd/nornicdb/pkg/storage" && (named.Obj().Name() == "Node" || named.Obj().Name() == "Edge")
    }
    var findings []string
    negativeFindings:=0
    for _, file := range files {
        source := positions.Position(file.Pos()).Filename
        if filepath.Base(source) == "graph_entity_identity.go" { continue }
        parents := map[ast.Node]ast.Node{}
        var stack []ast.Node
        ast.Inspect(file, func(node ast.Node) bool {
            if node == nil { stack = stack[:len(stack)-1]; return true }
            if len(stack) > 0 { parents[node] = stack[len(stack)-1] }
            stack = append(stack, node)
            return true
        })
        for selector, selection := range info.Selections {
            if selector.Pos() < file.Pos() || selector.End() > file.End() || selection.Kind() != types.FieldVal || !native(selection.Recv()) { continue }
            field := selection.Obj().Name()
            if field != "ID" && field != "Database" && field != "StartNode" && field != "EndNode" { continue }
            owner := "<initializer>"
            for parent := parents[selector]; parent != nil; parent = parents[parent] {
                if function, ok := parent.(*ast.FuncDecl); ok { owner = function.Name.Name; break }
            }
            // The sole deferred row-similarity adapter is the accepted key-domain
            // exception. Its projection/filter siblings receive no exemption.
            if filepath.Base(source) == "gds_node_similarity_rows.go" && owner == "similarityNodeID" { continue }
            if source==negativeName {negativeFindings++;continue}
            findings = append(findings, fmt.Sprintf("%s: %s reads native %s.%s outside identity owner", positions.Position(selector.Pos()), owner, selection.Recv(), field))
        }
    }
    sort.Strings(findings)
    if negativeFindings!=6 {t.Fatalf("typed identity guard missed its fresh aliased/inferred reader or provenance write: caught %d/6 native identity field accesses",negativeFindings)}
    t.Log("negative control: all 6 native identity accesses caught, including aliased/inferred readers and provenance writes")
    for _, finding := range findings { t.Log(finding) }
    if len(findings) != 0 { t.Fatalf("%d native identity consumers still bypass the owner", len(findings)) }
    t.Logf("complete compiler inventory: %d production files, no native identity owner bypasses", len(files)-1)
}
