package cypher

import (
 "go/ast"
 "go/parser"
 "go/token"
 "os"
 "strings"
 "testing"
)

// Enumerate all production callers, rather than keeping a hand-built consumer
// list. The row-similarity feature's separate key domain is the only deferral.
func TestGraphEntityIdentityReaderBoundary(t *testing.T) {
 entries, err := os.ReadDir("."); if err != nil { t.Fatal(err) }
 for _, entry := range entries {
  name := entry.Name()
  if !strings.HasSuffix(name,".go") || strings.HasSuffix(name,"_test.go") { continue }
  file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0); if err != nil { t.Fatal(err) }
  for _, decl := range file.Decls {
   fn, ok := decl.(*ast.FuncDecl); if !ok { continue }
   ast.Inspect(fn, func(node ast.Node) bool {
    call, ok := node.(*ast.CallExpr); if !ok { return true }
    id, ok := call.Fun.(*ast.Ident); if !ok { return true }
    if id.Name == "linkPredictionNodeID" || id.Name == "gdsNodeID" && fn.Name.Name != "similarityNodeID" {
     t.Errorf("%s: %s calls bare-local %s; resolve through the graph identity owner", name, fn.Name.Name, id.Name)
    }
    return true
   })
  }
 }
}