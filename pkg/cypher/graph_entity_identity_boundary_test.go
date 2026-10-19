package cypher

import (
 "go/ast"
 "go/parser"
 "go/token"
 "os"
 "strings"
 "testing"
)

// Inspect every production reference, including function-value aliases and
// package initializers. A newly added caller cannot silently escape the owner.
func TestGraphEntityIdentityReaderBoundary(t *testing.T) {
 entries, err := os.ReadDir("."); if err != nil { t.Fatal(err) }
 for _, entry := range entries {
  name := entry.Name()
  if !strings.HasSuffix(name,".go") || strings.HasSuffix(name,"_test.go") { continue }
  file, err := parser.ParseFile(token.NewFileSet(),name,nil,0); if err != nil { t.Fatal(err) }
  allowed := make(map[token.Pos]bool)
  var deferred *ast.BlockStmt
  for _, decl := range file.Decls {
   fn,ok := decl.(*ast.FuncDecl); if !ok { continue }
   if name == "graph_entity_identity.go" && fn.Name.Name == "gdsNodeID" { allowed[fn.Name.Pos()] = true }
   if name == "gds_node_similarity_rows.go" && fn.Name.Name == "similarityNodeID" { deferred = fn.Body }
  }
  ast.Inspect(file,func(node ast.Node) bool {
   id,ok := node.(*ast.Ident); if !ok || id.Name != "gdsNodeID" && id.Name != "linkPredictionNodeID" { return true }
   if allowed[id.Pos()] { return true }
   // Only row similarity's separately deferred key-domain adapter may use
   // the legacy reader. Projection filters are expressly outside that gap.
   if id.Name == "gdsNodeID" && deferred != nil && deferred.Pos() <= id.Pos() && id.End() <= deferred.End() { return true }
   t.Errorf("%s: bare-local %s reference outside identity owner/deferred similarity adapter",name,id.Name)
   return true
  })
 }
}