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
// Guard the causal boundary as well as known symbol references: GDS value
// readers may not decode an entity by selecting its bare ID. All function
// contexts that carry a graph executor must also carry its database selection.
func TestGraphEntityIdentityInputAndContextOwners(t *testing.T) {
 entries,err := os.ReadDir("."); if err != nil { t.Fatal(err) }
 for _,entry := range entries {
  name:=entry.Name()
  if !strings.HasSuffix(name,".go") || strings.HasSuffix(name,"_test.go") { continue }
  file,err:=parser.ParseFile(token.NewFileSet(),name,nil,0); if err != nil { t.Fatal(err) }
  ast.Inspect(file,func(node ast.Node) bool {
   literal,ok:=node.(*ast.CompositeLit); if !ok { return true }
   fields:=make(map[string]ast.Expr)
   for _,element:=range literal.Elts { if field,ok:=element.(*ast.KeyValueExpr); ok { if key,ok:=field.Key.(*ast.Ident); ok { fields[key.Name]=field.Value } } }
   if selection,ok:=literal.Type.(*ast.SelectorExpr); ok && selection.Sel.Name=="Context" && fields["Graphs"]!=nil && fields["Storage"]!=nil && fields["Database"]==nil {
    t.Errorf("%s: graph function context omits the selected database",name)
   }
   if typ,ok:=literal.Type.(*ast.Ident); ok && typ.Name=="StorageExecutor" {
    if manager,ok:=fields["dbManager"].(*ast.SelectorExpr); ok && manager.Sel.Name=="dbManager" && fields["graphIdentityScope"]==nil { t.Errorf("%s: executor clone drops its identity scope",name) }
   }
   return true
  })
  if !strings.HasPrefix(name,"gds_") { continue }
  for _,decl:=range file.Decls {
   function,ok:=decl.(*ast.FuncDecl); if !ok || function.Name.Name=="similarityNodeID" { continue }
   readsValue:=false
   if function.Type.Params!=nil { for _,param:=range function.Type.Params.List { if _,ok:=param.Type.(*ast.InterfaceType); ok { readsValue=true }; if typ,ok:=param.Type.(*ast.Ident); ok && typ.Name=="any" { readsValue=true } } }
   if !readsValue { continue }
   ast.Inspect(function.Body,func(node ast.Node) bool {
    if selector,ok:=node.(*ast.SelectorExpr); ok && selector.Sel.Name=="ID" { t.Errorf("%s: value reader %s bypasses the identity owner with .ID",name,function.Name.Name) }
    return true
   })
  }
 }
}
