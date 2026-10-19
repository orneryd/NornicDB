package storage

import (
 "go/ast"
 "go/build"
 "go/importer"
 "go/parser"
 "go/token"
 "go/types"
 "io/fs"
 "os"
 "path/filepath"
 "sort"
 "strings"
 "testing"
)

type lifecycleGuardFunction struct {
 name, path string
 decl *ast.FuncDecl
 calls []*lifecycleGuardFunction
 raw, lease, callback, constructed, async bool
}

type lifecycleGuardSource struct {
 functions []*lifecycleGuardFunction
 info *types.Info
 objects map[*types.Func]*lifecycleGuardFunction
}

// Resolve local method identities rather than joining unrelated methods by name.
// External export data is optional: this contract follows the storage package's
// physical commit primitives, whose declarations and receiver types are local.
func lifecycleGuardParse(t *testing.T, root string) *lifecycleGuardSource {
 t.Helper()
 fset := token.NewFileSet()
 var files []*ast.File
 paths := map[*ast.File]string{}
 err := filepath.WalkDir(root,func(path string, entry fs.DirEntry, err error) error {
  if err != nil { return err }
  if entry.IsDir() && filepath.Clean(path) != filepath.Clean(root) { return fs.SkipDir }; if entry.IsDir() || !strings.HasSuffix(path,".go") || strings.HasSuffix(path,"_test.go") { return nil }
  matched,err:=build.Default.MatchFile(filepath.Dir(path),filepath.Base(path))
  if err!=nil{return err};if !matched{return nil}
  file,err := parser.ParseFile(fset,path,nil,0)
  if err != nil { return err }
  files=append(files,file); paths[file]=path
  return nil
 })
 if err != nil { t.Fatal(err) }
 source := &lifecycleGuardSource{info:&types.Info{Defs:map[*ast.Ident]types.Object{},Uses:map[*ast.Ident]types.Object{},Selections:map[*ast.SelectorExpr]*types.Selection{},Types:map[ast.Expr]types.TypeAndValue{}},objects:map[*types.Func]*lifecycleGuardFunction{}}
 config:=types.Config{Importer:importer.Default(),Error:func(error){}}
 _,_ = config.Check("github.com/orneryd/nornicdb/pkg/storage",fset,files,source.info)
 for _,file:=range files { for _,decl:=range file.Decls {
  fn,ok:=decl.(*ast.FuncDecl); if !ok || fn.Body==nil { continue }
  item:=&lifecycleGuardFunction{name:fn.Name.Name,path:paths[file],decl:fn}
  if fn.Type.Results!=nil { for _,result:=range fn.Type.Results.List { if _,ok:=result.Type.(*ast.FuncType);ok { item.lease=true } } }
  for _,param:=range fn.Type.Params.List { if _,ok:=param.Type.(*ast.FuncType);ok { item.callback=true } }
  object,_:=source.info.Defs[fn.Name].(*types.Func)
  if object==nil { t.Fatalf("cannot resolve local function %s:%s",item.path,item.name) }
  source.objects[object]=item; source.functions=append(source.functions,item)
 } }
 for _,file:=range files {
  ast.Inspect(file,func(node ast.Node)bool {
   statement,ok:=node.(*ast.GoStmt);if !ok{return true}
   body:=&ast.BlockStmt{List:[]ast.Stmt{&ast.ExprStmt{X:statement.Call}}}
   if literal,ok:=statement.Call.Fun.(*ast.FuncLit);ok {body=literal.Body}
   source.functions=append(source.functions,&lifecycleGuardFunction{name:"asynchronous mutation entry",path:paths[file],async:true,decl:&ast.FuncDecl{Body:body}})
   return true
  })
 }
 return source
}

func (source *lifecycleGuardSource) target(call *ast.CallExpr) *lifecycleGuardFunction {
 var object types.Object
 switch fun:=call.Fun.(type) {
 case *ast.Ident: object=source.info.Uses[fun]
 case *ast.SelectorExpr:
  if selection:=source.info.Selections[fun];selection!=nil { object=selection.Obj() } else { object=source.info.Uses[fun.Sel] }
 }
 fn,_:=object.(*types.Func)
 return source.objects[fn]
}

func lifecycleGuardCallName(call *ast.CallExpr) string {
 switch fun:=call.Fun.(type) { case *ast.Ident:return fun.Name;case *ast.SelectorExpr:return fun.Sel.Name }
 return ""
}

func (source *lifecycleGuardSource) raw(call *ast.CallExpr) bool {
 if target:=source.target(call);target!=nil {
  // Physical commit primitives, never consumer names.
  switch target.name { case "withUpdate","withNotifyingUpdate","withUpdateUnits","commitEngineWrite","commitEngineWriteHeld","newCommitWriter": return true }
 }
 selector,ok:=call.Fun.(*ast.SelectorExpr);if !ok{return false}
 name:=selector.Sel.Name
 receiver:=types.TypeString(source.info.TypeOf(selector.X),func(p *types.Package)string{return p.Path()})
 if strings.Contains(receiver,"managedBadgerDB") { switch name {case "Update","DropPrefix","DropAll","Load":return true} }
 if strings.Contains(receiver,"dgraph-io/badger") {switch name {case "Set","Delete","SetEntry","Commit","DropPrefix","DropAll","Load":return true} }
 return false
}

func (source *lifecycleGuardSource) bypasses() []string {
 owner:=map[*lifecycleGuardFunction]bool{}
 for _,fn:=range source.functions { if fn.name=="beginNamespaceMutation" { owner[fn]=true } }
 // Only factories returning a lease or executing a callback can confer it on
 // a caller. An ordinary admitted mutation never blesses a sibling raw write.
 for changed:=true;changed; {
  changed=false
  for _,fn:=range source.functions {
   if owner[fn] || (!fn.lease && !fn.callback) {continue}
   ast.Inspect(fn.decl.Body,func(node ast.Node)bool {if _,ok:=node.(*ast.GoStmt);ok{return false};if call,ok:=node.(*ast.CallExpr);ok && owner[source.target(call)] {owner[fn]=true;changed=true};return true})
  }
 }
 for _,fn:=range source.functions {
  fn.raw=false;fn.calls=nil
  ast.Inspect(fn.decl.Body,func(node ast.Node)bool {
   if _,ok:=node.(*ast.GoStmt);ok{return false}
   call,ok:=node.(*ast.CallExpr);if !ok{return true}
   target:=source.target(call)
   if owner[target] && target.callback {return false}
   if source.raw(call) {fn.raw=true}
   if target!=nil {fn.calls=append(fn.calls,target)}
   return true
  })
 }
 var visit func(*lifecycleGuardFunction,map[*lifecycleGuardFunction]bool) bool
 visit=func(fn *lifecycleGuardFunction,seen map[*lifecycleGuardFunction]bool) bool {
  if seen[fn] {return false};seen[fn]=true
  for _,call:=range fn.calls {if owner[call] && call.lease {return false}}
  if fn.raw{return true}
  for _,call:=range fn.calls {if visit(call,seen){return true}}
  return false
 }
 var violations []string
 for _,fn:=range source.functions {
    if !fn.async && !ast.IsExported(fn.name) {continue}
  if !fn.async && fn.decl.Recv != nil {
   receiver:=fn.decl.Recv.List[0].Type
   if star,ok:=receiver.(*ast.StarExpr);ok{receiver=star.X}
   if name,ok:=receiver.(*ast.Ident);ok && !ast.IsExported(name.Name){continue}
  }
  // A factory can initialize only a newly allocated engine before publication.
  // No existing-engine argument or method receiver may use this construction rule.
  constructed:=!fn.async && fn.decl.Recv==nil
  if constructed {for _,param:=range fn.decl.Type.Params.List {
   ast.Inspect(param.Type,func(n ast.Node)bool {if id,ok:=n.(*ast.Ident);ok && strings.Contains(id.Name,"Engine"){constructed=false};return true})
  }}
  allocated:=false
  if constructed {ast.Inspect(fn.decl.Body,func(n ast.Node)bool {if lit,ok:=n.(*ast.CompositeLit);ok {if id,ok:=lit.Type.(*ast.Ident);ok && id.Name=="BadgerEngine" {allocated=true}};return true})}
  if constructed && allocated {continue}
  if visit(fn,map[*lifecycleGuardFunction]bool{}) {violations=append(violations,fn.path+":"+fn.name)}
 }
 sort.Strings(violations)
 return violations
}

func TestLifecycleMutationAdmissionStructure(t *testing.T) {
 source:=lifecycleGuardParse(t,".")
 for _,violation:=range source.bypasses(){t.Error("mutation reaches a physical sink without lifecycle admission:",violation)}
}

func lifecycleGuardContextViolations(root string) ([]string,error) {
 var violations []string
 err:=filepath.WalkDir(root,func(path string,entry fs.DirEntry,err error)error{
  if err!=nil{return err};if entry.IsDir() || !strings.HasSuffix(path,".go") || strings.HasSuffix(path,"_test.go"){return nil}
  file,err:=parser.ParseFile(token.NewFileSet(),path,nil,0);if err!=nil{return err}
  ast.Inspect(file,func(node ast.Node)bool {
   // References also cover method values passed to another function or aliased
   // before invocation; an async consumer remains part of the boundary.
   name:=""
   switch n:=node.(type) {
   case *ast.SelectorExpr:name=n.Sel.Name
   case *ast.CallExpr:if id,ok:=n.Fun.(*ast.Ident);ok{name=id.Name}
   }
   if (strings.HasPrefix(name,"Create") || strings.HasPrefix(name,"Drop")) && strings.HasSuffix(name,"Database") {violations=append(violations,path+": context-free lifecycle "+name)}
   return true
  });return nil
 })
 sort.Strings(violations);return violations,err
}
func TestLifecycleDDLContextStructure(t *testing.T) {
 violations,err:=lifecycleGuardContextViolations("..");if err!=nil{t.Fatal(err)}
 for _,violation:=range violations{t.Error(violation)}
}

func TestLifecycleMutationGuardRejectsNewEntry(t *testing.T) {
 root:=t.TempDir()
 // Names are deliberately novel; no consumer allowlist participates.
 const fixture=`package storage
 type BadgerEngine struct{}
 func (b *BadgerEngine) withUpdate() error {return nil}
 func (b *BadgerEngine) beginNamespaceMutation() (func(),error) {return func(){},nil}
 func (b *BadgerEngine) OrdinaryAdmittedMutation() error {release,_:=b.beginNamespaceMutation();defer release();return b.withUpdate()}
 func (b *BadgerEngine) NewlyAddedMutation() error {return b.withUpdate()}
 func (b *BadgerEngine) NewlyAddedSiblingBypass() error {b.OrdinaryAdmittedMutation();return b.withUpdate()}
 func (b *BadgerEngine) launchBackground() {go func(){b.withUpdate()}()}
 `
 if err:=os.WriteFile(filepath.Join(root,"new.go"),[]byte(fixture),0600);err!=nil{t.Fatal(err)}
 source:=lifecycleGuardParse(t,root)
 got:=source.bypasses()
 if len(got)!=3{t.Fatal("new mutation populations escaped structural guard",got)}
 if err:=os.Mkdir(filepath.Join(root,"ingress"),0700);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(root,"ingress","consumer.go"),[]byte("package storage;func NewlyAddedHandler(m any){go func(){ drop:=m.DropDatabase; drop(\"x\") }()}"),0600);err!=nil{t.Fatal(err)}
 violations,err:=lifecycleGuardContextViolations(root);if err!=nil{t.Fatal(err)};if len(violations)!=1{t.Fatal("new context-free consumer escaped guard",violations)}
}