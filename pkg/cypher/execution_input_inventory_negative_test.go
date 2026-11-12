package cypher

import (
	"github.com/stretchr/testify/require"
	"go/ast"
	"os"
	"path/filepath"
	"testing"
)

// Reusing the same method with a data argument and then a query argument must
// not let the data-only visit hide the executable one.
func TestExecutionInputInventoryPreservesDistinctBindings(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(`package cypher
 type StorageExecutor struct{}
 func(e *StorageExecutor) executeUserStatement(){}
 func(e *StorageExecutor) start(){e.read(0);e.read(1)}
 func(e *StorageExecutor) read(scope int){if scope==1{e.executeUserStatement()}}
 `), 0600))
	source, err := loadInventorySource(dir)
	require.NoError(t, err)
	require.True(t, source.reachesText(inventoryCall{name: "start"}, -1, nil, map[string]bool{}), "a data-only visit cannot mask the later executable-text binding")
}

func TestExecutionInputInventoryDoesNotTrustMutableBindings(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"parameter reassignment", `func(e *StorageExecutor) start(){e.read(0)}
   func(e *StorageExecutor) read(scope int){scope=1;if scope==1{e.executeUserStatement()}}`},
		{"local constant shadows package constant", `const queryScope=1
   func(e *StorageExecutor) start(){e.read(0)}
   func(e *StorageExecutor) read(scope int){const queryScope=0;if scope==queryScope{e.executeUserStatement()}}`},
		{"unknown parameter shadows package constant", `const scope=0
 func value()int{return 1}
 func(e *StorageExecutor) start(){e.read(value())}
 func(e *StorageExecutor) read(scope int){e.deep(scope)}
 func(e *StorageExecutor) deep(mode int){if mode==1{e.executeUserStatement()}}`},
		{"recursive method with new binding", `func(e *StorageExecutor) start(){e.read(0)}
   func(e *StorageExecutor) read(scope int){if scope==0{e.read(1)}else{e.executeUserStatement()}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			text := "package cypher\ntype StorageExecutor struct{}\nfunc(e *StorageExecutor) executeUserStatement(){}\n" + tc.body
			require.NoError(t, os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(text), 0600))
			source, err := loadInventorySource(dir)
			require.NoError(t, err)
			require.True(t, source.reachesText(inventoryCall{name: "start"}, -1, nil, map[string]bool{}))
		})
	}
}

func TestExecutionInputInventoryFactoryBindingProvenance(t *testing.T) {
	for _, tc := range []struct {
		name, globalWrite, factoryWrite string
		canonical, want                 bool
		argument, spec                  string
	}{
		{name: "immutable data", argument: "data"},
		{name: "global changed before registration", globalWrite: "data.scope=1;", argument: "data", want: true},
		{name: "factory changes captured value", factoryWrite: "d.scope=1;", argument: "data", want: true},
		{name: "canonical graph declaration", canonical: true, argument: "graph"},
		{name: "canonical query declaration", canonical: true, argument: "query", want: true},
		{name: "canonical global mutation", canonical: true, argument: "graph", globalWrite: "graph.scope=queryScope;", want: true},
		{name: "canonical factory mutation", canonical: true, argument: "graph", factoryWrite: "d.scope=queryScope;", want: true},
		{name: "canonical unknown name", canonical: true, argument: "graph", spec: `ProcedureSpec{Name:runtimeName()}`, want: true},
		{name: "canonical unknown declaration", canonical: true, argument: "runtimeDeclaration()", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			registration := `registerBuiltInProcedure("fixture",factory(` + tc.argument + `))`
			if tc.canonical {
				spec := tc.spec
				if spec == "" {
					spec = `ProcedureSpec{Name:"fixture"}`
				}
				registration = `registerProcedure(` + spec + `,factory(` + tc.argument + `),policy)`
			}
			text := `package cypher
 type StorageExecutor struct{}
 func(e *StorageExecutor) executeUserStatement(){}
 type declaration struct{scope int}
 type ProcedureSpec struct{Name string}
 const ( graphScope int=iota; queryScope )
 const policy=0
 var data=declaration{scope:0}
 var graph=declaration{scope:graphScope}
 var query=declaration{scope:queryScope}
 func runtimeName()string{return "fixture"}
 func runtimeDeclaration()declaration{return query}
 func registerBuiltInProcedure(name string,handler func(*StorageExecutor)){}
 func registerProcedure(spec ProcedureSpec,handler func(*StorageExecutor),policy int){}
 func factory(d declaration)func(*StorageExecutor){` + tc.factoryWrite + `return func(e *StorageExecutor){e.read(d.scope)}}
 func(e *StorageExecutor)read(scope int){if scope==queryScope{e.executeUserStatement()}}
 func init(){` + tc.globalWrite + registration + `}
 `
			require.NoError(t, os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(text), 0600))
			source, err := loadInventorySource(dir)
			require.NoError(t, err)
			var handler *ast.FuncLit
			ast.Inspect(source.functions["factory"].Body, func(node ast.Node) bool {
				if value, ok := node.(*ast.FuncLit); ok {
					handler = value
				}
				return true
			})
			require.NotNil(t, handler)
			frame := source.registrationFrame("fixture", handler)
			executable := false
			for _, call := range source.calls(handler.Body, "e", frame) {
				if source.reachesText(call, -1, nil, map[string]bool{}) {
					executable = true
				}
			}
			require.Equal(t, tc.want, executable)
		})
	}
}
