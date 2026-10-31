package storagecensus

import (
	"encoding/json"
	"fmt"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). The signature denominator is a
// plain go/types over-approximation, including APIs that never retain data.
// It uses the existing loader; it does not perform argument-flow analysis.
func TestResidualReferenceBoundaryPopulation(t *testing.T) {
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	path := filepath.Join(abs, "internal", "testfixture", "referenceboundary", "zz_residual_reference.go")
	const plant = `package referenceboundary
import "github.com/orneryd/nornicdb/pkg/storage"
type Delegate struct { storage.Engine }
func NewDelegate(engine storage.Engine) *Delegate { return &Delegate{Engine: engine} }
func (d *Delegate) Reference(node *storage.Node) {}
func (d *Delegate) Scalar(value int) {}
`
	p, err := load(abs, map[string][]byte{path: []byte(plant)})
	require.NoError(t, err)
	packages := map[string]*types.Package{}
	for _, fn := range p.funcs {
		if fn.Pkg != nil {
			packages[fn.Pkg.Pkg.Path()] = fn.Pkg.Pkg
		}
	}
	engine := packages[Module+"pkg/storage"].Scope().Lookup("Engine").Type().Underlying().(*types.Interface)
	var reference func(types.Type, map[types.Type]bool) bool
	reference = func(typ types.Type, seen map[types.Type]bool) bool {
		typ = types.Unalias(typ)
		if seen[typ] {
			return false
		}
		seen[typ] = true
		switch typ := typ.Underlying().(type) {
		case *types.Pointer, *types.Map, *types.Slice, *types.Interface, *types.Signature, *types.Chan:
			return true
		case *types.Array:
			return reference(typ.Elem(), seen)
		case *types.Struct:
			for i := 0; i < typ.NumFields(); i++ {
				if reference(typ.Field(i).Type(), seen) {
					return true
				}
			}
		}
		return false
	}
	delegate := func(typ types.Type) bool {
		if types.Implements(typ, engine) {
			return true
		}
		typ = types.Unalias(typ)
		if ptr, ok := typ.(*types.Pointer); ok {
			typ = types.Unalias(ptr.Elem())
		}
		if named, ok := typ.(*types.Named); ok && named.Obj().Pkg() != nil {
			return named.Obj().Pkg().Path() == Module+"pkg/storage" && (named.Obj().Name() == "BadgerTransaction" || named.Obj().Name() == "Node" || named.Obj().Name() == "Edge" || named.Obj().Name() == "Operation" || named.Obj().Name() == "PreparedEntityCommit")
		}
		return false
	}
	type row struct{ Boundary, Argument, Type, Position string }
	var rows []row
	seen := map[*types.Func]bool{}
	positive, negative := false, false
	visit := func(obj *types.Func) {
		if obj == nil || !obj.Exported() || seen[obj] {
			return
		}
		seen[obj] = true
		sig := obj.Type().(*types.Signature)
		pkg := obj.Pkg().Path()
		selected := pkg == Module+"pkg/storage" || pkg == Module+"pkg/replication"
		name := strings.TrimPrefix(pkg, Module) + "."
		if recv := sig.Recv(); recv != nil {
			selected = selected || delegate(recv.Type())
			typ := types.Unalias(recv.Type())
			if ptr, ok := typ.(*types.Pointer); ok {
				typ = types.Unalias(ptr.Elem())
			}
			if named, ok := typ.(*types.Named); ok {
				name += named.Obj().Name() + "."
			}
		}
		for i := 0; i < sig.Params().Len(); i++ {
			selected = selected || delegate(sig.Params().At(i).Type())
		}
		name += obj.Name()
		if !selected {
			return
		}
		for i := 0; i < sig.Params().Len(); i++ {
			arg := sig.Params().At(i)
			if !reference(arg.Type(), map[types.Type]bool{}) {
				continue
			}
			if strings.HasPrefix(pkg, Module+"internal/testfixture/referenceboundary") {
				positive = positive || obj.Name() == "Reference"
				negative = negative || obj.Name() == "Scalar"
			} else {
				rows = append(rows, row{name, arg.Name(), arg.Type().String(), p.pos(obj.Pos())})
			}
		}
	}
	for pkgPath, pkg := range packages {
		if !strings.HasPrefix(pkgPath, Module) || strings.HasPrefix(pkgPath, Module+"testing/") {
			continue
		}
		for _, name := range pkg.Scope().Names() {
			switch obj := pkg.Scope().Lookup(name).(type) {
			case *types.Func:
				visit(obj)
			case *types.TypeName:
				if named, ok := types.Unalias(obj.Type()).(*types.Named); ok {
					if _, abstract := named.Underlying().(*types.Interface); abstract {
						continue
					}
					for i := 0; i < named.NumMethods(); i++ {
						visit(named.Method(i))
					}
				}
			}
		}
	}
	require.True(t, positive, "new outside-package public engine delegate must enter the reference denominator")
	require.False(t, negative, "receiver identity alone must not make a scalar-only input reference-typed")
	sort.Slice(rows, func(i, j int) bool { return rows[i].Boundary+rows[i].Argument < rows[j].Boundary+rows[j].Argument })
	data, err := json.MarshalIndent(struct {
		Author, Status string
		Members        []row
	}{"#1548 implementation lane (Codex)", "Reference-signature denominator, not ownership approval; concrete declared methods plus public Engine/transaction/entity delegates; abstract interface declarations excluded", rows}, "", "  ")
	require.NoError(t, err)
	fmt.Printf("RESIDUAL-REFERENCE-BEGIN\n%s\nRESIDUAL-REFERENCE-END\n", data)
}
