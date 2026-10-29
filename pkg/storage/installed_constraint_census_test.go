package storage

import (
	"github.com/stretchr/testify/require"
	"go/ast"
	"go/token"
	"go/types"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Reconcile fixtures with actual admitted mutable property representations.
// A new admitted list/map kind needs an ownership subject before this passes.
func TestInstalledConstraintMutablePopulation(t *testing.T) {
	checked := checkStoragePackage(t, "")
	require.NotEmpty(t, checked.info.Uses)
	var actual []string
	for _, file := range checked.files {
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Name.Name != "validatePropertyValueForStorage" {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				clause, ok := node.(*ast.CaseClause)
				if !ok {
					return true
				}
				for _, expression := range clause.List {
					typ := checked.info.TypeOf(expression)
					if typ == nil {
						continue
					}
					switch typ.Underlying().(type) {
					case *types.Slice, *types.Map:
						name := types.TypeString(typ, nil)
						name = strings.ReplaceAll(name, "interface{}", "any")
						name = strings.ReplaceAll(name, "[]byte", "[]uint8")
						actual = append(actual, name)
					}
				}
				return true
			})
		}
	}
	want := []string{"[]any", "map[string]any", "[]uint8", "[]string", "[]int", "[]int32", "[]int64", "[]float32", "[]float64", "[]bool"}
	sort.Strings(actual)
	sort.Strings(want)
	require.Equal(t, want, actual)
	// The actual retained map cannot silently revert to public mutable DTOs.
	require.Equal(t, reflect.TypeOf(installedConstraintDefinition{}), reflect.TypeOf(NewSchemaManager().constraints).Elem())
}

func constraintViolationConstructionOwners(checked *checkedStorage) []string {
	violation := checked.pkg.Scope().Lookup("ConstraintViolationError").Type()
	var owners []string
	inspect := func(root ast.Node, owner string) {
		ast.Inspect(root, func(node ast.Node) bool {
			var result types.Type
			switch expression := node.(type) {
			case *ast.UnaryExpr:
				if expression.Op == token.AND {
					result = checked.info.TypeOf(expression)
				}
			case *ast.CallExpr:
				if identifier, ok := expression.Fun.(*ast.Ident); ok && identifier.Name == "new" {
					result = checked.info.TypeOf(expression)
				}
			}
			pointer, ok := result.(*types.Pointer)
			if ok && types.IdenticalIgnoreTags(pointer.Elem().Underlying(), violation.Underlying()) {
				owners = append(owners, owner)
			}
			return true
		})
	}
	for _, file := range checked.files {
		for _, decl := range file.Decls {
			if function, ok := decl.(*ast.FuncDecl); ok {
				if function.Body != nil {
					inspect(function.Body, function.Name.Name)
				}
			} else {
				inspect(decl, "<package>")
			}
		}
	}
	sort.Strings(owners)
	return owners
}

func TestInstalledConstraintViolationConstruction(t *testing.T) {
	checked := checkStoragePackage(t, "")
	require.Equal(t, []string{"newConstraintViolation"}, constraintViolationConstructionOwners(checked), "all pointer constructions require the shared report constructor")
	shape := checked.pkg.Scope().Lookup("ConstraintViolationError").Type().Underlying().(*types.Struct)
	var fields []string
	for index := 0; index < shape.NumFields(); index++ {
		field := shape.Field(index)
		if !field.Exported() {
			continue
		}
		switch field.Type().Underlying().(type) {
		case *types.Slice, *types.Map, *types.Pointer, *types.Interface, *types.Signature, *types.Chan:
			fields = append(fields, field.Name())
		}
	}
	sort.Strings(fields)
	require.Equal(t, []string{"Cause", "Properties"}, fields, "new public mutable error fields require ownership/cause proof")
}

func TestInstalledConstraintCensusCanaries(t *testing.T) {
	// One unknown producer cannot disappear behind an empty Uses graph or a
	// different Go construction spelling. Alias factory calls remain safe.
	overlay := `package storage
 type censusAlias = ConstraintViolationError
 type censusView ConstraintViolationError
 var censusGlobal = &ConstraintViolationError{}
 func CensusNew()*ConstraintViolationError{return new(ConstraintViolationError)}
 func CensusAlias()*ConstraintViolationError{var value censusAlias;return &value}
 func CensusConverted()*ConstraintViolationError{return (*ConstraintViolationError)(&censusView{})}
 func CensusFactoryAlias()*ConstraintViolationError{factory:=newConstraintViolation;return factory(ConstraintViolationError{})}
 `
	checked := checkStoragePackage(t, overlay)
	require.NotEmpty(t, checked.info.Uses)
	require.Equal(t, []string{"<package>", "CensusAlias", "CensusConverted", "CensusNew", "newConstraintViolation"}, constraintViolationConstructionOwners(checked))
}

// Inventory all exported storage errors with public reference fields, even
// when their names have no constraint substring. Other error contracts are
// classified separately; only the constraint report may carry installed data.
func TestInstalledConstraintExportedErrorInventory(t *testing.T) {
	checked := checkStoragePackage(t, "")
	errorType := types.Universe.Lookup("error").Type().Underlying().(*types.Interface)
	var found []string
	for _, name := range checked.pkg.Scope().Names() {
		object, ok := checked.pkg.Scope().Lookup(name).(*types.TypeName)
		if !ok || !object.Exported() {
			continue
		}
		if !types.Implements(object.Type(), errorType) && !types.Implements(types.NewPointer(object.Type()), errorType) {
			continue
		}
		shape, ok := object.Type().Underlying().(*types.Struct)
		if !ok {
			continue
		}
		for index := 0; index < shape.NumFields(); index++ {
			field := shape.Field(index)
			if !field.Exported() {
				continue
			}
			switch field.Type().Underlying().(type) {
			case *types.Slice, *types.Map, *types.Pointer, *types.Interface, *types.Signature, *types.Chan:
				found = append(found, name+"."+field.Name())
			}
		}
	}
	sort.Strings(found)
	require.NotEmpty(t, found)
	t.Logf("exported reference-bearing error fields: %v", found)
	require.Equal(t, []string{"ConstraintViolationError.Cause", "ConstraintViolationError.Properties"}, found,
		"a new exported reference-bearing error needs a route/ownership disposition")
}

func TestInstalledConstraintDefinitionsHoldNoReferences(t *testing.T) {
	var inspect func(reflect.Type)
	inspect = func(typ reflect.Type) {
		switch typ.Kind() {
		case reflect.Struct:
			for index := 0; index < typ.NumField(); index++ {
				inspect(typ.Field(index).Type)
			}
		case reflect.Array:
			inspect(typ.Elem())
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface, reflect.Chan, reflect.UnsafePointer, reflect.Uintptr:
			t.Fatalf("installed definition exposes mutable header state: %s", typ)
		}
	}
	// Functions are the deliberate private closure boundary. Their public
	// projections are verified separately through mutation and real writes.
	inspect(reflect.TypeOf(installedConstraintDefinition{}))
	inspect(reflect.TypeOf(ConstraintFact{}))
	inspect(reflect.TypeOf(PropertyTypeConstraint{}))
}
