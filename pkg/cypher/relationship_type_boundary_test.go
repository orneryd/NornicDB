package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

// Check representation access using the same compiler identities as the package guard.
func TestRelationshipOwnerTypeRepresentationMutations(t *testing.T) {
	const owner = `package cypher
type relationshipTypeFacts struct { relationshipTypeName string; relationshipTypeQuoted, relationshipTypeDynamic bool }
type relationshipTypes struct { relationshipTypeFirst relationshipTypeFacts; relationshipTypeRest []relationshipTypeFacts; relationshipTypeCount int }
func (t *relationshipTypes) append(v relationshipTypeFacts) {}`
	for _, body := range []string{
		`func bad(t relationshipTypes){consume(t.relationshipTypeFirst)}`,
		`func bad(t relationshipTypes){other:=t;consume(other.relationshipTypeRest)}`,
		`func bad(t relationshipTypeFacts){consume(t.relationshipTypeName)}`,
		`func bad(t relationshipTypeFacts){t.relationshipTypeDynamic=false}`,
		`func bad(){consume(relationshipTypeFacts{relationshipTypeName:"$(R)"})}`,
		`func bad(t relationshipTypes,v relationshipTypeFacts){t.append(v)}`,
		`func (t relationshipTypes) decoded()string{return "$(R)"}`,
		`type erased relationshipTypeFacts`,
		`type erased = relationshipTypes`,
	} {
		failures := relationshipTypedOwnerViolations(map[string]string{"relationship_declaration.go": owner, "consumer.go": "package cypher;func consume(interface{}){};" + body}, nil)
		caught := false
		for _, failure := range failures {
			caught = caught || strings.Contains(failure, "opaque relationship type") || strings.Contains(failure, "relationship type facts") || strings.Contains(failure, "relationship type method") || strings.Contains(failure, "relationship type representation")
		}
		if !caught {
			t.Errorf("unsealed type: %s; diagnostics: %v", body, failures)
		}
	}
	// Compiler proof of the construction boundary: the old prefix call, a
	// conversion, and list erasure cannot accept the actual nominal structs.
	for _, body := range []string{
		`func bad(t relationshipTypeFacts){prefix(t,"$(")}`,
		`func bad(t relationshipTypeFacts){_ = string(t)}`,
		`func bad(t relationshipTypes){_ = []string(t)}`,
		`func bad(t relationshipTypes){for _,name:=range t{prefix(name,"$(")}}`,
	} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "mutant.go", owner+`;func prefix(string,string)bool{return false};`+body, 0)
		if err != nil {
			t.Fatal(err)
		}
		config := types.Config{Error: func(error) {}}
		if _, err = config.Check("cypher", fset, []*ast.File{file}, nil); err == nil {
			t.Errorf("compiler accepted erased provenance: %s", body)
		}
	}
	if got := relationshipTypedOwnerViolations(map[string]string{"relationship_declaration.go": owner, "consumer.go": `package cypher;type plan struct{types relationshipTypes};func consume(interface{}){};func good(p plan){consume(p.types)};type unrelated struct{relationshipTypeName string};func(*unrelated)append(){};func other(value *unrelated){value.append();consume(value.relationshipTypeName)}`}, nil); len(got) != 0 {
		t.Fatal(strings.Join(got, "\n"))
	}
}
