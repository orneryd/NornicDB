package cypher

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

//go:embed testdata/value_contract_calls.json
var valueContractCallsJSON []byte

type valueContractCallSite struct {
	ID       string `json:"id"`
	File     string `json:"file"`
	Caller   string `json:"caller"`
	Callee   string `json:"callee"`
	Contract string `json:"contract"`
	Line     int    `json:"line"`
	Reason   string `json:"reason"`
}

// These are the canonical constructors, recursive operations and index
// adapters. All require a final cypherComparisonContract parameter. Index
// storage has no contract field and cannot supply a default on callers' behalf.
var valueContractConstructors = map[string]int{
	"temporalIdentityParts": 2, "zonedDateTimeIdentityParts": 3,
	"dateEqualityParts": 2, "clockEqualityParts": 3,
	"localDateTimeEqualityParts": 2, "durationEqualityParts": 2,
	"collectPropValues": 3, "filterNodesByAllowedPropSet": 4,
	"cartesianShiftValueKey": 3, "shiftCartesianIntKeySet": 3,
	"cypherValueKey": 2, "cypherValueKeyWithNumericPolicy": 3,
	"appendValueKey": 4, "appendNestedValueKey": 4,
	"cypherComparableKey": 2, "cypherValueHash": 2,
	"cypherValuesMatch": 3, "compareTemporalByContract": 3,
	"addByContract": 2, "findByContract": 2, "addTupleByContract": 2,
	"addTupleValuesByContract": 3, "addPositionByContract": 2, "hasByContract": 2,
}

// Fixed public-domain adapters still have their callers inventoried. They own
// an explicit null or collection policy, rather than a generic default key.
var valueContractAdapters = map[string]string{
	"cypherValueEquals":       "equality adapter",
	"apocValueSet":            "equality adapter",
	"apocSameValue":           "equivalence removal adapter (numeric kinds preserved)",
	"cypherEquality":          "three-valued equality adapter",
	"cypherEqualityValue":     "three-valued equality adapter",
	"toSet":                   "equivalence adapter",
	"appendTemporalValueKey":  "already canonical temporal key",
	"compareTemporalOrdering": "ordering (separate contract)",
	"temporalOrderParts":      "ordering (separate contract)",
}

var removedImplicitValueConstructors = map[string]bool{
	"cypherEquivalenceKey": true, "temporalEqualityParts": true,
	"temporalEquivalenceParts": true, "compareTemporalValues": true,
	"cypherEquivalenceHash": true, "cypherValuesEquivalent": true,
	"cartesianValueKey": true, "temporalKey": true,
}

func valueContractExpression(fs *token.FileSet, expr ast.Expr) string {
	var out bytes.Buffer
	if err := format.Node(&out, fs, expr); err != nil {
		panic(err)
	}
	return out.String()
}

func valueContractCallee(expr ast.Expr) (string, *ast.Ident) {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name, expr
	case *ast.SelectorExpr:
		return expr.Sel.Name, expr.Sel
	case *ast.IndexExpr:
		return valueContractCallee(expr.X)
	case *ast.IndexListExpr:
		return valueContractCallee(expr.X)
	case *ast.ParenExpr:
		return valueContractCallee(expr.X)
	}
	return "", nil
}

func inspectValueContractSites(fs *token.FileSet, file *ast.File, filename string) ([]valueContractCallSite, []string) {
	var sites []valueContractCallSite
	var errors []string
	// Constructor aliases would conceal consumers from a call-site inventory.
	// Keep constructor references direct, including explicitly instantiated
	// generic calls and method calls, so every new caller is observable.
	direct := map[*ast.Ident]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.FuncDecl:
			direct[node.Name] = true
		case *ast.CallExpr:
			_, identifier := valueContractCallee(node.Fun)
			direct[identifier] = true
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		if identifier, ok := node.(*ast.Ident); ok {
			if _, constructor := valueContractConstructors[identifier.Name]; constructor && !direct[identifier] {
				errors = append(errors, "constructor aliases are not inventoried: "+identifier.Name)
			}
		}
		return true
	})
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		caller := fn.Name.Name
		if fn.Recv != nil {
			caller = valueContractExpression(fs, fn.Recv.List[0].Type) + "." + caller
		}
		if removedImplicitValueConstructors[fn.Name.Name] {
			errors = append(errors, "implicit constructor remains: "+caller)
		}
		propagates := false
		for _, p := range fn.Type.Params.List {
			for _, id := range p.Names {
				if id.Name == "contract" && valueContractExpression(fs, p.Type) == "cypherComparisonContract" {
					propagates = true
				}
			}
		}
		if _, constructor := valueContractConstructors[fn.Name.Name]; constructor && !propagates {
			errors = append(errors, "constructor must require contract: "+caller)
		}
		ordinal := map[string]int{}
		var contractRanges []*ast.RangeStmt
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if r, ok := n.(*ast.RangeStmt); ok {
				if id, ok := r.Value.(*ast.Ident); ok && id.Name == "contract" {
					contractRanges = append(contractRanges, r)
				}
			}
			return true
		})
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			callee, _ := valueContractCallee(call.Fun)
			if removedImplicitValueConstructors[callee] {
				errors = append(errors, "implicit call remains: "+caller+" -> "+callee)
			}
			arity, constructor := valueContractConstructors[callee]
			contract, adapter := valueContractAdapters[callee]
			if !constructor && !adapter {
				return true
			}
			if constructor {
				if len(call.Args) != arity {
					errors = append(errors, fmt.Sprintf("%s -> %s requires explicit contract (%d arguments)", caller, callee, arity))
					return true
				}
				contract = valueContractExpression(fs, call.Args[len(call.Args)-1])
				inContractRange := false
				for _, r := range contractRanges {
					inContractRange = inContractRange || call.Pos() >= r.Pos() && call.End() <= r.End()
				}
				if contract != "cypherValueEqualityContract" && contract != "cypherEquivalenceContract" && !(contract == "contract" && (propagates || strings.HasSuffix(filename, "_test.go") && inContractRange)) {
					errors = append(errors, "undeclared contract: "+caller+" -> "+callee+" ("+contract+")")
				}
			}
			ordinal[callee]++
			sites = append(sites, valueContractCallSite{ID: fmt.Sprintf("%s:%s:%s:%d", filename, caller, callee, ordinal[callee]), File: filename, Caller: caller, Callee: callee, Contract: contract, Line: fs.Position(call.Pos()).Line})
			return true
		})
	}
	return sites, errors
}

func TestValueContractInventory(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	// Discover forwarded constructors as well, rather than allowing a new
	// wrapper to hide its callers from the guard's mechanical population.
	for _, path := range files {
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && len(fn.Type.Params.List) > 0 {
				last := fn.Type.Params.List[len(fn.Type.Params.List)-1]
				if valueContractExpression(fs, last.Type) == "cypherComparisonContract" {
					arity := 0
					for _, p := range fn.Type.Params.List {
						arity += len(p.Names)
					}
					valueContractConstructors[fn.Name.Name] = arity
				}
			}
		}
	}
	var actual []valueContractCallSite
	for _, path := range files {
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		sites, errors := inspectValueContractSites(fs, file, path)
		for _, err := range errors {
			t.Error(err)
		}
		actual = append(actual, sites...)
	}
	sort.Slice(actual, func(i, j int) bool { return actual[i].ID < actual[j].ID })
	if os.Getenv("NORNICDB_VALUE_CONTRACT_INVENTORY") == "1" {
		data, err := json.Marshal(actual)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("VALUE_CONTRACT_SITES " + string(data))
		return
	}
	var registered []valueContractCallSite
	if err := json.Unmarshal(valueContractCallsJSON, &registered); err != nil {
		t.Fatal(err)
	}
	for i := range registered {
		if strings.TrimSpace(registered[i].Reason) == "" {
			t.Errorf("missing contract reason: %s", registered[i].ID)
		}
		registered[i].Reason = ""
		registered[i].Line = 0
	}
	for i := range actual {
		actual[i].Line = 0
	}
	if !reflect.DeepEqual(registered, actual) {
		want := map[string]string{}
		for _, site := range registered {
			want[site.ID] = site.Contract
		}
		for _, site := range actual {
			if contract, ok := want[site.ID]; !ok || contract != site.Contract {
				t.Errorf("unregistered or changed contract: %s: %s", site.ID, site.Contract)
			}
			delete(want, site.ID)
		}
		for id := range want {
			t.Errorf("stale contract declaration: %s", id)
		}
		t.Fatal("comparison call inventory changed; review every site and its required contract before updating testdata/value_contract_calls.json")
	}
}

func TestValueContractInventoryRejectsImplicitCalls(t *testing.T) {
	for _, source := range []string{
		"package cypher; func consumer(v any) { cypherValueKey(v) }",
		"package cypher; func consumer(v any) { cypherValueKey(v,0) }",
		"package cypher; func consumer(v any) { cypherEquivalenceKey(v) }",
		"package cypher; func consumer(v any) { var contract cypherComparisonContract; cypherValueKey(v,contract) }",
		"package cypher; func consumer(v any) { hidden:=cypherValueKey; hidden(v,cypherEquivalenceContract) }",
	} {
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, "consumer.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, errors := inspectValueContractSites(fs, file, "consumer.go")
		if len(errors) == 0 {
			t.Fatalf("implicit constructor accepted: %s", source)
		}
	}
}
