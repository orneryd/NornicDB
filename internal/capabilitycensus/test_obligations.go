package capabilitycensus

// Author: Lane #1644 (Codex). Test names index executable obligations; they
// never establish a behavioral disposition. Only actual compiling test
// declarations and the finite executable-adapter registries belong here.

import (
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// ObligationDeclarations indexes real tests and their finite adapter registries.
// Registry presence is checked separately from executing each adapter's oracle.
type ObligationDeclarations struct {
	Names    map[string]bool
	Registry workcensus.Inventory
}

// TestObligations consumes the shared engine's compiled package scopes rather
// than rendering every test body, inactive fixture and testdata document. The
// shared engine also extracts the registry syntax and owns its source identity.
// Results are fresh: adding a compiling declaration must affect the next query.
func TestObligations(root string) (ObligationDeclarations, error) {
	result := ObligationDeclarations{Names: map[string]bool{}}
	program, err := workcensus.LoadProgram(workcensus.Config{
		Dir: root, Patterns: []string{"./..."}, Tests: true,
		BuildFlags: []string{"-tags=noui,nolocalllm"},
		Env:        append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"),
	})
	if err != nil {
		return result, err
	}
	registryFiles := map[string]bool{}
	for _, pkg := range program.Packages {
		for _, name := range pkg.Types.Scope().Names() {
			fn, ok := pkg.Types.Scope().Lookup(name).(*types.Func)
			if !ok || !testFunction(fn) {
				continue
			}
			filename := program.FileSet.PositionFor(fn.Pos(), false).Filename
			if strings.HasSuffix(filename, "_test.go") {
				result.Names[name] = true
			}
		}
		for _, filename := range pkg.CompiledGoFiles {
			switch filepath.Base(filename) {
			case "engine_population_contract_test.go", "foundation_public_obligation_test.go", "foundation_external_obligation_test.go":
				registryFiles[filename] = true
			}
		}
	}
	var paths []string
	for filename := range registryFiles {
		paths = append(paths, filename)
	}
	sort.Strings(paths)
	// A syntax-only request with no documents would load the current package.
	// No compiled registry is a genuine empty result for the gate to reject.
	if len(paths) == 0 {
		return result, nil
	}
	config := workcensus.Config{Dir: root, SourceOnly: true, IncludeSyntax: true,
		Selectors: []workcensus.Selector{
			{Kind: "declaration", Node: "declaration", Reachability: "all"},
			{Kind: "composite", Node: "composite", Reachability: "all"},
		},
	}
	for _, filename := range paths {
		config.Documents = append(config.Documents, workcensus.Document{Path: filename, Language: "go"})
	}
	result.Registry, err = workcensus.Extract(config)
	return result, err
}

func testFunction(fn *types.Func) bool {
	name := fn.Name()
	if !strings.HasPrefix(name, "Test") {
		return false
	}
	if suffix := strings.TrimPrefix(name, "Test"); suffix != "" {
		first, _ := utf8.DecodeRuneInString(suffix)
		if unicode.IsLower(first) {
			return false
		}
	}
	sig := fn.Type().(*types.Signature)
	if sig.Recv() != nil || sig.TypeParams().Len() != 0 || sig.Variadic() || sig.Params().Len() != 1 || sig.Results().Len() != 0 {
		return false
	}
	pointer, ok := types.Unalias(sig.Params().At(0).Type()).(*types.Pointer)
	if !ok {
		return false
	}
	typ, ok := types.Unalias(pointer.Elem()).(*types.Named)
	return ok && typ.Obj().Pkg() != nil && typ.Obj().Pkg().Path() == "testing" && typ.Obj().Name() == "T"
}
