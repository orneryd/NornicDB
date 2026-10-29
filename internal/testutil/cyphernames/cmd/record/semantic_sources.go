package main

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path"
	"strings"
)

// This parses concrete assertions of actual passing programs. The completion
// catalog separately assigns an exact construction to its semantic observation;
// neither this source census nor a passing symbol grants reach.
func semanticSources(passed []string) map[string]any {
	passing := map[string]bool{}
	for _, name := range passed {
		passing[name] = true
	}
	files, err := exec.Command("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard").Output()
	must(err)
	result := map[string]any{}
	seen := map[string]bool{}
	for _, file := range strings.Split(string(files), "\x00") {
		if seen[file] || !strings.HasSuffix(file, "_test.go") {
			continue
		}
		seen[file] = true
		data, err := os.ReadFile(file)
		must(err)
		fs := token.NewFileSet()
		tree, err := parser.ParseFile(fs, file, data, 0)
		must(err)
		for _, declaration := range tree.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			outcome := "github.com/orneryd/nornicdb/" + path.Dir(file) + "#" + fn.Name.Name
			if !passing[outcome] {
				continue
			}
			var assertions []map[string]any
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				callee := ""
				if selected, ok := call.Fun.(*ast.SelectorExpr); ok {
					callee = selected.Sel.Name
				}
				switch callee {
				case "Equal", "EqualValues", "ElementsMatch", "Contains", "RoundTrip", "SchemaRoundTrip", "Check", "CheckSchema":
					start, end := fs.Position(call.Pos()), fs.Position(call.End())
					assertions = append(assertions, map[string]any{"file": file, "line": start.Line, "source": string(data[start.Offset:end.Offset]), "callee": callee})
				}
				return true
			})
			start, end := fs.Position(fn.Pos()), fs.Position(fn.End())
			result[file+"#"+fn.Name.Name] = map[string]any{"file": file, "function": fn.Name.Name, "outcome": outcome,
				"file_sha256": fmt.Sprintf("%x", sha256.Sum256(data)), "source": string(data[start.Offset:end.Offset]), "assertions": assertions}
		}
	}
	return result
}
