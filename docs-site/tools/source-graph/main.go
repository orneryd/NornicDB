// source-graph extracts static Go declarations and package-resolved calls.
// Method selector calls without type information are deliberately not invented.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

type Symbol struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	File    string `json:"file"`
	Line    int    `json:"line"`
	End     int    `json:"end"`
	Package string `json:"package"`
	Body    string `json:"body"`
	Hash    string `json:"hash"`
}
type Edge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Kind   string `json:"kind"`
}
type Graph struct {
	Nodes []Symbol `json:"nodes"`
	Links []Edge   `json:"links"`
}
type parsed struct {
	tree  *ast.File
	path  string
	data  []byte
	lines []string
}

func main() {
	root := os.Args[1]
	fs := token.NewFileSet()
	files := []parsed{}
	graph := Graph{Nodes: []Symbol{}, Links: []Edge{}}
	names := map[string][]string{}
	walkErr := filepath.WalkDir(filepath.Join(root, "pkg"), func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			panic(err)
		}
		t, err := parser.ParseFile(fs, p, b, 0)
		if err != nil {
			panic(err)
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		f := parsed{t, rel, b, strings.Split(string(b), "\n")}
		files = append(files, f)
		for _, decl := range t.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			line, end := fs.Position(fn.Pos()).Line, fs.Position(fn.End()).Line
			name := fn.Name.Name
			if fn.Recv != nil {
				name = receiver(fn.Recv.List[0].Type) + "." + name
			}
			id := symbolID(rel, name, line)
			body := strings.Join(f.lines[line-1:end], "\n")
			sum := sha256.Sum256([]byte(body))
			graph.Nodes = append(graph.Nodes, Symbol{id, name, rel, line, end, filepath.ToSlash(filepath.Dir(rel)), body, hex.EncodeToString(sum[:])})
			if fn.Recv == nil {
				key := filepath.ToSlash(filepath.Dir(rel)) + ":" + fn.Name.Name
				names[key] = append(names[key], id)
			}
		}
		return nil
	})
	if walkErr != nil {
		panic(walkErr)
	}
	for _, f := range files {
		imports := map[string]string{}
		for _, im := range f.tree.Imports {
			p := strings.Trim(im.Path.Value, "\"")
			if !strings.Contains(p, "/nornicdb/pkg/") {
				continue
			}
			alias := filepath.Base(p)
			if im.Name != nil {
				alias = im.Name.Name
			}
			imports[alias] = "pkg/" + strings.SplitN(p, "/nornicdb/pkg/", 2)[1]
		}
		for _, decl := range f.tree.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil {
				name = receiver(fn.Recv.List[0].Type) + "." + name
			}
			from := symbolID(f.path, name, fs.Position(fn.Pos()).Line)
			seen := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				key := ""
				to := ""
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if fun.Obj != nil && fun.Obj.Kind != ast.Fun {
						return true
					}
					if fun.Obj != nil {
						if declaration, ok := fun.Obj.Decl.(*ast.FuncDecl); ok {
							to = symbolID(f.path, declaration.Name.Name, fs.Position(declaration.Pos()).Line)
						}
					}
					key = filepath.ToSlash(filepath.Dir(f.path)) + ":" + fun.Name
				case *ast.SelectorExpr:
					if ident, ok := fun.X.(*ast.Ident); ok {
						if ident.Obj != nil && ident.Obj.Kind != ast.Pkg {
							return true
						}
						if pkg := imports[ident.Name]; pkg != "" {
							key = pkg + ":" + fun.Sel.Name
						}
					}
				}
				if to == "" && len(names[key]) == 1 {
					to = names[key][0]
				}
				if to != "" && !seen[to] {
					graph.Links = append(graph.Links, Edge{from, to, "static-call"})
					seen[to] = true
				}
				return true
			})
		}
	}
	if err := valueconvert.PropertyJSON.NewEncoder(os.Stdout).Encode(graph); err != nil {
		panic(err)
	}
}
func symbolID(file, name string, line int) string {
	id := file + ":" + name
	if name == "init" || name == "_" {
		id += "@" + strconv.Itoa(line)
	}
	return id
}

func receiver(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.StarExpr:
		return receiver(v.X)
	case *ast.IndexExpr:
		return receiver(v.X)
	case *ast.IndexListExpr:
		return receiver(v.X)
	}
	return "receiver"
}
