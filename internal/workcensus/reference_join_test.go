package workcensus

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// This is a query over Extract's typed declaration views, not another source
// enumerator. In particular there is no parser, AST walk, loader or source hash.
// Object positions join fields, methods, aliases and values to the smallest
// selected declaration; a field therefore belongs to its containing type.
type referenceDeclaration struct {
	ID, Owner, Digest, File, Kind string
	Line                          int
}

type referenceEdge struct {
	From, To, Object, Name, Kind, File string
	Line, Column, Count                int
}

type referenceSymbol struct {
	Declaration, Name, Kind, Object, Value string
}

type referenceQuery struct {
	Declarations []referenceDeclaration
	Symbols      []referenceSymbol
	Edges        []referenceEdge
	Containment  [][2]string
	Uses         int
	ExternalUses int
	UnownedLocal []string
	Limitations  []string
}

func referenceObjectKind(obj types.Object) string {
	switch o := obj.(type) {
	case *types.Func:
		if o.Type().(*types.Signature).Recv() != nil {
			return "method"
		}
		return "function"
	case *types.Var:
		if o.IsField() {
			return "field"
		}
		return "var"
	case *types.TypeName:
		return "type"
	case *types.Const:
		return "const"
	case *types.PkgName:
		return "import"
	}
	return "other"
}

func typedReferenceQuery(t *testing.T, inv Inventory) referenceQuery {
	t.Helper()
	q := referenceQuery{Limitations: inv.Limitations}
	infos := map[*types.Info]*GoSyntax{}
	byFile := map[string][]Member{}
	var fset *token.FileSet
	for _, m := range inv.Members {
		if m.Kind != "declaration" || m.Syntax == nil || m.Syntax.Info == nil {
			continue
		}
		fset = m.Syntax.FileSet
		infos[m.Syntax.Info] = m.Syntax
		pos := fset.PositionFor(m.Syntax.Node.Pos(), false)
		byFile[pos.Filename] = append(byFile[pos.Filename], m)
		q.Declarations = append(q.Declarations, referenceDeclaration{m.ID, m.Owner, m.Digest, m.File, m.Node, m.Line})
	}
	if fset == nil {
		t.Fatal("query needs typed all-source declaration views")
	}
	ownerAt := func(pos token.Pos, exclude string) *Member {
		file := fset.PositionFor(pos, false).Filename
		var best *Member
		for i := range byFile[file] {
			m := &byFile[file][i]
			if m.ID == exclude || pos < m.Syntax.Node.Pos() || pos >= m.Syntax.Node.End() {
				continue
			}
			if best == nil || m.Syntax.Node.End()-m.Syntax.Node.Pos() < best.Syntax.Node.End()-best.Syntax.Node.Pos() {
				best = m
			}
		}
		return best
	}
	// A literal's body is a separate engine owner. Retain the enclosing
	// declaration -> literal edge even when no identifier names the literal.
	for _, ms := range byFile {
		for _, m := range ms {
			if parent := ownerAt(m.Syntax.Node.Pos(), m.ID); parent != nil {
				q.Containment = append(q.Containment, [2]string{parent.ID, m.ID})
			}
		}
	}
	edges := map[string]*referenceEdge{}
	unowned := map[string]bool{}
	packages := map[string]bool{}
	for _, syntax := range infos {
		packages[syntax.Package.Path()] = true
	}
	for info := range infos {
		for id, obj := range info.Defs {
			if obj == nil || obj.Pkg() == nil || obj.Name() == "_" {
				continue
			}
			kind := referenceObjectKind(obj)
			// Local bindings are already attributed to their declaration/body.
			// The cross-package API surface includes package objects, fields and
			// methods, including unexported ones used within the same package.
			if obj.Parent() != obj.Pkg().Scope() && kind != "field" && kind != "method" {
				continue
			}
			if owner := ownerAt(id.Pos(), ""); owner != nil {
				symbol := referenceSymbol{Declaration: owner.ID, Name: obj.Name(), Kind: kind, Object: types.ObjectString(obj, func(p *types.Package) string { return p.Path() })}
				if constant, ok := obj.(*types.Const); ok {
					symbol.Value = constant.Val().ExactString()
				}
				q.Symbols = append(q.Symbols, symbol)
			}
		}
		for id, obj := range info.Uses {
			from := ownerAt(id.Pos(), "")
			if from == nil {
				continue // Outside the selected declaration population.
			}
			q.Uses++
			if fn, ok := obj.(*types.Func); ok {
				obj = fn.Origin()
			}
			to := ownerAt(obj.Pos(), "")
			if to == nil {
				q.ExternalUses++
				if obj.Pkg() != nil && packages[obj.Pkg().Path()] {
					unowned[types.ObjectString(obj, func(p *types.Package) string { return p.Path() })] = true
				}
				continue
			}
			if from.ID == to.ID {
				continue
			}
			object := types.ObjectString(obj, func(p *types.Package) string { return p.Path() })
			key := from.ID + "\x00" + to.ID + "\x00" + object
			pos := fset.PositionFor(id.Pos(), false)
			if edge := edges[key]; edge != nil {
				edge.Count++
				if pos.Line < edge.Line || pos.Line == edge.Line && pos.Column < edge.Column {
					edge.Line, edge.Column = pos.Line, pos.Column
				}
			} else {
				edges[key] = &referenceEdge{From: from.ID, To: to.ID, Object: object, Name: obj.Name(), Kind: referenceObjectKind(obj), File: from.File, Line: pos.Line, Column: pos.Column, Count: 1}
			}
		}
	}
	for _, edge := range edges {
		q.Edges = append(q.Edges, *edge)
	}
	for name := range unowned {
		q.UnownedLocal = append(q.UnownedLocal, name)
	}
	sort.Strings(q.UnownedLocal)
	sort.Slice(q.Symbols, func(i, j int) bool {
		a, b := q.Symbols[i], q.Symbols[j]
		return a.Declaration+"\x00"+a.Kind+"\x00"+a.Name+"\x00"+a.Object < b.Declaration+"\x00"+b.Kind+"\x00"+b.Name+"\x00"+b.Object
	})
	sort.Slice(q.Declarations, func(i, j int) bool { return q.Declarations[i].ID < q.Declarations[j].ID })
	sort.Slice(q.Edges, func(i, j int) bool {
		a, b := q.Edges[i], q.Edges[j]
		return a.From+"\x00"+a.To+"\x00"+a.Object < b.From+"\x00"+b.To+"\x00"+b.Object
	})
	sort.Slice(q.Containment, func(i, j int) bool {
		return q.Containment[i][0]+"\x00"+q.Containment[i][1] < q.Containment[j][0]+"\x00"+q.Containment[j][1]
	})
	return q
}

func TestGenericCensusTypedReferenceJoin(t *testing.T) {
	c := declarationIdentityConfig(t, true, `package sample
type Left struct { Same int }
type Right struct { Same string }
const (First = 1; Second = First + 1)
func (x Left) Read() int { return x.Same }
func (x Right) Read() string { return x.Same }
var Bound = Left.Read
var Literal = func(x Right) string { return x.Read() }
func Consumer(a Left, b Right) (int, string) { return Bound(a), Literal(b) }
`)
	c.SourceOnly, c.IncludeSyntax = true, true
	c.Selectors = []Selector{{Kind: "declaration", Node: "declaration", Reachability: "all"}}
	q := typedReferenceQuery(t, mustExtract(t, c))
	owners := map[string]string{}
	for _, d := range q.Declarations {
		owners[d.ID] = d.Owner
	}
	seen := map[string]bool{}
	for _, e := range q.Edges {
		seen[owners[e.From]+" -> "+owners[e.To]] = true
	}
	for _, pair := range []string{
		"(sample.Left).Read -> sample.type:Left",
		"(sample.Right).Read -> sample.type:Right",
		"sample.var:Bound -> (sample.Left).Read",
		"sample.Consumer -> sample.var:Bound",
		"sample.Consumer -> sample.var:Literal",
	} {
		if !seen[pair] {
			t.Fatalf("lost typed reference %s: %v", pair, seen)
		}
	}
	if len(q.Containment) == 0 || len(q.UnownedLocal) != 0 {
		t.Fatal("lost literal containment or local declaration", q.Containment, q.UnownedLocal)
	}
	fields := map[string]bool{}
	var second string
	for _, s := range q.Symbols {
		if s.Kind == "field" && s.Name == "Same" {
			fields[s.Declaration] = true
		}
		if s.Kind == "const" && s.Name == "Second" {
			second = s.Value
		}
	}
	if len(fields) != 2 || second != "2" {
		t.Fatal("field identities or typed constant value lost", fields, second)
	}
	// A new field/method use must reach the other type, despite matching names.
	source, err := os.ReadFile(filepath.Join(c.Dir, "source.go"))
	if err != nil {
		t.Fatal(err)
	}
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "source.go"): []byte(strings.Replace(string(source), "return Bound(a), Literal(b)", "_ = a.Read(); return Bound(a), b.Read()", 1))}
	changed := typedReferenceQuery(t, mustExtract(t, c))
	found := false
	for _, e := range changed.Edges {
		if strings.Contains(e.From, "sample.Consumer/") && strings.Contains(e.To, "(sample.Right).Read/") {
			found = true
		}
	}
	if !found {
		t.Fatal("planted same-name method reference escaped typed join")
	}
	// A source-identical constant can change value through another constant.
	// Typed symbol facts must expose that, independently of source digests.
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "source.go"): []byte(strings.Replace(string(source), "First = 1", "First = 3", 1))}
	changed = typedReferenceQuery(t, mustExtract(t, c))
	for _, s := range changed.Symbols {
		if s.Name == "Second" && s.Kind == "const" && s.Value != second && s.Value == "4" {
			return
		}
	}
	t.Fatal("dependent constant's changed value escaped typed facts")
}

var referenceRevisions = flag.String("workcensus-reference-revisions", "", "optional comma-separated name=git-revision snapshots for the shared typed declaration query")
var referenceTags = flag.String("workcensus-reference-tags", "", "build tags for optional revision queries")

func TestGenericCensusRevisionReferences(t *testing.T) {
	if *referenceRevisions == "" {
		t.Skip("optional compiled revision query; generic join control runs independently")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range strings.Split(*referenceRevisions, ",") {
		name, revision, ok := strings.Cut(item, "=")
		if !ok || name == "" || revision == "" {
			t.Fatal("expected name=revision", item)
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			archive := exec.Command("git", "-C", root, "archive", revision)
			var stderr bytes.Buffer
			archive.Stderr = &stderr
			stream, err := archive.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := archive.Start(); err != nil {
				t.Fatal(err)
			}
			tr := tar.NewReader(stream)
			for {
				h, err := tr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(dir, filepath.FromSlash(h.Name))
				rel, err := filepath.Rel(dir, path)
				if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					t.Fatal("unsafe archive path", h.Name)
				}
				if h.Typeflag != tar.TypeReg {
					continue
				}
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, os.FileMode(h.Mode))
				if err != nil {
					t.Fatal(err)
				}
				_, copyErr := io.Copy(f, tr)
				closeErr := f.Close()
				if copyErr != nil || closeErr != nil {
					t.Fatal(copyErr, closeErr)
				}
			}
			if err := archive.Wait(); err != nil {
				t.Fatal(err, stderr.String())
			}
			c := Config{Dir: dir, Patterns: []string{"./..."}, SourceOnly: true, IncludeSyntax: true,
				Env:       append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=-mod=readonly"),
				Selectors: []Selector{{Kind: "declaration", Node: "declaration", Reachability: "all"}}}
			if *referenceTags != "" {
				c.BuildFlags = []string{"-tags=" + *referenceTags}
			}
			q := typedReferenceQuery(t, mustExtract(t, c))
			raw, err := json.Marshal(q)
			if err != nil {
				t.Fatal(err)
			}
			var compressed bytes.Buffer
			zw := gzip.NewWriter(&compressed)
			if _, err := zw.Write(raw); err != nil {
				t.Fatal(err)
			}
			if err := zw.Close(); err != nil {
				t.Fatal(err)
			}
			fmt.Printf("WORKCENSUS-REVISION-REFERENCES %s %s %s\n", name, revision, base64.StdEncoding.EncodeToString(compressed.Bytes()))
			t.Logf("%d declarations, %d grouped typed edges, %d examined uses, %d external uses, %d unowned-local facts", len(q.Declarations), len(q.Edges), q.Uses, q.ExternalUses, len(q.UnownedLocal))
		})
		runtime.GC()
	}
}
