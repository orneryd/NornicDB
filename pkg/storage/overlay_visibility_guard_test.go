package storage

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// overlayBodyReadersOutsideComposition are the functions that may take the
// write-behind buffer's bodies without overlay_visibility.go, each for a
// reason that is not a read.
var overlayBodyReadersOutsideComposition = map[string]string{
	"BadgerEngine.replayWriteBehind": "the flush: lands a drained generation's commits in storage",
}

// Every read that merges the write-behind overlay with storage takes the
// buffer's bodies through overlay_visibility.go, which applies the read's
// visibility as its storage path does. A new reader of the buffer's bodies or
// fields outside the buffer and that file fails here until it reads through
// the composition (or is classified above as no read).
func TestOverlayBodiesAreReadThroughVisibility(t *testing.T) {
	fset := token.NewFileSet()
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		require.NoError(t, err)
		if !buildsOnLinuxForOverlayGuard(file) {
			continue
		}
		files = append(files, file)
	}
	info := &types.Info{Selections: make(map[*ast.SelectorExpr]*types.Selection)}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil), Error: func(error) {}}
	pkg, _ := conf.Check("github.com/orneryd/nornicdb/pkg/storage", fset, files, info)
	require.NotNil(t, pkg)

	isBuffer := func(typ types.Type) bool {
		if ptr, ok := typ.(*types.Pointer); ok {
			typ = ptr.Elem()
		}
		named, ok := typ.(*types.Named)
		return ok && named.Obj().Pkg() == pkg && (named.Obj().Name() == "WriteBehindBuffer" || named.Obj().Name() == "CommitBuffer")
	}
	bodyType := func(typ types.Type) bool {
		s := typ.String()
		for _, body := range []string{".Node", ".Edge", ".bufferedEntityState", ".bufferedCommit", ".bufferedMutation", ".CommitBuffer", ".pendingNodeIndex"} {
			if strings.Contains(s, body) {
				return true
			}
		}
		return false
	}
	carriesBodies := func(sig *types.Signature) bool {
		for i := 0; i < sig.Results().Len(); i++ {
			if bodyType(sig.Results().At(i).Type()) {
				return true
			}
		}
		return false
	}
	var readers []string
	used := make(map[string]bool)
	for _, file := range files {
		inComposition := filepath.Base(fset.Position(file.Pos()).Filename) == "overlay_visibility.go"
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || inComposition {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				recv := fn.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					if id.Name == "WriteBehindBuffer" || id.Name == "CommitBuffer" {
						continue
					}
					name = id.Name + "." + name
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				selection := info.Selections[sel]
				if selection == nil || !isBuffer(selection.Recv()) {
					return true
				}
				switch selection.Kind() {
				case types.FieldVal:
					if !bodyType(selection.Type()) {
						return true
					}
				case types.MethodVal:
					if sig, ok := selection.Type().(*types.Signature); !ok || !carriesBodies(sig) {
						return true
					}
				default:
					return true
				}
				if _, classified := overlayBodyReadersOutsideComposition[name]; classified {
					used[name] = true
					return true
				}
				readers = append(readers, name+" uses "+sel.Sel.Name+" at "+fset.Position(sel.Pos()).String())
				return true
			})
		}
	}
	sort.Strings(readers)
	require.Empty(t, readers, "overlay bodies read outside overlay_visibility.go")
	for name := range overlayBodyReadersOutsideComposition {
		require.True(t, used[name], "classified overlay reader %s no longer reads the buffer", name)
	}
}

// buildsOnLinuxForOverlayGuard reports whether file builds for the linux
// production build the CI runs.
func buildsOnLinuxForOverlayGuard(file *ast.File) bool {
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if !strings.HasPrefix(comment.Text, "//go:build ") {
				continue
			}
			text := comment.Text
			return !strings.Contains(text, "!linux") && !strings.Contains(text, "windows") && !strings.Contains(text, "darwin") && !strings.Contains(text, "ignore")
		}
	}
	return true
}
