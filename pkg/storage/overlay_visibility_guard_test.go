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
	"BadgerEngine.replayWriteBehind":       "the flush: lands a drained generation's commits in storage",
	"BadgerEngine.FlushWriteBehind":        "captures the generations rotated before the flush, to wait for their delivery",
	"BadgerTransaction.settleBufferedNode": "asks whether the buffer holds the node, to land it first",
}

// Every read that merges the write-behind overlay with storage takes the
// buffer's bodies through the visibility composition: scan adapters in
// overlay_visibility.go, or entityStateLocked with an explicit complete read
// policy. A new raw body reader outside those owners fails here.
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
			calls := make(map[*ast.SelectorExpr]*ast.CallExpr)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if selected, ok := call.Fun.(*ast.SelectorExpr); ok {
						calls[selected] = call
					}
				}
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
				case types.MethodVal, types.MethodExpr:
					if sig, ok := selection.Type().(*types.Signature); !ok || !carriesBodies(sig) {
						return true
					}
					if sel.Sel.Name == "entityStateLocked" && overlayReadHasVisibility(calls[sel], selection.Kind() == types.MethodExpr) {
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
	require.Empty(t, readers, "overlay bodies read outside the canonical visibility composition")
	for name := range overlayBodyReadersOutsideComposition {
		require.True(t, used[name], "classified overlay reader %s no longer reads the buffer", name)
	}
}

// overlayReadHasVisibility accepts only a direct canonical point read with a
// complete stack policy. A raw method value, omitted policy, policy variable,
// or partial/nil literal cannot silently become a visible body read.
func overlayReadHasVisibility(call *ast.CallExpr, receiverArgument bool) bool {
	if call == nil {
		return false
	}
	policyIndex := 2
	if receiverArgument {
		policyIndex++
	}
	if len(call.Args) != policyIndex+1 {
		return false
	}
	policy, ok := call.Args[policyIndex].(*ast.CompositeLit)
	if !ok || len(policy.Elts) != 2 {
		return false
	}
	policyType, ok := policy.Type.(*ast.Ident)
	if !ok || policyType.Name != "bufferedReadVisibility" {
		return false
	}
	fields := make(map[string]bool, 2)
	for _, value := range policy.Elts {
		field, ok := value.(*ast.KeyValueExpr)
		if !ok {
			return false
		}
		key, ok := field.Key.(*ast.Ident)
		if !ok || (key.Name != "engine" && key.Name != "visibility") || fields[key.Name] {
			return false
		}
		if value, ok := field.Value.(*ast.Ident); ok && value.Name == "nil" {
			return false
		}
		fields[key.Name] = true
	}
	return fields["engine"] && fields["visibility"]
}

func TestOverlayVisibilityReadRequiresCompletePolicy(t *testing.T) {
	for _, tc := range []struct {
		name, expression string
		receiverArgument bool
		allowed          bool
	}{
		{"visible", "w.entityStateLocked(kind, id, bufferedReadVisibility{engine: b, visibility: v})", false, true},
		{"stored", "w.entityStateLocked(kind, id, bufferedReadVisibility{visibility: storedRead, engine: b})", false, true},
		{"method-expression", "(*WriteBehindBuffer).entityStateLocked(w, kind, id, bufferedReadVisibility{engine: b, visibility: v})", true, true},
		{"raw-point", "w.entityStateLocked(kind, id)", false, false},
		{"raw-method-expression", "(*WriteBehindBuffer).entityStateLocked(w, kind, id)", true, false},
		{"nil-policy", "w.entityStateLocked(kind, id, nil)", false, false},
		{"variable-policy", "w.entityStateLocked(kind, id, policy)", false, false},
		{"missing-engine", "w.entityStateLocked(kind, id, bufferedReadVisibility{visibility: v})", false, false},
		{"missing-visibility", "w.entityStateLocked(kind, id, bufferedReadVisibility{engine: b})", false, false},
		{"nil-engine", "w.entityStateLocked(kind, id, bufferedReadVisibility{engine: nil, visibility: v})", false, false},
		{"nil-visibility", "w.entityStateLocked(kind, id, bufferedReadVisibility{engine: b, visibility: nil})", false, false},
		{"duplicate-engine", "w.entityStateLocked(kind, id, bufferedReadVisibility{engine: b, engine: b})", false, false},
		{"positional-policy", "w.entityStateLocked(kind, id, bufferedReadVisibility{b, v})", false, false},
		{"other-policy", "w.entityStateLocked(kind, id, otherPolicy{engine: b, visibility: v})", false, false},
		{"extra-argument", "w.entityStateLocked(kind, id, bufferedReadVisibility{engine: b, visibility: v}, policy)", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expression, err := parser.ParseExpr(tc.expression)
			require.NoError(t, err)
			require.Equal(t, tc.allowed, overlayReadHasVisibility(expression.(*ast.CallExpr), tc.receiverArgument))
		})
	}
	require.False(t, overlayReadHasVisibility(nil, false), "a method value carries no read policy")
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
