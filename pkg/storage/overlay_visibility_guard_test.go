package storage

import (
	"go/ast"
	"go/parser"
	"go/types"
	"path/filepath"

	"github.com/orneryd/nornicdb/internal/workcensus"
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
	readers, used := overlayBodyReaders(t, nil)
	require.Empty(t, readers, "overlay bodies read outside the canonical visibility composition")
	for name := range overlayBodyReadersOutsideComposition {
		require.True(t, used[name], "classified overlay reader %s no longer reads the buffer", name)
	}
}

// Generic parsing, typing, source traversal and owner identity are supplied by
// workcensus. This client interprets only the storage visibility policy.
func overlayBodyReaders(t *testing.T, overlay map[string][]byte) ([]string, map[string]bool) {
	t.Helper()
	const pkgPath = "github.com/orneryd/nornicdb/pkg/storage"
	inv, err := workcensus.Extract(workcensus.Config{
		Dir: "../..", Patterns: []string{"./pkg/storage"}, Packages: []string{pkgPath},
		BuildFlags: []string{"-tags=noui,nolocalllm"}, SourceOnly: true, IncludeSyntax: true,
		Overlay: overlay, Selectors: []workcensus.Selector{{Kind: "overlay reference", Node: "identifier", Reachability: "all"}},
	})
	require.NoError(t, err)
	isBuffer := func(typ types.Type) bool {
		if ptr, ok := typ.(*types.Pointer); ok {
			typ = ptr.Elem()
		}
		named, ok := typ.(*types.Named)
		return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == pkgPath && (named.Obj().Name() == "WriteBehindBuffer" || named.Obj().Name() == "CommitBuffer")
	}
	bodyType := func(typ types.Type) bool {
		for _, body := range []string{".Node", ".Edge", ".bufferedEntityState", ".bufferedCommit", ".bufferedMutation", ".CommitBuffer", ".pendingNodeIndex"} {
			if strings.Contains(typ.String(), body) {
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
	for _, member := range inv.Members {
		site := member.Syntax
		if site == nil || site.Info == nil || filepath.Base(member.File) == "overlay_visibility.go" {
			continue
		}
		id, ok := site.Node.(*ast.Ident)
		if !ok {
			continue
		}
		var selected *ast.SelectorExpr
		var call *ast.CallExpr
		var fn *ast.FuncDecl
		for _, ancestor := range site.Ancestors {
			switch node := ancestor.(type) {
			case *ast.SelectorExpr:
				if node.Sel == id {
					selected = node
				}
			case *ast.CallExpr:
				if sel, ok := node.Fun.(*ast.SelectorExpr); ok && sel.Sel == id {
					call = node
				}
			case *ast.FuncDecl:
				fn = node
			}
		}
		if fn == nil {
			fn, _ = site.Owner.(*ast.FuncDecl)
		}
		if selected == nil || fn == nil {
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
		selection := site.Info.Selections[selected]
		if selection == nil || !isBuffer(selection.Recv()) {
			continue
		}
		switch selection.Kind() {
		case types.FieldVal:
			if !bodyType(selection.Type()) {
				continue
			}
		case types.MethodVal, types.MethodExpr:
			if sig, ok := selection.Type().(*types.Signature); !ok || !carriesBodies(sig) {
				continue
			}
			if selected.Sel.Name == "entityStateLocked" && overlayReadHasVisibility(call, selection.Kind() == types.MethodExpr) {
				continue
			}
		default:
			continue
		}
		if _, classified := overlayBodyReadersOutsideComposition[name]; classified {
			used[name] = true
			continue
		}
		readers = append(readers, name+" uses "+selected.Sel.Name+" at "+site.FileSet.Position(selected.Pos()).String())
	}
	sort.Strings(readers)
	return readers, used
}

func TestOverlayVisibilitySourceControl(t *testing.T) {
	path, err := filepath.Abs("census_overlay_probe.go")
	require.NoError(t, err)
	readers, _ := overlayBodyReaders(t, map[string][]byte{path: []byte(`package storage
func censusRawOverlay(b *BadgerEngine) []*Node {
 nodes, _ := b.writeBehind.allNodesOverlayLocked()
 return nodes
}
`)})
	require.Len(t, readers, 1)
	require.Contains(t, readers[0], "censusRawOverlay uses allNodesOverlayLocked")
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
