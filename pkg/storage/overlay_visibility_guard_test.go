package storage

import (
	"go/ast"
	"go/parser"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
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
// overlayBodyCompositionProblems interprets the canonical typed population.
// It neither loads/parses files nor restricts consumers to the storage package.
func overlayBodyCompositionProblems(inventory workcensus.Inventory) ([]string, map[string]bool) {
	isBuffer := func(typ types.Type) bool {
		return readCensusNamed(typ, "WriteBehindBuffer") || readCensusNamed(typ, "CommitBuffer")
	}
	var carriesBody func(types.Type, map[types.Type]bool) bool
	carriesBody = func(typ types.Type, seen map[types.Type]bool) bool {
		if typ == nil || seen[typ] {
			return false
		}
		seen[typ] = true
		typ = types.Unalias(typ)
		if named, ok := typ.(*types.Named); ok {
			if named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "github.com/orneryd/nornicdb/pkg/storage" {
				switch named.Obj().Name() {
				case "Node", "Edge", "bufferedEntityState", "bufferedCommit", "bufferedMutation", "CommitBuffer", "pendingNodeIndex":
					return true
				}
			}
			return carriesBody(named.Underlying(), seen)
		}
		switch value := typ.(type) {
		case *types.Pointer:
			return carriesBody(value.Elem(), seen)
		case *types.Slice:
			return carriesBody(value.Elem(), seen)
		case *types.Array:
			return carriesBody(value.Elem(), seen)
		case *types.Map:
			return carriesBody(value.Elem(), seen)
		case *types.Struct:
			for i := 0; i < value.NumFields(); i++ {
				if carriesBody(value.Field(i).Type(), seen) {
					return true
				}
			}
		}
		return false
	}
	var problems []string
	used := map[string]bool{}
	seen := map[string]bool{}
	declarations := readCensusDeclarations(inventory)
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil || readCensusPath(member) == "pkg/storage/overlay_visibility.go" {
			continue
		}
		var selected *ast.SelectorExpr
		switch node := member.Syntax.Node.(type) {
		case *ast.CallExpr:
			selected, _ = node.Fun.(*ast.SelectorExpr)
		case *ast.Ident:
			for _, ancestor := range member.Syntax.Ancestors {
				if candidate, ok := ancestor.(*ast.SelectorExpr); ok && candidate.Sel == node {
					selected = candidate
				}
			}
		}
		if selected == nil {
			continue
		}
		identity := readCensusPath(member) + ":" + strconv.Itoa(int(selected.Pos()))
		if seen[identity] {
			continue
		}
		seen[identity] = true
		selection := member.Syntax.Info.Selections[selected]
		if selection == nil {
			continue
		}
		buffer := isBuffer(selection.Recv())
		if signature, ok := selection.Obj().Type().(*types.Signature); ok && signature.Recv() != nil {
			buffer = buffer || isBuffer(signature.Recv().Type())
		}
		if !buffer {
			continue
		}
		fn := declarations[member.OwnerID]
		name := member.Owner
		if fn != nil {
			name = fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				typ := member.Syntax.Info.TypeOf(fn.Recv.List[0].Type)
				if isBuffer(typ) {
					continue
				}
				typ = types.Unalias(typ)
				if pointer, ok := typ.(*types.Pointer); ok {
					typ = types.Unalias(pointer.Elem())
				}
				if named, ok := typ.(*types.Named); ok {
					name = named.Obj().Name() + "." + name
				}
			}
		}
		body := false
		switch selection.Kind() {
		case types.FieldVal:
			body = carriesBody(selection.Type(), map[types.Type]bool{})
		case types.MethodVal, types.MethodExpr:
			if signature, ok := selection.Type().(*types.Signature); ok {
				for i := 0; i < signature.Results().Len(); i++ {
					body = body || carriesBody(signature.Results().At(i).Type(), map[types.Type]bool{})
				}
			}
			var call *ast.CallExpr
			if candidate, ok := member.Syntax.Node.(*ast.CallExpr); ok && candidate.Fun == selected {
				call = candidate
			}
			for _, ancestor := range member.Syntax.Ancestors {
				if candidate, ok := ancestor.(*ast.CallExpr); ok && candidate.Fun == selected {
					call = candidate
				}
			}
			if selected.Sel.Name == "entityStateLocked" && overlayReadHasVisibility(call, selection.Kind() == types.MethodExpr) {
				continue
			}
		}
		if !body {
			continue
		}
		if _, allowed := overlayBodyReadersOutsideComposition[name]; allowed {
			used[name] = true
			continue
		}
		problems = append(problems, member.ID+": "+name+" uses "+selected.Sel.Name+" outside the shared overlay composition")
	}
	sort.Strings(problems)
	return problems, used
}

func TestOverlayBodiesAreReadThroughVisibility(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	problems, used := overlayBodyCompositionProblems(result.Inventory)
	require.Empty(t, problems, "overlay bodies read outside the canonical visibility composition")
	for name := range overlayBodyReadersOutsideComposition {
		require.True(t, used[name], "classified overlay reader %s no longer reads the buffer", name)
	}
}

func TestOverlayBodyOutsidePackageControl(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod":                "module github.com/orneryd/nornicdb\ngo 1.26.0\n",
		"pkg/storage/buffer.go": "package storage\ntype Node struct{}\ntype WriteBehindBuffer struct{}\nfunc(*WriteBehindBuffer)ReadBody()*Node{return nil}\n",
	}
	for path, source := range files {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(root, path), []byte(source), 0644))
	}
	baseline, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	problems, _ := overlayBodyCompositionProblems(baseline)
	require.Empty(t, problems)
	path := filepath.Join(root, "plugins", "unfamiliar", "read.go")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	require.NoError(t, os.WriteFile(path, []byte(`package unfamiliar
import raw "github.com/orneryd/nornicdb/pkg/storage"
type Alias = raw.WriteBehindBuffer
func opaque(buffer *Alias)*raw.Node{return buffer.ReadBody()}
`), 0644))
	changed, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	problems, _ = overlayBodyCompositionProblems(changed)
	require.NotEmpty(t, problems, "a new typed raw-body caller outside the owner package must fail the same gate")
	require.Contains(t, strings.Join(problems, "\n"), "plugins/unfamiliar/read.go:")
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
