package storage

// Snapshot Edge encoder census author: Claude (storage-format-owners lane), 30 October 2026.

import (
	"bytes"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/printer"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// snapshotEdgeRecordEncoders lists the compiled functions that produce the
// snapshot Edge record: a hand-written encoder of its field names, or a direct
// reflected msgpack encoding of an Edge value. SSA sees renamed copies too.
func snapshotEdgeRecordEncoders(built storageSSABuild) []string {
	edge := built.pkg.Scope().Lookup("Edge").Type()
	isEdge := func(t types.Type) bool {
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		return types.Identical(t, edge)
	}
	isEncoder := func(fn *ssa.Function) bool {
		if fn.Pkg != nil && strings.HasSuffix(fn.Pkg.Pkg.Path(), "vmihailenco/msgpack/v5") {
			return true
		}
		recv := fn.Signature.Recv()
		return recv != nil && strings.Contains(recv.Type().String(), "vmihailenco/msgpack/v5.Encoder")
	}
	var out []string
	for fn := range ssautil.AllFunctions(built.prog) {
		if fn.Pkg != built.ssa {
			continue
		}
		names := map[string]bool{}
		encodes, reflected := false, false
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				for _, operand := range instr.Operands(nil) {
					if c, ok := (*operand).(*ssa.Const); ok && c.Value != nil && c.Value.Kind() == constant.String {
						names[constant.StringVal(c.Value)] = true
					}
				}
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				callee := call.Common().StaticCallee()
				if callee == nil || !isEncoder(callee) {
					continue
				}
				encodes = true
				for _, arg := range call.Common().Args {
					if boxed, ok := arg.(*ssa.MakeInterface); ok && isEdge(boxed.X.Type()) {
						reflected = true
					}
				}
			}
		}
		if encodes && (reflected || names["StartNode"] && names["EndNode"] && names["VisibilitySuppressed"]) {
			out = append(out, fn.String())
		}
	}
	sort.Strings(out)
	return out
}

// renamedSnapshotEdgeEncoder is encodeSnapshotEdge's own source under another
// name: the planted copy the census must report.
func renamedSnapshotEdgeEncoder(t *testing.T) string {
	t.Helper()
	fset := token.NewFileSet()
	source, err := os.ReadFile("snapshot_edge_transcoder.go")
	require.NoError(t, err)
	file, err := parser.ParseFile(fset, "snapshot_edge_transcoder.go", source, 0)
	require.NoError(t, err)
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == "encodeSnapshotEdge" {
			fn.Name.Name = "plantedSnapshotEdgeCopy"
			var out bytes.Buffer
			require.NoError(t, printer.Fprint(&out, fset, fn))
			return `import "github.com/vmihailenco/msgpack/v5"` + "\n" + out.String()
		}
	}
	t.Fatal("encodeSnapshotEdge not found")
	return ""
}

func TestSnapshotEdgeRecordHasOneEncoder(t *testing.T) {
	population := loadStorageSSAPopulation(t)
	owner := "github.com/orneryd/nornicdb/pkg/storage.encodeSnapshotEdge"
	require.Equal(t, []string{owner}, snapshotEdgeRecordEncoders(population.build(t, "", "")))
	for name, control := range map[string]struct{ source, planted string }{
		"renamed copy": {renamedSnapshotEdgeEncoder(t), "github.com/orneryd/nornicdb/pkg/storage.plantedSnapshotEdgeCopy"},
		"reflected":    {`import "github.com/vmihailenco/msgpack/v5"` + "\nfunc plantedReflectedEdge(enc *msgpack.Encoder, edge *Edge) error { return enc.Encode(edge) }", "github.com/orneryd/nornicdb/pkg/storage.plantedReflectedEdge"},
	} {
		t.Run(name, func(t *testing.T) {
			got := snapshotEdgeRecordEncoders(population.build(t, "snapshot_edge_census_mutation.go", control.source))
			require.ElementsMatch(t, []string{owner, control.planted}, got, "the census must report the planted second encoder")
		})
	}
}
