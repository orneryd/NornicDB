package storagecensus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

func TestPreparedCompositePrefixCursorClassificationIsExact(t *testing.T) {
	// The field classifier resolves numeric types through ImportedPackage. The
	// generic ssaFixture deliberately builds a non-importable primary package;
	// register this checked fixture as importable rather than weakening lookup.
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "prepared_fields.go", `package storage
 type preparedCompositeNodeKeys struct { prefixCount int; unknownProgress int; unknownOffsets []int }
 func(k *preparedCompositeNodeKeys)appendPrefix(){k.prefixCount++;k.unknownProgress++;k.unknownOffsets=append(k.unknownOffsets,k.prefixCount)}
 type preparedCompositeNodeKeysLookalike struct{prefixCount int}
 func(k *preparedCompositeNodeKeysLookalike)advance(){k.prefixCount++}
 func CensusFixtureUses(k *preparedCompositeNodeKeys, look *preparedCompositeNodeKeysLookalike){k.appendPrefix();look.advance()}
 `, 0)
	require.NoError(t, err)
	checked, info, err := ssautil.BuildPackage(&types.Config{}, fset, types.NewPackage(Module+"pkg/storage", "storage"), []*ast.File{file}, ssa.SanityCheckFunctions)
	require.NoError(t, err)
	prog := ssa.NewProgram(fset, ssa.SanityCheckFunctions)
	pkg := prog.CreatePackage(checked.Pkg, []*ast.File{file}, info, true)
	pkg.Build()
	p := &program{fset: fset, ssa: prog}
	require.Same(t, pkg, prog.ImportedPackage(Module+"pkg/storage"))
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg == pkg {
			p.funcs = append(p.funcs, fn)
		}
	}
	analysis := &analysis{p: p}
	var fields []string
	for _, w := range analysis.fieldWrites() {
		fields = append(fields, w.field)
	}
	for _, field := range []string{"preparedCompositeNodeKeys.prefixCount", "preparedCompositeNodeKeys.unknownProgress", "preparedCompositeNodeKeys.unknownOffsets", "preparedCompositeNodeKeysLookalike.prefixCount"} {
		require.Contains(t, fields, "pkg/storage."+field, "fixture must actually expose each field write")
		_, typ := analysis.lookupField("pkg/storage." + field)
		require.NotNil(t, typ, "fixture numeric field types must be available")
		require.True(t, numeric(typ.Type()))
	}
	r := &Report{}
	analysis.checkFields(r)
	text := strings.Join(r.Violations, "\n")
	row, ok := classOf("pkg/storage.preparedCompositeNodeKeys.prefixCount")
	require.True(t, ok)
	require.Equal(t, "index-entry preparation prefix cursor", row.class)
	require.NotContains(t, text, "unclassified field: pkg/storage.preparedCompositeNodeKeys.prefixCount ")
	var accepted []FieldWriteRow
	for _, w := range r.FieldWrites {
		if w.Field == "pkg/storage.preparedCompositeNodeKeys.prefixCount" {
			accepted = append(accepted, w)
		}
	}
	require.Len(t, accepted, 1)
	require.Equal(t, "index-entry preparation prefix cursor", accepted[0].Class)
	require.Equal(t, "pkg/storage.preparedCompositeNodeKeys.appendPrefix", accepted[0].Func)
	for _, field := range []string{"preparedCompositeNodeKeys.unknownProgress", "preparedCompositeNodeKeys.unknownOffsets", "preparedCompositeNodeKeysLookalike.prefixCount"} {
		require.Contains(t, text, "unclassified field: pkg/storage."+field)
	}
}
