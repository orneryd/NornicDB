package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

func TestPreparedCompositePrefixCursorClassificationIsExact(t *testing.T) {
	p, pkg := ssaFixture(t, `package storage
 type preparedCompositeNodeKeys struct { prefixCount int; unknownProgress int; unknownOffsets []int }
 func(k *preparedCompositeNodeKeys)appendPrefix(){k.prefixCount++;k.unknownProgress++;k.unknownOffsets=append(k.unknownOffsets,k.prefixCount)}
 type preparedCompositeNodeKeysLookalike struct{prefixCount int}
 func(k *preparedCompositeNodeKeysLookalike)advance(){k.prefixCount++}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg == pkg {
			p.funcs = append(p.funcs, fn)
		}
	}
	r := &Report{}
	(&analysis{p: p}).checkFields(r)
	text := strings.Join(r.Violations, "\n")
	row, ok := classOf("pkg/storage.preparedCompositeNodeKeys.prefixCount")
	require.True(t, ok)
	require.Equal(t, "index-entry preparation prefix cursor", row.class)
	require.NotContains(t, text, "unclassified field: pkg/storage.preparedCompositeNodeKeys.prefixCount ")
	for _, field := range []string{"preparedCompositeNodeKeys.unknownProgress", "preparedCompositeNodeKeys.unknownOffsets", "preparedCompositeNodeKeysLookalike.prefixCount"} {
		require.Contains(t, text, "unclassified field: pkg/storage."+field)
	}
}
