package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

func TestPreparedCompositePrefixCursorClassificationIsExact(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
 type preparedCompositeNodeKeys struct { prefixCount int; unknownProgress int; unknownOffsets []int }
 func(k *preparedCompositeNodeKeys)appendPrefix(){k.prefixCount++;k.unknownProgress++;k.unknownOffsets=append(k.unknownOffsets,k.prefixCount)}
 type preparedCompositeNodeKeysLookalike struct{prefixCount int}
 func(k *preparedCompositeNodeKeysLookalike)advance(){k.prefixCount++}
 func CensusFixtureUses(k *preparedCompositeNodeKeys, look *preparedCompositeNodeKeysLookalike){k.appendPrefix();look.advance()}
 `, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
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
