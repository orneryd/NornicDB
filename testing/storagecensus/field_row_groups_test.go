// Author: declared-list-owners lane (Claude), 2026-10-30.

package storagecensus

import (
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// separateEqualRows names each pair of fields whose rows declare the same
// class, owners and readers through separate lists rather than one shared row.
func separateEqualRows(rows map[string]fieldRow) []string {
	names := make([]string, 0, len(rows))
	for name, row := range rows {
		if len(row.owners) > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var separate []string
	for i, a := range names {
		for _, b := range names[i+1:] {
			x, y := rows[a], rows[b]
			if x.class != y.class || !slices.Equal(x.owners, y.owners) || !slices.Equal(x.readers, y.readers) {
				continue
			}
			if &x.owners[0] != &y.owners[0] || (len(x.readers) > 0 && &x.readers[0] != &y.readers[0]) {
				separate = append(separate, a+" = "+b)
			}
		}
	}
	return separate
}

// TestFieldOwnershipGroupsHaveOneRow proves that fields sharing one ownership
// contract reference one row, and that the census catches a copied row.
func TestFieldOwnershipGroupsHaveOneRow(t *testing.T) {
	require.Empty(t, separateEqualRows(fieldClasses))
	for _, field := range []string{"pkg/multidb.DatabaseInfo.nodeSize", "pkg/multidb.DatabaseInfo.edgeSize", "pkg/multidb.DatabaseInfo.totalSize"} {
		require.Equal(t, "quota", fieldClasses[field].class)
	}

	planted := make(map[string]fieldRow, len(fieldClasses)+1)
	for name, row := range fieldClasses {
		planted[name] = row
	}
	size := fieldClasses["pkg/multidb.DatabaseInfo.nodeSize"]
	planted["pkg/multidb.DatabaseInfo.copiedSize"] = fieldRow{class: size.class, owners: slices.Clone(size.owners), readers: slices.Clone(size.readers)}
	separate := separateEqualRows(planted)
	require.Len(t, separate, 3)
	for _, pair := range separate {
		require.Contains(t, pair, "copiedSize")
	}
}
