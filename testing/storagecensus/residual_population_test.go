package storagecensus

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). This is client fixture data for
// the existing module-wide census API, not a private extractor or exemption.
func TestResidualPopulationFindsOutsidePackageWriter(t *testing.T) {
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	path := filepath.Join(abs, "internal", "testfixture", "storagefault", "zz_residual_writer.go")
	const source = `package storagefault
import badger "github.com/dgraph-io/badger/v4"
func ResidualUnorderedPut(txn *badger.Txn) error {
 return txn.Set([]byte{0x01, 'r', '7', '3'}, []byte("body"))
}
`
	report, err := Census(root, map[string][]byte{path: []byte(source)})
	require.NoError(t, err)
	found := false
	for _, write := range report.Writes {
		found = found || strings.Contains(write.Func, "ResidualUnorderedPut")
	}
	require.True(t, found, "new outside-package writer must enter the census population")
	rejected := false
	for _, violation := range report.Violations {
		rejected = rejected || strings.Contains(violation, "ResidualUnorderedPut")
	}
	require.True(t, rejected, "the census must reject that writer for its own effect")
}
