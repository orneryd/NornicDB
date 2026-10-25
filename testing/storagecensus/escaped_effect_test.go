package storagecensus

// Author: #1667 implementation lane (Codex). The same physical callback effect
// may execute synchronously and escape; the safe path cannot erase the escape.

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestEscapingEffectCannotBorrowSynchronousCoverage(t *testing.T) {
	for _, escaping := range []bool{false, true} {
		t.Run(fmt.Sprint(escaping), func(t *testing.T) {
			dir := t.TempDir()
			store := ""
			if escaping {
				store = "saved=f"
			}
			for name, source := range map[string]string{
				"go.mod":           "module github.com/orneryd/nornicdb\n\ngo 1.26\nrequire github.com/dgraph-io/badger/v4 v4.0.0\nreplace github.com/dgraph-io/badger/v4 => ./badger\n",
				"badger/go.mod":    "module github.com/dgraph-io/badger/v4\n\ngo 1.26\n",
				"badger/source.go": "package badger\ntype Txn struct{}\nfunc(*Txn)Set(key,value []byte)error{return nil}\n",
				"pkg/storage/source.go": `package storage
import("sync";badger "github.com/dgraph-io/badger/v4")
const prefixMVCCMeta byte=0xff
const prefixMVCCMetaLabelCount byte=2
type BadgerEngine struct{labelCountWriteMu sync.Mutex}
var saved func([]byte,[]byte)error
func invoke(f func([]byte,[]byte)error)error{return f([]byte{prefixMVCCMeta,prefixMVCCMetaLabelCount},nil)}
func helper(tx *badger.Txn){
 f:=tx.Set
 _=invoke(f)
 ` + store + `
}
func Root(b *BadgerEngine,tx *badger.Txn){b.labelCountWriteMu.Lock();defer b.labelCountWriteMu.Unlock();helper(tx)}
`,
			} {
				path := filepath.Join(dir, name)
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
				require.NoError(t, os.WriteFile(path, []byte(source), 0600))
			}
			r, err := Census(dir, nil)
			require.NoError(t, err)
			found := false
			for _, row := range r.Unordered {
				if row.Space != "prefixMVCCMetaLabelCount" {
					continue
				}
				for _, entry := range row.Entries {
					found = found || entry == "pkg/storage.Root"
				}
			}
			require.Equal(t, escaping, found, "caller hold covers synchronous invocation, never a stored callback's future invocation")
		})
	}
}
