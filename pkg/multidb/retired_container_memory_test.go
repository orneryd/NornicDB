package multidb

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRetiredDatabaseReferencesCollectible(t *testing.T) {
	for _, constituent := range []bool{false, true} {
		t.Run(map[bool]string{false: "alias", true: "constituent"}[constituent], func(t *testing.T) {
			owner, err := NewDatabaseManager(storage.NewMemoryEngine(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = owner.Close() })
			reference := func() weak.Pointer[byte] {
				key := strings.Repeat("retired", 64)
				ref := weak.Make(unsafe.StringData(key))
				if constituent {
					owner.databases["owned"] = &DatabaseInfo{Name: "owned", Type: "composite", Constituents: []ConstituentRef{{Alias: key}}}
					require.NoError(t, owner.RemoveConstituent("owned", key))
				} else {
					owner.databases["owned"] = &DatabaseInfo{Name: "owned", Aliases: []string{key}}
					require.NoError(t, owner.DropAlias(key))
				}
				return ref
			}()
			runtime.GC()
			require.Nil(t, reference.Value())
			runtime.KeepAlive(owner)
		})
	}
}
