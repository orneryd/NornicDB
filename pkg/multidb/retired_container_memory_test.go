package multidb

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRetiredDatabaseReferencesCollectible(t *testing.T) {
	for _, constituent := range []bool{false, true} {
		t.Run(map[bool]string{false: "alias", true: "constituent"}[constituent], func(t *testing.T) {
			owner, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
			require.NoError(t, err)
			fixtureowner.Own(t, owner)
			reference := func() weak.Pointer[byte] {
				key := strings.Repeat("retired", 64)
				ref := weak.Make(unsafe.StringData(key))
				if constituent {
					owner.databases["owned"] = &DatabaseInfo{Name: "owned", Type: "composite", Constituents: []ConstituentRef{{Alias: key}}}
					require.NoError(t, owner.RemoveConstituent(t.Context(), "owned", key))
				} else {
					owner.databases["owned"] = &DatabaseInfo{Name: "owned", Aliases: []string{key}}
					require.NoError(t, owner.DropAlias(t.Context(), key))
				}
				return ref
			}()
			runtime.GC()
			require.Nil(t, reference.Value())
			runtime.KeepAlive(owner)
		})
	}
}
