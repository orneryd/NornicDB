package fabric

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// LocationSourceKey joins the storage/native physical-location owner. Fragment
// admission and native provenance share the same physical location identity.
// Existing local and remote key strings remain unchanged.
func LocationSourceKey(location Location) string {
	switch location := location.(type) {
	case *LocationLocal:
		return storage.PhysicalLocationKey("", location.DBName, false)
	case *LocationRemote:
		return storage.PhysicalLocationKey(location.URI, location.DBName, true)
	default:
		return fmt.Sprintf("unknown:%T", location)
	}
}
