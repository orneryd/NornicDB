package cypher

import (
	"errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

// A descriptor's display requires its manager's key, not a second decoder or
// access to the execution identity. Legacy registrations retain their readers.
func (e *StorageExecutor) constituentDisplayView(ref multidb.ConstituentRef) (multidb.ConstituentRef, error) {
	owner, ok := e.dbManager.(interface {
		RemoteConstituentForDisplay(multidb.ConstituentRef) (multidb.ConstituentRef, error)
	})
	if !ok {
		if ref.ConnectionV1 == "" {
			return ref, nil
		}
		return multidb.ConstituentRef{}, errors.New("remote connection display owner unavailable")
	}
	return owner.RemoteConstituentForDisplay(ref)
}
