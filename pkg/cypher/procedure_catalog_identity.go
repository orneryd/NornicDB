package cypher

import (
	"errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// migrateProcedureCatalogIdentity moves an older lowercase key to the exact
// name stored in its validated payload. A newer exact-name record wins over
// a leftover legacy copy; create-before-delete keeps failures retryable.
func (e *StorageExecutor) migrateProcedureCatalogIdentity(node *storage.Node, record persistedProcedureRecord) (bool, error) {
	canonical := procedureCatalogNodeID(record.Name)
	if node.ID == canonical || node.ID != storage.NodeID(procedureCatalogPrefix+lowerASCII(strings.TrimSpace(record.Name))) {
		return false, nil
	}
	existing, err := e.storage.GetNode(canonical)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return false, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
	}
	superseded := existing != nil
	if !superseded {
		migrated := *node
		migrated.ID = canonical
		if _, err := e.storage.CreateNode(&migrated); err != nil {
			return false, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
		}
	}
	if err := e.storage.DeleteNode(node.ID); err != nil {
		return false, localizedError(localization.CypherProceduresDropFailed(record.Name, err), err)
	}
	return superseded, nil
}
