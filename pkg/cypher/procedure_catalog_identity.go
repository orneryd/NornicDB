package cypher

import (
	"encoding/base64"
	"errors"
	"strings"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// migrateProcedureCatalogIdentity moves an older lowercase key to the exact
// name stored in its validated payload. A newer exact-name record wins over
// a leftover legacy copy; create-before-delete keeps failures retryable.
func (e *StorageExecutor) migrateProcedureCatalogIdentity(store storage.Engine, node *storage.Node, record persistedProcedureRecord) (bool, error) {
	canonical := procedureCatalogNodeID(record.Name)
	if node.ID == canonical || node.ID != storage.NodeID(procedureCatalogPrefix+lowerASCII(strings.TrimSpace(record.Name))) {
		return false, nil
	}
	existing, err := store.GetNode(canonical)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return false, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
	}
	superseded := existing != nil
	if !superseded {
		migrated := *node
		migrated.ID = canonical
		if _, err := store.CreateNode(&migrated); err != nil {
			return false, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
		}
	}
	if err := store.DeleteNode(node.ID); err != nil {
		return false, localizedError(localization.CypherProceduresDropFailed(record.Name, err), err)
	}
	return superseded, nil
}

// decodeProcedureCatalogRecord is shared by loading and mutation-time identity
// resolution; the serialized payload, rather than its historical key, names the procedure.
func decodeProcedureCatalogRecord(node *storage.Node) (persistedProcedureRecord, error) {
	var record persistedProcedureRecord
	invalid := func() (persistedProcedureRecord, error) {
		return record, localizedError(localization.CypherProceduresCatalogRecordDecodeFailed(string(node.ID)), nerrors.ErrProcedureCatalogRecordDecodeFailed)
	}
	var payload []byte
	switch raw := node.Properties["record"].(type) {
	case []byte:
		payload = raw
	case string:
		var err error
		payload, err = base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return invalid()
		}
	default:
		return invalid()
	}
	if err := util.DecodeMsgpackBytes(payload, &record); err != nil {
		return invalid()
	}
	return record, nil
}

// resolveProcedureCatalogIdentity normalizes the exact key and its old lowercase
// slot before any install/drop mutation. A legacy payload in the requested slot
// may belong to a different exact name: preserve it before reusing that slot.
func (e *StorageExecutor) resolveProcedureCatalogIdentity(store storage.Engine, name string) (*storage.Node, error) {
	canonical := procedureCatalogNodeID(name)
	legacy := procedureCatalogNodeID(lowerASCII(strings.TrimSpace(name)))
	keys := []storage.NodeID{canonical}
	if legacy != canonical {
		keys = append(keys, legacy)
	}
	for _, key := range keys {
		node, err := store.GetNode(key)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
		}
		if node == nil {
			continue
		}
		record, err := decodeProcedureCatalogRecord(node)
		if err != nil {
			return nil, err
		}
		if _, _, _, err := e.compilePersistedProcedure(record); err != nil {
			return nil, localizedError(localization.CypherProceduresCatalogRecordInvalid(string(node.ID)), nerrors.ErrProcedureCatalogRecordInvalid)
		}
		if _, err := e.migrateProcedureCatalogIdentity(store, node, record); err != nil {
			return nil, err
		}
	}
	node, err := store.GetNode(canonical)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
	}
	if node == nil {
		return nil, nil
	}
	record, err := decodeProcedureCatalogRecord(node)
	if err != nil {
		return nil, err
	}
	if record.Name != name {
		return nil, localizedError(localization.CypherProceduresCatalogRecordInvalid(string(node.ID)), nerrors.ErrProcedureCatalogRecordInvalid)
	}
	return node, nil
}
