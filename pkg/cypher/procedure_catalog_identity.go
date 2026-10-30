package cypher

import (
	"context"
	"encoding/base64"
	"errors"
	"slices"
	"strings"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

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

// resolveProcedureCatalogIdentity owns exact-name resolution and catalog key
// normalization for both mutation-time lookup and a record already being loaded.
// Existing exact records win; create-before-delete keeps failed moves retryable.
func (e *StorageExecutor) resolveProcedureCatalogIdentity(ctx context.Context, store storage.Engine, name string, supplied ...*storage.Node) (*storage.Node, bool, error) {
	store, err := admittedStatementWriter(ctx, store)
	if err != nil {
		return nil, false, err
	}
	canonical := procedureCatalogNodeID(name)
	nodes := supplied
	if len(supplied) == 0 {
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
				return nil, false, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
			}
			if node != nil {
				nodes = append(nodes, node)
			}
		}
	}
	superseded := false
	for _, node := range nodes {
		record, err := decodeProcedureCatalogRecord(node)
		if err != nil {
			return nil, false, err
		}
		if _, _, _, err := e.compilePersistedProcedure(ctx, record); err != nil {
			return nil, false, localizedError(localization.CypherProceduresCatalogRecordInvalid(string(node.ID)), nerrors.ErrProcedureCatalogRecordInvalid)
		}
		destinationID := procedureCatalogNodeID(record.Name)
		if node.ID == destinationID || node.ID != storage.NodeID(procedureCatalogPrefix+lowerASCII(strings.TrimSpace(record.Name))) {
			continue
		}
		existing, err := store.GetNode(destinationID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, false, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
		}
		superseded = existing != nil
		if superseded {
			destination, err := decodeProcedureCatalogRecord(existing)
			if err != nil {
				return nil, false, err
			}
			if destination.Name != record.Name || !slices.Contains(existing.Labels, procedureCatalogLabel) {
				return nil, false, localizedError(localization.CypherProceduresCatalogRecordInvalid(string(existing.ID)), nerrors.ErrProcedureCatalogRecordInvalid)
			}
			if _, _, _, err := e.compilePersistedProcedure(ctx, destination); err != nil {
				return nil, false, localizedError(localization.CypherProceduresCatalogRecordInvalid(string(existing.ID)), nerrors.ErrProcedureCatalogRecordInvalid)
			}
		} else {
			migrated := *node
			migrated.ID = destinationID
			if _, err := store.CreateNode(&migrated); err != nil {
				return nil, false, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
			}
		}
		if err := store.DeleteNode(node.ID); err != nil {
			return nil, false, localizedError(localization.CypherProceduresDropFailed(record.Name, err), err)
		}
	}
	if len(supplied) != 0 {
		return nil, superseded, nil
	}
	node, err := store.GetNode(canonical)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
	}
	if node == nil {
		return nil, false, nil
	}
	record, err := decodeProcedureCatalogRecord(node)
	if err != nil {
		return nil, false, err
	}
	if record.Name != name {
		return nil, false, localizedError(localization.CypherProceduresCatalogRecordInvalid(string(node.ID)), nerrors.ErrProcedureCatalogRecordInvalid)
	}
	return node, superseded, nil
}

// admittedStatementWriter binds a statement's direct writes (outside the
// statement transaction) to the admission its request context carries.
func admittedStatementWriter(ctx context.Context, store storage.Engine) (storage.Engine, error) {
	origin, admitted := storage.VectorWorkOrigin(ctx)
	if !admitted {
		return nil, storage.ErrVectorWorkNotAdmitted
	}
	return storage.EngineWithVectorWorkOrigin(store, origin)
}
