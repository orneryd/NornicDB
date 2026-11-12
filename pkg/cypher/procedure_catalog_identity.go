package cypher

import (
	"context"
	"encoding/base64"
	"errors"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/internal/queryprofile"
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
	canonical := procedureCatalogNodeID(name)
	nodes := supplied
	if len(supplied) == 0 {
		legacy := procedureCatalogNodeID(lowerASCII(strings.TrimSpace(name)))
		keys := []storage.NodeID{canonical}
		if legacy != canonical {
			keys = append(keys, legacy)
		}
		for _, key := range keys {
			node, err := readProcedureCatalogNode(ctx, store, key)
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
		existing, err := readProcedureCatalogNode(ctx, store, destinationID)
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
			counter := procedureCatalogMutationCounter(ctx, store, profileKindNodeCreateRequest)
			_, err := store.CreateNode(ctx, &migrated)
			queryprofile.RequestResult(counter, err)
			if err != nil {
				return nil, false, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
			}
		}
		counter := procedureCatalogMutationCounter(ctx, store, profileKindNodeDeleteRequest)
		err = store.DeleteNode(ctx, node.ID)
		queryprofile.RequestResult(counter, err)
		if err != nil {
			return nil, false, localizedError(localization.CypherProceduresDropFailed(record.Name, err), err)
		}
	}
	if len(supplied) != 0 {
		return nil, superseded, nil
	}
	node, err := readProcedureCatalogNode(ctx, store, canonical)
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

// Catalog identity and listing share one acquisition owner per read shape.
// Counts describe actual attempts/populations, before case and record filters.
func readProcedureCatalogNode(ctx context.Context, store storage.Engine, id storage.NodeID) (*storage.Node, error) {
	counter := queryprofile.Access(ctx, profileKindNodeByIdSeek, "")
	node, err := store.GetNode(ctx, id)
	queryprofile.EntityReadResult(counter, node, err)
	return node, err
}

func readProcedureCatalogNodes(ctx context.Context, store storage.Engine) ([]*storage.Node, error) {
	nodes, err := store.GetNodesByLabel(ctx, procedureCatalogLabel)
	queryprofile.EntityPopulation(ctx, profileKindNodeByLabelScan, procedureCatalogLabel, nodes)
	return nodes, err
}

// Native data transactions already own their mutation request accounting.
// Catalog commands may instead persist directly; report that actual request
// here without counting a transparent transaction adapter's same call twice.
func procedureCatalogMutationCounter(ctx context.Context, store storage.Engine, kind string) *queryprofile.ReadCounter {
	if queryprofile.FromContext(ctx) == nil || membershipOfStorage(store).storage != nil {
		return nil
	}
	return queryprofile.Access(ctx, kind, "")
}
