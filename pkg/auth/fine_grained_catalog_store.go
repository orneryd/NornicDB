package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const fineGrainedCatalogID storage.NodeID = "fine_grained_entitlement_catalog"

const fineGrainedCatalogVersion = 1

// ErrFineGrainedCatalogChanged means the catalog changed since the editor read
// it. The editor must reload before applying another replacement.
var ErrFineGrainedCatalogChanged = errors.New("fine-grained entitlement catalog changed")

type fineGrainedCatalogRecord struct {
	Version int                `json:"version"`
	Catalog FineGrainedCatalog `json:"catalog"`
}

// ReadFineGrainedCatalog reads a committed policy document and its revision.
// An empty revision means that no catalog has been created.
func (r *RoleStore) ReadFineGrainedCatalog(ctx context.Context) (FineGrainedCatalog, string, error) {
	return readFineGrainedCatalog(ctx, r.storage)
}

// ReplaceFineGrainedCatalog validates definitions and role references and
// persists them as one system transaction if expectedRevision still matches.
// This is a storage operation. Its caller must authorize policy administration.
//
// Read the current revision before editing. Use an empty expected revision to
// create the initial catalog.
func (r *RoleStore) ReplaceFineGrainedCatalog(ctx context.Context, expectedRevision string, actions []FineGrainedAction, catalog FineGrainedCatalog) error {
	if _, err := CompileFineGrainedPolicy(actions, catalog); err != nil {
		return err
	}
	roles := make([]string, 0, len(catalog.Assignments)+len(catalog.Privileges))
	for role := range catalog.Assignments {
		if NormalizeRoleName(role) != FineGrainedPublicRole {
			roles = append(roles, role)
		}
	}
	for _, privilege := range catalog.Privileges {
		if NormalizeRoleName(privilege.Role) != FineGrainedPublicRole {
			roles = append(roles, privilege.Role)
		}
	}
	if err := r.writes.lock(ctx); err != nil {
		return err
	}
	defer r.writes.unlock()
	return WithRoleRecords(ctx, r.storage, roles, func(view storage.Engine) error {
		_, revision, err := readFineGrainedCatalog(ctx, view)
		if err != nil {
			return err
		}
		if revision != expectedRevision {
			return ErrFineGrainedCatalogChanged
		}
		return writeFineGrainedCatalog(ctx, view, catalog)
	})
}

func readFineGrainedCatalog(ctx context.Context, view storage.Engine) (FineGrainedCatalog, string, error) {
	node, err := view.GetNode(ctx, fineGrainedCatalogID)
	if errors.Is(err, storage.ErrNotFound) {
		return FineGrainedCatalog{}, "", nil
	}
	if err != nil {
		return FineGrainedCatalog{}, "", err
	}
	raw, ok := node.Properties["catalog"].(string)
	var record fineGrainedCatalogRecord
	if !ok || convert.PropertyJSON.Unmarshal([]byte(raw), &record) != nil || record.Version != fineGrainedCatalogVersion {
		return FineGrainedCatalog{}, "", fmt.Errorf("%w: malformed or unsupported stored catalog", ErrInvalidFineGrainedPolicy)
	}
	hash := sha256.Sum256([]byte(raw))
	return record.Catalog, hex.EncodeToString(hash[:]), nil
}

func writeFineGrainedCatalog(ctx context.Context, view storage.Engine, catalog FineGrainedCatalog) error {
	raw, err := convert.PropertyJSON.MarshalState(fineGrainedCatalogRecord{Version: fineGrainedCatalogVersion, Catalog: catalog})
	if err != nil {
		return err
	}
	return putRoleRecord(ctx, view, &storage.Node{
		ID:         fineGrainedCatalogID,
		Labels:     []string{"_System", "_FineGrainedEntitlementCatalog"},
		Properties: map[string]any{"catalog": string(raw)},
	}, false)
}

// changeFineGrainedRole follows the existing role and membership transaction.
// Definitions are reusable, so only the role's references move or disappear.
func changeFineGrainedRole(ctx context.Context, view storage.Engine, oldName, newName string, remove bool) error {
	catalog, _, err := readFineGrainedCatalog(ctx, view)
	if err != nil {
		return err
	}
	changed := false
	for name, ids := range catalog.Assignments {
		if NormalizeRoleName(name) != oldName {
			continue
		}
		delete(catalog.Assignments, name)
		if !remove {
			catalog.Assignments[newName] = ids
		}
		changed = true
	}
	filtered := catalog.Privileges[:0]
	for _, privilege := range catalog.Privileges {
		if NormalizeRoleName(privilege.Role) == oldName {
			changed = true
			if remove {
				continue
			}
			privilege.Role = newName
		}
		filtered = append(filtered, privilege)
	}
	catalog.Privileges = filtered
	if changed {
		return writeFineGrainedCatalog(ctx, view, catalog)
	}
	return nil
}
