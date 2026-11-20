package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const fineGrainedCatalogID storage.NodeID = "fine_grained_entitlement_catalog"

const fineGrainedCatalogVersion = 2

// ErrFineGrainedCatalogChanged means the catalog changed since the editor read
// it. The editor must reload before applying another replacement.
var ErrFineGrainedCatalogChanged = errors.New("fine-grained entitlement catalog changed")

type fineGrainedCatalogRecord struct {
	Version      int                  `json:"version"`
	Catalog      FineGrainedCatalog   `json:"catalog"`
	RoleCreation map[string]time.Time `json:"roleCreation,omitempty"`
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
	if err := r.writes.lock(ctx); err != nil {
		return err
	}
	defer r.writes.unlock()
	return WithRoleRecords(ctx, r.storage, fineGrainedCustomRoles(catalog), func(view storage.Engine) error {
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

// PrepareFineGrainedCatalog reads policy on activation and removes references
// to roles that no longer have the identity recorded with their assignments.
func (r *RoleStore) PrepareFineGrainedCatalog(ctx context.Context) (FineGrainedCatalog, string, error) {
	if err := r.writes.lock(ctx); err != nil {
		return FineGrainedCatalog{}, "", err
	}
	defer r.writes.unlock()
	var record fineGrainedCatalogRecord
	var revision string
	err := WithStoreTransaction(ctx, r.storage, func(view storage.Engine) error {
		var err error
		record, revision, err = fineGrainedCatalogAt(ctx, view)
		if err != nil || revision == "" {
			return err
		}
		changed, err := reconcileFineGrainedRoles(ctx, view, &record)
		if err == nil && changed {
			err = writeFineGrainedCatalog(ctx, view, record.Catalog)
			if err == nil {
				_, revision, err = fineGrainedCatalogAt(ctx, view)
			}
		}
		return err
	})
	return record.Catalog, revision, err
}

// reconcileFineGrainedRoles runs during activation only. A rename retains the
// stored creation time, while a replacement role has a new one. Older catalogs
// have no identity evidence and require explicit reassignment of custom grants.
func reconcileFineGrainedRoles(ctx context.Context, view storage.Engine, record *fineGrainedCatalogRecord) (bool, error) {
	rolesByName := make(map[string]time.Time)
	rolesByCreation := make(map[time.Time]string)
	if len(record.RoleCreation) > 0 {
		err := view.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{Prefix: rolePrefix, Projection: []string{}}, func(node *storage.Node) error {
			name := roleNameFromNodeID(string(node.ID))
			if name == "" || node.CreatedAt.IsZero() || !slices.Contains(node.Labels, roleLabel) {
				return nil
			}
			created := node.CreatedAt.UTC()
			rolesByName[name] = created
			if _, ambiguous := rolesByCreation[created]; ambiguous {
				name = ""
			}
			rolesByCreation[created] = name
			return nil
		})
		if err != nil {
			return false, err
		}
	}

	changed := record.Version != fineGrainedCatalogVersion
	currentName := func(role string) string {
		name := NormalizeRoleName(role)
		if name == FineGrainedPublicRole || IsBuiltinRole(name) {
			return role
		}
		created := record.RoleCreation[name]
		if !created.IsZero() && created.Equal(rolesByName[name]) {
			return role
		}
		changed = true
		return rolesByCreation[created]
	}
	catalog := &record.Catalog
	if len(catalog.Assignments) > 0 {
		assignments := make(map[string][]string, len(catalog.Assignments))
		for role, ids := range catalog.Assignments {
			if name := currentName(role); name != "" {
				assignments[name] = append(assignments[name], ids...)
			}
		}
		catalog.Assignments = assignments
	}
	privileges := catalog.Privileges[:0]
	for _, privilege := range catalog.Privileges {
		if name := currentName(privilege.Role); name != "" {
			privilege.Role = name
			privileges = append(privileges, privilege)
		}
	}
	catalog.Privileges = privileges
	return changed, nil
}

func fineGrainedCustomRoles(catalog FineGrainedCatalog) []string {
	unique := make(map[string]struct{}, len(catalog.Assignments))
	add := func(name string) {
		role := NormalizeRoleName(name)
		if role != FineGrainedPublicRole && !IsBuiltinRole(role) {
			unique[role] = struct{}{}
		}
	}
	for name := range catalog.Assignments {
		add(name)
	}
	for _, privilege := range catalog.Privileges {
		add(privilege.Role)
	}
	roles := make([]string, 0, len(unique))
	for role := range unique {
		roles = append(roles, role)
	}
	slices.Sort(roles)
	return roles
}

func fineGrainedRoleCreation(ctx context.Context, view storage.Engine, name string) (time.Time, error) {
	node, err := view.GetNode(ctx, storage.NodeID(rolePrefix+name))
	if errors.Is(err, storage.ErrNotFound) {
		return time.Time{}, &RoleNotFoundError{Role: name}
	}
	if err != nil {
		return time.Time{}, err
	}
	if !slices.Contains(node.Labels, roleLabel) || node.CreatedAt.IsZero() {
		return time.Time{}, &RoleNotFoundError{Role: name}
	}
	return node.CreatedAt, nil
}

func readFineGrainedCatalog(ctx context.Context, view storage.Engine) (FineGrainedCatalog, string, error) {
	record, revision, err := fineGrainedCatalogAt(ctx, view)
	return record.Catalog, revision, err
}

func fineGrainedCatalogAt(ctx context.Context, view storage.Engine) (fineGrainedCatalogRecord, string, error) {
	node, err := view.GetNode(ctx, fineGrainedCatalogID)
	if errors.Is(err, storage.ErrNotFound) {
		return fineGrainedCatalogRecord{}, "", nil
	}
	if err != nil {
		return fineGrainedCatalogRecord{}, "", err
	}
	raw, ok := node.Properties["catalog"].(string)
	var record fineGrainedCatalogRecord
	if !ok || convert.PropertyJSON.Unmarshal([]byte(raw), &record) != nil ||
		(record.Version != 1 && record.Version != fineGrainedCatalogVersion) {
		return fineGrainedCatalogRecord{}, "", fmt.Errorf("%w: malformed or unsupported stored catalog", ErrInvalidFineGrainedPolicy)
	}
	hash := sha256.Sum256([]byte(raw))
	return record, hex.EncodeToString(hash[:]), nil
}

func writeFineGrainedCatalog(ctx context.Context, view storage.Engine, catalog FineGrainedCatalog) error {
	record := fineGrainedCatalogRecord{Version: fineGrainedCatalogVersion, Catalog: catalog}
	roles := fineGrainedCustomRoles(catalog)
	if len(roles) > 0 {
		record.RoleCreation = make(map[string]time.Time, len(roles))
		for _, role := range roles {
			created, err := fineGrainedRoleCreation(ctx, view, role)
			if err != nil {
				return err
			}
			record.RoleCreation[role] = created.UTC()
		}
	}
	raw, err := convert.PropertyJSON.MarshalState(record)
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
