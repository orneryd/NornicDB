package auth

import (
	"context"
	"sort"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RoleNotFoundError is ErrRoleNotFound for one role, by the name its
// caller gave.
type RoleNotFoundError struct{ Role string }

func (e *RoleNotFoundError) Error() string { return ErrRoleNotFound.Error() + ": " + e.Role }
func (e *RoleNotFoundError) Unwrap() error { return ErrRoleNotFound }

// WithRoleRecords is the one owner of writes to what a role owns besides
// its role node (procedure grants, entitlement overrides, database
// allowlists, database privileges) and the one check that the roles they
// name exist. It runs mutate in one system-store transaction
// (WithStoreTransaction) after taking each named role's lock
// (LockRoleForGrant), in one order for every caller, so the write and a
// concurrent delete or rename of the role can't both commit. A role that
// doesn't exist is refused with a RoleNotFoundError, and nothing is
// written. Callers publish their caches only after it returns nil.
func WithRoleRecords(ctx context.Context, system storage.Engine, roles []string, mutate func(view storage.Engine) error) error {
	names := make([]string, 0, len(roles))
	given := make(map[string]string, len(roles))
	for _, role := range roles {
		name := NormalizeRoleName(role)
		if name == "" {
			return ErrInvalidRoleName
		}
		if _, dup := given[name]; !dup {
			given[name] = role
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return WithStoreTransaction(ctx, system, func(view storage.Engine) error {
		locks := NewRoleStore(view)
		for _, name := range names {
			exists, err := locks.LockRoleForGrant(ctx, name)
			if err != nil {
				return err
			}
			if !exists {
				return &RoleNotFoundError{Role: given[name]}
			}
		}
		return mutate(view)
	})
}

// putRoleRecord stores node, creating it or replacing the stored one with
// its creation time kept; remove deletes the stored node instead (a missing
// one is no change). The stored node read is the immutable Before image the
// update reports, so it is retained unchanged and only node becomes After.
func putRoleRecord(view storage.Engine, node *storage.Node, remove bool) error {
	view = storage.SystemStoreEngine(view)
	existing, err := view.GetNode(node.ID)
	if err == storage.ErrNotFound {
		if remove {
			return nil
		}
		_, err = view.CreateNode(node)
		return err
	}
	if err != nil {
		return err
	}
	if remove {
		return view.DeleteNode(node.ID)
	}
	node.CreatedAt = existing.CreatedAt
	return view.UpdateNode(storage.NodeUpdateRequest{Before: existing, After: node})
}
