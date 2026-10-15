package auth

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// withRoleRecords is the one owner of writes to the records a role owns
// besides its role node: entitlement overrides, database allowlists and
// database privileges. It runs mutate in one system-store transaction
// (WithStoreTransaction) after taking each named role's lock
// (LockRoleForGrant), the same lock GRANT takes, so the write and a
// concurrent delete or rename of the role can't both commit. A role that
// doesn't exist is refused with ErrRoleNotFound naming it, and nothing is
// written. Callers publish their caches only after it returns nil.
func withRoleRecords(ctx context.Context, system storage.Engine, roles []string, mutate func(view storage.Engine) error) error {
	names := make([]string, 0, len(roles))
	seen := make(map[string]struct{}, len(roles))
	for _, role := range roles {
		role = strings.ToLower(strings.TrimSpace(role))
		if role == "" {
			return ErrInvalidRoleName
		}
		if _, dup := seen[role]; !dup {
			seen[role] = struct{}{}
			names = append(names, role)
		}
	}
	// One lock order for every caller.
	sort.Strings(names)
	return WithStoreTransaction(ctx, system, func(view storage.Engine) error {
		locks := NewRoleStore(view)
		for _, role := range names {
			exists, err := locks.LockRoleForGrant(ctx, role)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("%w: %s", ErrRoleNotFound, role)
			}
		}
		return mutate(view)
	})
}

// putRoleRecord stores node, creating it or replacing the stored one with
// its creation time kept; remove deletes the stored node instead (a missing
// one is no change).
func putRoleRecord(view storage.Engine, node *storage.Node, remove bool) error {
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
	return view.UpdateNode(node)
}
