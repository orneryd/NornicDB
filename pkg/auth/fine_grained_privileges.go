package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
)

// FineGrainedPrivilege is a Cypher-administered, role-owned security record.
// Entitlements are its evaluated constituents. The declarative fields retain
// enough information for SHOW PRIVILEGES and restore through Cypher commands.
type FineGrainedPrivilege struct {
	Key          string                   `json:"key"`
	Role         string                   `json:"role"`
	Name         string                   `json:"name"`
	Graph        string                   `json:"graph"`
	Segment      string                   `json:"segment"`
	Resource     string                   `json:"resource"`
	Clause       string                   `json:"clause"`
	Deny         bool                     `json:"deny,omitempty"`
	Immutable    bool                     `json:"immutable,omitempty"`
	Entitlements []FineGrainedEntitlement `json:"entitlements"`
}

// FineGrainedPrivilegeKey identifies a canonical privilege clause within its stable resource.
func FineGrainedPrivilegeKey(databaseID string, kind FineGrainedResourceKind, clause string) string {
	digest := sha256.Sum256([]byte(databaseID + "\x00" + string(kind) + "\x00" + clause))
	return hex.EncodeToString(digest[:])
}

// ApplyFineGrainedPrivileges performs the grant or revoke on an in-memory
// catalog. The runtime validates roles and writes this catalog transactionally.
func ApplyFineGrainedPrivileges(catalog *FineGrainedCatalog, privileges []FineGrainedPrivilege, verb string, allowImmutable bool) error {
	for _, candidate := range privileges {
		for _, existing := range catalog.Privileges {
			if matchingPrivilege(existing, candidate, verb) && existing.Immutable && !allowImmutable {
				if verb == "grant" || verb == "deny" {
					continue // A repeated mutable grant leaves the immutable grant intact.
				}
				return ErrFineGrainedForbidden
			}
		}
	}
	for _, candidate := range privileges {
		if verb == "grant" || verb == "deny" {
			if !slices.ContainsFunc(catalog.Privileges, func(existing FineGrainedPrivilege) bool {
				return matchingPrivilege(existing, candidate, verb)
			}) {
				catalog.Privileges = append(catalog.Privileges, candidate)
			}
			continue
		}
		catalog.Privileges = slices.DeleteFunc(catalog.Privileges, func(existing FineGrainedPrivilege) bool {
			return matchingPrivilege(existing, candidate, verb) && (!candidate.Immutable || existing.Immutable)
		})
	}
	return nil
}

func matchingPrivilege(existing, candidate FineGrainedPrivilege, verb string) bool {
	if existing.Key != candidate.Key || existing.Role != candidate.Role {
		return false
	}
	switch verb {
	case "grant", "deny":
		return candidate.Deny == existing.Deny
	case "revoke_grant":
		return !existing.Deny
	case "revoke_deny":
		return existing.Deny
	default:
		return true
	}
}
