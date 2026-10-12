// Package auth: procedure execution privileges (Neo4j's GRANT EXECUTE
// [BOOSTED] PROCEDURE … ON DBMS TO role).
//
// A grant names a role, a procedure name or glob (db.cdc.*), and whether it
// is boosted. A boosted grant lets the role run a matching procedure that
// needs administrator rights; a plain EXECUTE grant adds nothing to what the
// role may already run. Grants are stored in the system database as
// _ProcedurePrivilege nodes.
package auth

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const procedurePrivilegeLabel = "_ProcedurePrivilege"

// ProcedurePrivilege is one EXECUTE grant: Role may run the procedures
// Procedure (a name or glob) names; Boosted lifts their administrator
// requirement.
type ProcedurePrivilege struct {
	Role      string
	Procedure string
	Boosted   bool
}

// procedurePrivilegesVersion counts grant changes in this process, so a
// store that cached the grants reloads them after another store changed them.
var procedurePrivilegesVersion atomic.Uint64

// ProcedurePrivilegeStore reads and writes procedure grants in the system
// database.
type ProcedurePrivilegeStore struct {
	storage storage.Engine
	mu      sync.RWMutex
	grants  []ProcedurePrivilege
	version uint64
	loaded  bool
}

// NewProcedurePrivilegeStore creates a store over the system database.
func NewProcedurePrivilegeStore(systemStorage storage.Engine) *ProcedurePrivilegeStore {
	return &ProcedurePrivilegeStore{storage: systemStorage}
}

// normalizeGrantRole is a role's name as privileges store it: lower case,
// without a role_ prefix (PrivilegesStore.Resolve).
func normalizeGrantRole(role string) string {
	return strings.TrimPrefix(strings.ToLower(strings.TrimSpace(role)), "role_")
}

func procedurePrivilegeNodeID(grant ProcedurePrivilege) storage.NodeID {
	kind := "execute"
	if grant.Boosted {
		kind = "boosted"
	}
	return storage.NodeID("proc_priv:" + kind + ":" + grant.Role + ":" + grant.Procedure)
}

// Load reads every grant from the system database.
func (p *ProcedurePrivilegeStore) Load(ctx context.Context) error {
	if p == nil || p.storage == nil {
		return nil
	}
	version := procedurePrivilegesVersion.Load()
	var grants []ProcedurePrivilege
	err := storage.StreamNodesWithFallback(ctx, p.storage, 1000, func(n *storage.Node) error {
		for _, label := range n.Labels {
			if label != procedurePrivilegeLabel {
				continue
			}
			role, _ := n.Properties["role"].(string)
			procedure, _ := n.Properties["procedure"].(string)
			boosted, _ := n.Properties["boosted"].(bool)
			if role != "" && procedure != "" {
				grants = append(grants, ProcedurePrivilege{Role: role, Procedure: procedure, Boosted: boosted})
			}
			break
		}
		return nil
	})
	if err != nil {
		return err
	}
	sortProcedurePrivileges(grants)
	p.mu.Lock()
	p.grants, p.version, p.loaded = grants, version, true
	p.mu.Unlock()
	return nil
}

func sortProcedurePrivileges(grants []ProcedurePrivilege) {
	sort.Slice(grants, func(i, j int) bool {
		if grants[i].Role != grants[j].Role {
			return grants[i].Role < grants[j].Role
		}
		if grants[i].Procedure != grants[j].Procedure {
			return grants[i].Procedure < grants[j].Procedure
		}
		return !grants[i].Boosted && grants[j].Boosted
	})
}

// current returns the grants, reloading them when they changed since the
// store last read them.
func (p *ProcedurePrivilegeStore) current() []ProcedurePrivilege {
	p.mu.RLock()
	fresh := p.loaded && p.version == procedurePrivilegesVersion.Load()
	grants := p.grants
	p.mu.RUnlock()
	if fresh {
		return grants
	}
	if err := p.Load(context.Background()); err != nil {
		return grants
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.grants
}

// Grant records that role may execute the procedures procedure names
// (boosted or not). Granting what is already granted changes nothing.
func (p *ProcedurePrivilegeStore) Grant(ctx context.Context, role, procedure string, boosted bool) error {
	grant := ProcedurePrivilege{Role: normalizeGrantRole(role), Procedure: strings.TrimSpace(procedure), Boosted: boosted}
	node := &storage.Node{
		ID:         procedurePrivilegeNodeID(grant),
		Labels:     []string{procedurePrivilegeLabel, "_System"},
		Properties: map[string]any{"role": grant.Role, "procedure": grant.Procedure, "boosted": grant.Boosted},
	}
	if _, err := p.storage.GetNode(node.ID); err == nil {
		return nil
	}
	if _, err := p.storage.CreateNode(node); err != nil {
		return err
	}
	procedurePrivilegesVersion.Add(1)
	return p.Load(ctx)
}

// Revoke removes role's grant for procedure (boosted or not), and reports
// whether there was one.
func (p *ProcedurePrivilegeStore) Revoke(ctx context.Context, role, procedure string, boosted bool) (bool, error) {
	grant := ProcedurePrivilege{Role: normalizeGrantRole(role), Procedure: strings.TrimSpace(procedure), Boosted: boosted}
	id := procedurePrivilegeNodeID(grant)
	if _, err := p.storage.GetNode(id); err != nil {
		return false, nil
	}
	if err := p.storage.DeleteNode(id); err != nil {
		return false, err
	}
	procedurePrivilegesVersion.Add(1)
	return true, p.Load(ctx)
}

// List returns every grant, in role, procedure order.
func (p *ProcedurePrivilegeStore) List() []ProcedurePrivilege {
	if p == nil {
		return nil
	}
	return append([]ProcedurePrivilege(nil), p.current()...)
}

// AllowsBoosted reports whether any of roles holds a boosted grant whose
// name or glob matches procedure.
func (p *ProcedurePrivilegeStore) AllowsBoosted(roles []string, procedure string) bool {
	if p == nil || procedure == "" || len(roles) == 0 {
		return false
	}
	grants := p.current()
	for _, role := range roles {
		role = normalizeGrantRole(role)
		for _, grant := range grants {
			if grant.Boosted && grant.Role == role && ProcedureGlobMatches(grant.Procedure, procedure) {
				return true
			}
		}
	}
	return false
}

// ProcedureGlobMatches reports whether a procedure name matches a grant's
// name or glob, as Neo4j matches them: * stands for any characters, dots
// included, and ? for one; names compare case-insensitively.
func ProcedureGlobMatches(glob, name string) bool {
	glob, name = strings.ToLower(glob), strings.ToLower(name)
	// Iterative wildcard match with backtracking to the last *.
	g, n, star, mark := 0, 0, -1, 0
	for n < len(name) {
		switch {
		case g < len(glob) && (glob[g] == '?' || glob[g] == name[n]):
			g++
			n++
		case g < len(glob) && glob[g] == '*':
			star, mark = g, n
			g++
		case star >= 0:
			g = star + 1
			mark++
			n = mark
		default:
			return false
		}
	}
	for g < len(glob) && glob[g] == '*' {
		g++
	}
	return g == len(glob)
}
