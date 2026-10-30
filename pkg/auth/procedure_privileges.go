// Package auth: procedure execution privileges (Neo4j's GRANT [IMMUTABLE]
// EXECUTE [BOOSTED] PROCEDURE … ON DBMS TO role).
//
// A grant names a role, a procedure name or glob (db.cdc.*) and whether it
// is boosted; a role holds at most one such grant, which is immutable or
// not. A boosted grant lets the role run a matching procedure that needs
// administrator rights; a plain EXECUTE grant adds nothing to what the role
// may already run.
//
// Grants are stored in the system database as _ProcedurePrivilege nodes,
// kept apart from graph data the way the other system stores keep theirs:
//   - the system database runs no Cypher graph writes (the cypher package
//     refuses CREATE, MERGE, SET, DELETE, REMOVE and write procedures there,
//     on every route), so grants originate in GRANT and REVOKE through this
//     store; role changes move or remove them through the shared transaction owner;
//   - as RoleStore and PrivilegesStore key their nodes by an ID only the
//     store chooses (role:…, db_priv:…), a node counts as a grant only when
//     its ID is the one Grant gives that grant, so a node made any other way
//     (a Cypher CREATE gets a generated ID) is never read as a grant.
//
// The store keeps no copy of the grants: it reads them from storage each
// time it is asked (a label lookup over the few grant nodes, done only when
// a caller without administrator rights calls a procedure that needs them).
// So a change is seen as soon as it is committed, by this process and by
// any other process or server instance over the same storage (a GRANT in
// an explicit transaction when that transaction commits), and a read that
// fails denies and returns its error. A version marker that every check
// compares would also cost a storage read per check, and would add a write
// to every change that must stay in step with it.
package auth

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const procedurePrivilegeLabel = "_ProcedurePrivilege"

// ProcedurePrivilege is one EXECUTE grant: Role may run the procedures
// Procedure (a name or glob) names; Boosted lifts their administrator
// requirement; Immutable marks a grant made with GRANT IMMUTABLE.
type ProcedurePrivilege struct {
	Role      string
	Procedure string
	Boosted   bool
	Immutable bool
}

// ProcedurePrivilegeStore reads and writes procedure grants in the system
// database (or in an explicit transaction's view of it).
type ProcedurePrivilegeStore struct {
	storage storage.Engine
}

// NewProcedurePrivilegeStore creates a store over the system database.
func NewProcedurePrivilegeStore(systemStorage storage.Engine) *ProcedurePrivilegeStore {
	return &ProcedurePrivilegeStore{storage: systemStorage}
}

// NormalizeRoleName is the one form of a role's name, for every role store,
// record and lookup: trimmed, lower case, without the role_ prefix identity
// providers add (ROLE_ops is the role ops).
func NormalizeRoleName(role string) string {
	return strings.TrimPrefix(cyphertext.NameLower(strings.TrimSpace(role)), "role_")
}

// procedurePrivilegeNodeID is the ID of the node holding role's grant for
// procedure (boosted or not), which only this store chooses.
func procedurePrivilegeNodeID(grant ProcedurePrivilege) storage.NodeID {
	kind := "execute"
	if grant.Boosted {
		kind = "boosted"
	}
	return storage.NodeID("proc_priv:" + kind + ":" + grant.Role + ":" + grant.Procedure)
}

// procedurePrivilegeNode is the grant a node holds, if it is one: a node
// whose ID is the one Grant gives its grant.
func procedurePrivilegeNode(n *storage.Node) (ProcedurePrivilege, bool) {
	role, _ := n.Properties["role"].(string)
	procedure, _ := n.Properties["procedure"].(string)
	boosted, _ := n.Properties["boosted"].(bool)
	immutable, _ := n.Properties["immutable"].(bool)
	grant := ProcedurePrivilege{Role: role, Procedure: procedure, Boosted: boosted, Immutable: immutable}
	return grant, role != "" && procedure != "" && n.ID == procedurePrivilegeNodeID(grant)
}

// grants reads every grant from the system database.
func (p *ProcedurePrivilegeStore) grants() ([]ProcedurePrivilege, error) {
	if p == nil || p.storage == nil {
		return nil, nil
	}
	nodes, err := p.storage.GetNodesByLabel(procedurePrivilegeLabel)
	if err != nil {
		return nil, err
	}
	var grants []ProcedurePrivilege
	for _, n := range nodes {
		if grant, ok := procedurePrivilegeNode(n); ok {
			grants = append(grants, grant)
		}
	}
	return grants, nil
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

// Lookup returns role's grant for procedure (boosted or not), if it holds
// one. A read that fails is an error, never "no grant".
func (p *ProcedurePrivilegeStore) Lookup(ctx context.Context, role, procedure string, boosted bool) (ProcedurePrivilege, bool, error) {
	key := ProcedurePrivilege{Role: NormalizeRoleName(role), Procedure: strings.TrimSpace(procedure), Boosted: boosted}
	node, err := p.storage.GetNode(procedurePrivilegeNodeID(key))
	if errors.Is(err, storage.ErrNotFound) {
		return ProcedurePrivilege{}, false, nil
	}
	if err != nil {
		return ProcedurePrivilege{}, false, err
	}
	grant, ok := procedurePrivilegeNode(node)
	return grant, ok, nil
}

// Grant records that role may execute the procedures procedure names
// (boosted or not). As in Neo4j, a role holds one such grant: granting it
// again changes nothing, except that GRANT IMMUTABLE makes a regular grant
// immutable.
func (p *ProcedurePrivilegeStore) Grant(ctx context.Context, role, procedure string, boosted, immutable bool) error {
	existing, found, err := p.Lookup(ctx, role, procedure, boosted)
	if err != nil {
		return err
	}
	if found && (existing.Immutable || !immutable) {
		return nil
	}
	grant := ProcedurePrivilege{Role: NormalizeRoleName(role), Procedure: strings.TrimSpace(procedure), Boosted: boosted, Immutable: immutable}
	node := &storage.Node{
		ID:     procedurePrivilegeNodeID(grant),
		Labels: []string{procedurePrivilegeLabel, "_System"},
		Properties: map[string]any{
			"role": grant.Role, "procedure": grant.Procedure, "boosted": grant.Boosted, "immutable": grant.Immutable,
		},
	}
	if found {
		return p.storage.UpdateNode(node)
	}
	_, err = p.storage.CreateNode(node)
	return err
}

// Revoke removes role's grant for procedure (boosted or not), and reports
// whether there was one. With immutableOnly (REVOKE IMMUTABLE) it removes
// only an immutable grant, as Neo4j does; without, either.
func (p *ProcedurePrivilegeStore) Revoke(ctx context.Context, role, procedure string, boosted, immutableOnly bool) (bool, error) {
	grant, found, err := p.Lookup(ctx, role, procedure, boosted)
	if err != nil || !found || (immutableOnly && !grant.Immutable) {
		return false, err
	}
	if err := p.storage.DeleteNode(procedurePrivilegeNodeID(grant)); err != nil {
		return false, err
	}
	return true, nil
}

// List returns every grant, in role, procedure order.
func (p *ProcedurePrivilegeStore) List(ctx context.Context) ([]ProcedurePrivilege, error) {
	grants, err := p.grants()
	if err != nil {
		return nil, err
	}
	sortProcedurePrivileges(grants)
	return grants, nil
}

// AllowsBoosted reports whether any of roles holds a boosted grant whose
// name or glob matches procedure: a principal's privileges are the union of
// its roles'. When the grants can't be read it denies, and returns the
// error.
func (p *ProcedurePrivilegeStore) AllowsBoosted(roles []string, procedure string) (bool, error) {
	if p == nil || procedure == "" || len(roles) == 0 {
		return false, nil
	}
	grants, err := p.grants()
	if err != nil {
		return false, err
	}
	for _, role := range roles {
		role = NormalizeRoleName(role)
		for _, grant := range grants {
			if grant.Boosted && grant.Role == role && ProcedureGlobMatches(grant.Procedure, procedure) {
				return true, nil
			}
		}
	}
	return false, nil
}

// ProcedureGlobMatches reports whether a procedure name matches a grant's
// name or glob, as Neo4j matches them: * stands for any characters, dots
// included, and ? for one character; names compare case-insensitively.
func ProcedureGlobMatches(glob, name string) bool {
	// The named pattern protocol counts canonical virtual codepoints. Ordinary
	// runs keep the existing lowercase policy; admitted lone UTF16 units remain
	// one distinct name character rather than becoming replacement runes.
	g, n := cyphertext.NameLower(glob), cyphertext.NameLower(name)
	gi, ni, star, mark := 0, 0, -1, 0
	for ni < len(n) {
		var pattern rune
		patternWidth := 0
		if gi < len(g) {
			pattern, patternWidth = cyphertext.ReadCodepoint(g, gi)
		}
		value, valueWidth := cyphertext.ReadCodepoint(n, ni)
		switch {
		case gi < len(g) && (pattern == '?' || pattern == value):
			gi += patternWidth
			ni += valueWidth
		case gi < len(g) && pattern == '*':
			gi += patternWidth
			star, mark = gi, ni
		case star >= 0:
			gi = star
			_, width := cyphertext.ReadCodepoint(n, mark)
			mark += width
			ni = mark
		default:
			return false
		}
	}
	for gi < len(g) && g[gi] == '*' {
		gi++
	}
	return gi == len(g)
}
