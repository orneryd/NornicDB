// Package auth: user impersonation privileges (Neo4j's GRANT | DENY |
// REVOKE [IMMUTABLE] IMPERSONATE [(user[, …] | *)] ON DBMS TO | FROM role).
//
// A privilege names a role, a user (or * for every user), whether it grants
// or denies, and whether it is immutable. A principal may impersonate a user
// when one of its roles is granted that user (or *), or it holds the admin
// entitlement (Neo4j's admin role has ALL DBMS PRIVILEGES, which include
// IMPERSONATE), and none of its roles is denied that user (or *): as in
// Neo4j, a DENY wins over any GRANT.
//
// Privileges are stored in the system database as _ImpersonatePrivilege
// nodes, the way ProcedurePrivilegeStore keeps EXECUTE grants: a node counts
// as a privilege only when its ID is the one this store gives it, the store
// keeps no copy and reads them from storage each time it is asked (only when
// a client asks to impersonate someone), and a read that fails denies.
package auth

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const impersonatePrivilegeLabel = "_ImpersonatePrivilege"

// ImpersonateTarget is whom an IMPERSONATE privilege names: one user, or
// every user (All, Neo4j's bare IMPERSONATE (*)). A user whose name is * is
// ImpersonateUser("*"), never every user.
type ImpersonateTarget struct {
	User string
	All  bool
}

// ImpersonateAllUsers is the target of IMPERSONATE (*).
var ImpersonateAllUsers = ImpersonateTarget{All: true}

// ImpersonateUser is the target naming the one user name.
func ImpersonateUser(name string) ImpersonateTarget {
	return ImpersonateTarget{User: name}
}

// ImpersonatePrivilege is one IMPERSONATE privilege: Role may (or, when
// Denied, may not) impersonate User, or every user when AllUsers (User is
// then empty).
type ImpersonatePrivilege struct {
	Role      string
	User      string
	AllUsers  bool
	Denied    bool
	Immutable bool
}

// Target is whom the privilege names.
func (p ImpersonatePrivilege) Target() ImpersonateTarget {
	return ImpersonateTarget{User: p.User, All: p.AllUsers}
}

// ImpersonatePrivilegeStore reads and writes impersonation privileges in the
// system database (or in an explicit transaction's view of it).
type ImpersonatePrivilegeStore struct {
	storage storage.Engine
}

// NewImpersonatePrivilegeStore creates a store over the system database.
func NewImpersonatePrivilegeStore(systemStorage storage.Engine) *ImpersonatePrivilegeStore {
	return &ImpersonatePrivilegeStore{storage: systemStorage}
}

// impersonatePrivilegeNodeID is the ID of the node holding privilege, which
// only this store chooses. The role's length keeps a role and a user that
// contain ':' apart; every user is "*" and a named user "u:" + name, so a
// user named * is never every user.
func impersonatePrivilegeNodeID(privilege ImpersonatePrivilege) storage.NodeID {
	kind := "granted"
	if privilege.Denied {
		kind = "denied"
	}
	target := "u:" + privilege.User
	if privilege.AllUsers {
		target = "*"
	}
	return storage.NodeID("imp_priv:" + kind + ":" + strconv.Itoa(len(privilege.Role)) + ":" + privilege.Role + ":" + target)
}

// impersonatePrivilegeNode is the privilege a node holds, if it is one: a
// node whose ID is the one Set gives its privilege.
func impersonatePrivilegeNode(n *storage.Node) (ImpersonatePrivilege, bool) {
	role, _ := n.Properties["role"].(string)
	user, _ := n.Properties["user"].(string)
	allUsers, _ := n.Properties["allUsers"].(bool)
	denied, _ := n.Properties["denied"].(bool)
	immutable, _ := n.Properties["immutable"].(bool)
	privilege := ImpersonatePrivilege{Role: role, User: user, AllUsers: allUsers, Denied: denied, Immutable: immutable}
	return privilege, role != "" && allUsers == (user == "") && n.ID == impersonatePrivilegeNodeID(privilege)
}

func impersonatePrivilegeKey(role string, target ImpersonateTarget, denied bool) ImpersonatePrivilege {
	privilege := ImpersonatePrivilege{Role: NormalizeRoleName(role), AllUsers: target.All, Denied: denied}
	if !target.All {
		privilege.User = strings.TrimSpace(target.User)
	}
	return privilege
}

// privileges reads every impersonation privilege from the system database.
func (p *ImpersonatePrivilegeStore) privileges() ([]ImpersonatePrivilege, error) {
	if p == nil || p.storage == nil {
		return nil, nil
	}
	nodes, err := p.storage.GetNodesByLabel(impersonatePrivilegeLabel)
	if err != nil {
		return nil, err
	}
	var privileges []ImpersonatePrivilege
	for _, n := range nodes {
		if privilege, ok := impersonatePrivilegeNode(n); ok {
			privileges = append(privileges, privilege)
		}
	}
	return privileges, nil
}

// Lookup returns role's GRANT (or DENY) of target, if it holds one. A read
// that fails is an error, never "no privilege".
func (p *ImpersonatePrivilegeStore) Lookup(ctx context.Context, role string, target ImpersonateTarget, denied bool) (ImpersonatePrivilege, bool, error) {
	node, err := p.storage.GetNode(impersonatePrivilegeNodeID(impersonatePrivilegeKey(role, target, denied)))
	if errors.Is(err, storage.ErrNotFound) {
		return ImpersonatePrivilege{}, false, nil
	}
	if err != nil {
		return ImpersonatePrivilege{}, false, err
	}
	privilege, ok := impersonatePrivilegeNode(node)
	return privilege, ok, nil
}

// Set records role's GRANT (or DENY) of target. As for procedure grants, a
// role holds one such privilege: setting it again changes nothing, except
// that an IMMUTABLE one makes a regular one immutable.
func (p *ImpersonatePrivilegeStore) Set(ctx context.Context, role string, target ImpersonateTarget, denied, immutable bool) error {
	if !target.All && strings.TrimSpace(target.User) == "" {
		return errors.New("auth: an impersonation privilege names a user or every user")
	}
	existing, found, err := p.Lookup(ctx, role, target, denied)
	if err != nil {
		return err
	}
	if found && (existing.Immutable || !immutable) {
		return nil
	}
	privilege := impersonatePrivilegeKey(role, target, denied)
	privilege.Immutable = immutable
	node := &storage.Node{
		ID:     impersonatePrivilegeNodeID(privilege),
		Labels: []string{impersonatePrivilegeLabel, "_System"},
		Properties: map[string]any{
			"role": privilege.Role, "user": privilege.User, "allUsers": privilege.AllUsers, "denied": privilege.Denied, "immutable": privilege.Immutable,
		},
	}
	if found {
		return p.storage.UpdateNode(node)
	}
	_, err = p.storage.CreateNode(node)
	return err
}

// Remove removes role's GRANT (or DENY) of target, and reports whether there
// was one. With immutableOnly (REVOKE IMMUTABLE) it removes only an
// immutable privilege, as Neo4j does; without, either.
func (p *ImpersonatePrivilegeStore) Remove(ctx context.Context, role string, target ImpersonateTarget, denied, immutableOnly bool) (bool, error) {
	privilege, found, err := p.Lookup(ctx, role, target, denied)
	if err != nil || !found || (immutableOnly && !privilege.Immutable) {
		return false, err
	}
	if err := p.storage.DeleteNode(impersonatePrivilegeNodeID(privilege)); err != nil {
		return false, err
	}
	return true, nil
}

// List returns every privilege, in role, user order (every user first), a
// DENY before the GRANT of the same user (as Neo4j lists them).
func (p *ImpersonatePrivilegeStore) List(ctx context.Context) ([]ImpersonatePrivilege, error) {
	privileges, err := p.privileges()
	if err != nil {
		return nil, err
	}
	sort.Slice(privileges, func(i, j int) bool {
		if privileges[i].Role != privileges[j].Role {
			return privileges[i].Role < privileges[j].Role
		}
		if privileges[i].AllUsers != privileges[j].AllUsers {
			return privileges[i].AllUsers
		}
		if privileges[i].User != privileges[j].User {
			return privileges[i].User < privileges[j].User
		}
		return privileges[i].Denied && !privileges[j].Denied
	})
	return privileges, nil
}

// AllowsImpersonation reports whether a principal with roles may impersonate
// user: no role is denied user or *, and a role is granted user or *, or the
// principal is an administrator (admin: its admin entitlement, Neo4j's ALL
// DBMS PRIVILEGES). When the privileges can't be read it denies, and
// returns the error.
func (p *ImpersonatePrivilegeStore) AllowsImpersonation(roles []string, admin bool, user string) (bool, error) {
	user = strings.TrimSpace(user)
	if user == "" {
		return false, nil
	}
	privileges, err := p.privileges()
	if err != nil {
		return false, err
	}
	held := make(map[string]bool, len(roles))
	for _, role := range roles {
		held[NormalizeRoleName(role)] = true
	}
	granted := admin
	for _, privilege := range privileges {
		if !held[privilege.Role] || (!privilege.AllUsers && privilege.User != user) {
			continue
		}
		if privilege.Denied {
			return false, nil
		}
		granted = true
	}
	return granted, nil
}
