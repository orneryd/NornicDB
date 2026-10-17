// Package auth: graph read privileges (Neo4j's GRANT | DENY MATCH {*} ON
// GRAPH … [NODES … | FOR (n:L) WHERE …] TO role) and access scopes.
//
// A graph privilege names a role, a graph (a database name, or * for every
// graph), whether it grants or denies, and its segment: which nodes it
// matches, as the canonical text the cypher package parses and prints
// (NODES *, NODES A, B, FOR (n:L) WHERE n.p = v, or a collection rule
// FOR (n:L) WHERE (n)-[:R]->(:C {k: v})). Privileges are stored in the
// system database as _GraphPrivilege nodes, like EXECUTE grants: a node
// counts as a privilege only when its ID is the one this store gives it, the
// store keeps no copy and reads them each time it is asked, and a read that
// fails denies. Role delete and rename move them with the role
// (roleOwnedRecords).
//
// An access scope (CREATE ACCESS SCOPE) is a graph's declaration of how
// collection membership reaches nodes derived from a document; it is stored
// as an _AccessScope node holding its canonical definition.
package auth

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	graphPrivilegeLabel = "_GraphPrivilege"
	accessScopeLabel    = "_AccessScope"
)

// GraphPrivilege is one graph read privilege.
type GraphPrivilege struct {
	Role    string
	Graph   string
	Denied  bool
	Segment string
}

// GraphPrivilegeStore reads and writes graph privileges in the system
// database (or in a transaction's view of it).
type GraphPrivilegeStore struct {
	storage storage.Engine
}

// NewGraphPrivilegeStore creates a store over the system database.
func NewGraphPrivilegeStore(systemStorage storage.Engine) *GraphPrivilegeStore {
	return &GraphPrivilegeStore{storage: systemStorage}
}

// NormalizeGraphName is a privilege's graph as stored: * or a lower-case
// database name.
func NormalizeGraphName(graph string) string {
	return strings.ToLower(strings.TrimSpace(graph))
}

// graphPrivilegeNodeID is the ID of the node holding privilege, which only
// this store chooses; the role's and graph's lengths keep their boundaries
// apart.
func graphPrivilegeNodeID(privilege GraphPrivilege) storage.NodeID {
	kind := "granted"
	if privilege.Denied {
		kind = "denied"
	}
	return storage.NodeID("graph_priv:" + kind + ":" + strconv.Itoa(len(privilege.Role)) + ":" + privilege.Role + ":" +
		strconv.Itoa(len(privilege.Graph)) + ":" + privilege.Graph + ":" + privilege.Segment)
}

func normalizeGraphPrivilege(privilege GraphPrivilege) GraphPrivilege {
	privilege.Role = NormalizeRoleName(privilege.Role)
	privilege.Graph = NormalizeGraphName(privilege.Graph)
	privilege.Segment = strings.TrimSpace(privilege.Segment)
	return privilege
}

// graphPrivilegeNode is the privilege a node holds, if it is one.
func graphPrivilegeNode(n *storage.Node) (GraphPrivilege, bool) {
	role, _ := n.Properties["role"].(string)
	graph, _ := n.Properties["graph"].(string)
	denied, _ := n.Properties["denied"].(bool)
	segment, _ := n.Properties["segment"].(string)
	privilege := GraphPrivilege{Role: role, Graph: graph, Denied: denied, Segment: segment}
	return privilege, role != "" && graph != "" && segment != "" && n.ID == graphPrivilegeNodeID(privilege)
}

func graphPrivilegeStorageNode(privilege GraphPrivilege) *storage.Node {
	return &storage.Node{
		ID:     graphPrivilegeNodeID(privilege),
		Labels: []string{graphPrivilegeLabel, "_System"},
		Properties: map[string]any{
			"role": privilege.Role, "graph": privilege.Graph, "denied": privilege.Denied, "segment": privilege.Segment,
		},
	}
}

// Put records privilege; recording it again changes nothing.
func (p *GraphPrivilegeStore) Put(ctx context.Context, privilege GraphPrivilege) error {
	privilege = normalizeGraphPrivilege(privilege)
	if privilege.Role == "" || privilege.Graph == "" || privilege.Segment == "" {
		return errors.New("graph privilege needs a role, a graph and a segment")
	}
	if _, err := p.storage.GetNode(graphPrivilegeNodeID(privilege)); err == nil {
		return nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	_, err := p.storage.CreateNode(graphPrivilegeStorageNode(privilege))
	return err
}

// Remove deletes privilege and reports whether it was recorded.
func (p *GraphPrivilegeStore) Remove(ctx context.Context, privilege GraphPrivilege) (bool, error) {
	privilege = normalizeGraphPrivilege(privilege)
	if _, err := p.storage.GetNode(graphPrivilegeNodeID(privilege)); errors.Is(err, storage.ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, p.storage.DeleteNode(graphPrivilegeNodeID(privilege))
}

// List returns every graph privilege, in role, graph, grant-before-deny,
// segment order.
func (p *GraphPrivilegeStore) List(ctx context.Context) ([]GraphPrivilege, error) {
	if p == nil || p.storage == nil {
		return nil, nil
	}
	nodes, err := p.storage.GetNodesByLabel(graphPrivilegeLabel)
	if err != nil {
		return nil, err
	}
	var privileges []GraphPrivilege
	for _, n := range nodes {
		if privilege, ok := graphPrivilegeNode(n); ok {
			privileges = append(privileges, privilege)
		}
	}
	sort.Slice(privileges, func(i, j int) bool {
		a, b := privileges[i], privileges[j]
		switch {
		case a.Role != b.Role:
			return a.Role < b.Role
		case a.Graph != b.Graph:
			return a.Graph < b.Graph
		case a.Denied != b.Denied:
			return !a.Denied
		}
		return a.Segment < b.Segment
	})
	return privileges, nil
}

// ForRoles returns the privileges any of roles holds on database (named, or
// through *). A read that fails returns its error, never "no privileges".
func (p *GraphPrivilegeStore) ForRoles(ctx context.Context, roles []string, database string) ([]GraphPrivilege, error) {
	all, err := p.List(ctx)
	if err != nil {
		return nil, err
	}
	held := make(map[string]bool, len(roles))
	for _, role := range roles {
		held[NormalizeRoleName(role)] = true
	}
	database = NormalizeGraphName(database)
	var out []GraphPrivilege
	for _, privilege := range all {
		if held[privilege.Role] && (privilege.Graph == "*" || privilege.Graph == database) {
			out = append(out, privilege)
		}
	}
	return out, nil
}

// AccessScope is one graph's access scope: its name and canonical definition.
type AccessScope struct {
	Graph      string
	Name       string
	Definition string
}

// AccessScopeStore reads and writes access scopes in the system database.
type AccessScopeStore struct {
	storage storage.Engine
}

// NewAccessScopeStore creates a store over the system database.
func NewAccessScopeStore(systemStorage storage.Engine) *AccessScopeStore {
	return &AccessScopeStore{storage: systemStorage}
}

func accessScopeNodeID(graph string) storage.NodeID {
	return storage.NodeID("access_scope:" + NormalizeGraphName(graph))
}

// Get returns graph's scope; found is false when it has none. A graph has at
// most one scope.
func (s *AccessScopeStore) Get(ctx context.Context, graph string) (AccessScope, bool, error) {
	if s == nil || s.storage == nil {
		return AccessScope{}, false, nil
	}
	node, err := s.storage.GetNode(accessScopeNodeID(graph))
	if errors.Is(err, storage.ErrNotFound) {
		return AccessScope{}, false, nil
	}
	if err != nil {
		return AccessScope{}, false, err
	}
	scope := AccessScope{Graph: NormalizeGraphName(graph)}
	scope.Name, _ = node.Properties["name"].(string)
	scope.Definition, _ = node.Properties["definition"].(string)
	return scope, scope.Name != "" && scope.Definition != "", nil
}

// Put records scope, replacing graph's existing one.
func (s *AccessScopeStore) Put(ctx context.Context, scope AccessScope) error {
	node := &storage.Node{
		ID:     accessScopeNodeID(scope.Graph),
		Labels: []string{accessScopeLabel, "_System"},
		Properties: map[string]any{
			"graph": NormalizeGraphName(scope.Graph), "name": scope.Name, "definition": scope.Definition,
		},
	}
	if _, err := s.storage.GetNode(node.ID); err == nil {
		return s.storage.UpdateNode(node)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	_, err := s.storage.CreateNode(node)
	return err
}

// Delete removes graph's scope and reports whether it had one.
func (s *AccessScopeStore) Delete(ctx context.Context, graph string) (bool, error) {
	if _, err := s.storage.GetNode(accessScopeNodeID(graph)); errors.Is(err, storage.ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, s.storage.DeleteNode(accessScopeNodeID(graph))
}

// List returns every scope, by graph.
func (s *AccessScopeStore) List(ctx context.Context) ([]AccessScope, error) {
	nodes, err := s.storage.GetNodesByLabel(accessScopeLabel)
	if err != nil {
		return nil, err
	}
	var scopes []AccessScope
	for _, node := range nodes {
		graph, _ := node.Properties["graph"].(string)
		if node.ID != accessScopeNodeID(graph) {
			continue
		}
		scope := AccessScope{Graph: graph}
		scope.Name, _ = node.Properties["name"].(string)
		scope.Definition, _ = node.Properties["definition"].(string)
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(i, j int) bool { return scopes[i].Graph < scopes[j].Graph })
	return scopes, nil
}
