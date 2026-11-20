package auth

import (
	"cmp"
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
)

// FineGrainedResourceKind identifies the namespace of an authorized target.
type FineGrainedResourceKind string

const (
	FineGrainedDBMS         FineGrainedResourceKind = "dbms"
	FineGrainedDatabase     FineGrainedResourceKind = "database"
	FineGrainedNode         FineGrainedResourceKind = "node"
	FineGrainedRelationship FineGrainedResourceKind = "relationship"
)

// FineGrainedAction is the scope contract registered by an operation owner.
// Property actions must receive one concrete property key per decision.
type FineGrainedAction struct {
	ID           string                  `json:"id"`
	ResourceKind FineGrainedResourceKind `json:"resourceKind"`
	Property     bool                    `json:"property,omitempty"`
	Creates      bool                    `json:"creates,omitempty"`
}

// FineGrainedScope selects resources of one kind. An empty database ID
// selects all databases and is inherent for DBMS actions. Empty selector
// lists select all values in that dimension. Populated dimensions intersect.
// Types are node labels or relationship types, matched exactly.
type FineGrainedScope struct {
	Kind             FineGrainedResourceKind `json:"kind"`
	DatabaseID       string                  `json:"databaseId,omitempty"`
	HomeDatabase     bool                    `json:"homeDatabase,omitempty"`
	Types            []string                `json:"types,omitempty"`
	EntityIDs        []string                `json:"entityIds,omitempty"`
	Properties       []string                `json:"properties,omitempty"`
	Predicate        *FineGrainedPredicate   `json:"predicate,omitempty"`
	PropertyPatterns []string                `json:"propertyPatterns,omitempty"`
}

// FineGrainedResource is the resolved target of one operation. The operation
// owner supplies stable IDs and actual labels/types, not caller assertions.
type FineGrainedResource struct {
	Kind           FineGrainedResourceKind
	DatabaseID     string
	HomeDatabaseID string
	Types          []string
	EntityID       string
	CreatingEntity bool
	Property       string
	// EntityProperties is the authoritative image used by property predicates.
	EntityProperties map[string]any
}

// FineGrainedEntitlement is a named, reusable binding of an action and scope.
// Separate definitions let a role grant traversal and only selected properties.
type FineGrainedEntitlement struct {
	ID     string           `json:"id"`
	Action string           `json:"action"`
	Scope  FineGrainedScope `json:"scope"`
	Deny   bool             `json:"deny,omitempty"`
}

// FineGrainedCatalog is persisted independently of legacy coarse grants.
// Role names refer to the existing RBAC role records.
type FineGrainedCatalog struct {
	Definitions []FineGrainedEntitlement `json:"definitions"`
	Assignments map[string][]string      `json:"assignments"`
	Privileges  []FineGrainedPrivilege   `json:"privileges,omitempty"`
}

// ErrInvalidFineGrainedPolicy is returned for invalid or unresolved policy.
var ErrInvalidFineGrainedPolicy = errors.New("invalid fine-grained policy")

var (
	entitlementIdentifier = regexp.MustCompile(`^[a-z][a-z0-9_.-]*$`)
	operationIdentifier   = regexp.MustCompile(`^[a-z][A-Za-z0-9_.-]*$`)
)

// FineGrainedPolicy is an immutable, compiled catalog. Bind it to the
// previously resolved roles of the executing principal before evaluation.
type FineGrainedPolicy struct {
	actions map[string]FineGrainedAction
	roles   map[string]map[string][]FineGrainedEntitlement
}

// CompileFineGrainedPolicy validates the full catalog against the registered
// operation contracts. A role referring to an unknown definition is invalid.
//
// For example, register graph.node.property.read as a node property action
// and give the reviewers role a definition scoped to the title property.
func CompileFineGrainedPolicy(actions []FineGrainedAction, catalog FineGrainedCatalog) (*FineGrainedPolicy, error) {
	policy := &FineGrainedPolicy{
		actions: make(map[string]FineGrainedAction, len(actions)),
		roles:   make(map[string]map[string][]FineGrainedEntitlement, len(catalog.Assignments)),
	}
	for _, action := range actions {
		_, duplicate := policy.actions[action.ID]
		if duplicate || !operationIdentifier.MatchString(action.ID) || !action.valid() {
			return nil, fmt.Errorf("%w: invalid or duplicate action %q", ErrInvalidFineGrainedPolicy, action.ID)
		}
		policy.actions[action.ID] = action
	}
	definitions := make(map[string]FineGrainedEntitlement, len(catalog.Definitions))
	for _, definition := range catalog.Definitions {
		action, known := policy.actions[definition.Action]
		_, duplicate := definitions[definition.ID]
		if !known || duplicate || !entitlementIdentifier.MatchString(definition.ID) || (!definition.Scope.validFor(action) || definition.Deny && definition.Scope.Predicate != nil && definition.Scope.Predicate.Operator == "NATIVE TAG") {
			return nil, fmt.Errorf("%w: invalid or duplicate entitlement %q for action %q", ErrInvalidFineGrainedPolicy, definition.ID, definition.Action)
		}
		definition.Scope = definition.Scope.clone()
		definitions[definition.ID] = definition
	}
	for name, ids := range catalog.Assignments {
		role := NormalizeRoleName(name)
		if _, duplicate := policy.roles[role]; duplicate || role == "" {
			return nil, fmt.Errorf("%w: invalid or duplicate role %q", ErrInvalidFineGrainedPolicy, name)
		}
		grants := make(map[string][]FineGrainedEntitlement)
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			definition, ok := definitions[id]
			if !ok || seen[id] {
				return nil, fmt.Errorf("%w: role %q references unknown or duplicate entitlement %q", ErrInvalidFineGrainedPolicy, name, id)
			}
			seen[id] = true
			grants[definition.Action] = append(grants[definition.Action], definition)
		}
		policy.roles[role] = grants
	}
	for _, privilege := range catalog.Privileges {
		role := NormalizeRoleName(privilege.Role)
		if role == "" || privilege.Key == "" || len(privilege.Entitlements) == 0 {
			return nil, ErrInvalidFineGrainedPolicy
		}
		grants := policy.roles[role]
		if grants == nil {
			grants = make(map[string][]FineGrainedEntitlement)
			policy.roles[role] = grants
		}
		for _, entitlement := range privilege.Entitlements {
			action, known := policy.actions[entitlement.Action]
			if !known || !entitlement.Scope.validFor(action) {
				return nil, fmt.Errorf("%w: invalid privilege %q", ErrInvalidFineGrainedPolicy, privilege.Name)
			}
			entitlement.Scope = entitlement.Scope.clone()
			grants[entitlement.Action] = append(grants[entitlement.Action], entitlement)
		}
	}

	return policy, nil
}

// FineGrainedAccess holds the compiled policy for one executing principal.
type FineGrainedAccess struct {
	boostedDatabase string
	nativeABAC      bool
	nativeTags      []string
	actions         map[string]FineGrainedAction
	grants          map[string][]FineGrainedEntitlement
}

// ForRoles resolves the union of the given roles once for a request or other
// admitted work. Built-in names confer only their explicit assignments.
func (p *FineGrainedPolicy) ForRoles(roles []string) FineGrainedAccess {
	grants := make(map[string][]FineGrainedEntitlement)
	for _, role := range roles {
		for action, definitions := range p.roles[NormalizeRoleName(role)] {
			grants[action] = append(grants[action], definitions...)
		}
	}
	return FineGrainedAccess{actions: p.actions, grants: grants}
}

// Allows checks one action on a fully resolved resource. An applicable deny
// overrides a grant. Unknown actions and incomplete targets are denied.
func (a FineGrainedAccess) Allows(actionID string, resource FineGrainedResource) bool {
	granted, _ := a.Decision(actionID, resource)
	return granted
}

// Decision distinguishes an explicit deny from the absence of a grant.
// A compound operation may combine grants but must apply explicit denials.
func (a FineGrainedAccess) Decision(actionID string, resource FineGrainedResource) (allowed, denied bool) {
	action, known := a.actions[actionID]
	if !known || !resource.validFor(action) {
		return false, false
	}

	if a.boostedDatabase != "" && a.boostedDatabase == resource.DatabaseID && boostedGraphAction(actionID, action) {
		return true, false
	}
	for _, definition := range a.grants[actionID] {
		if definition.Scope.matches(resource, a.nativeABAC, a.nativeTags) {
			if definition.Deny {
				return false, true
			}
			allowed = true
		}
	}
	return allowed, false
}

func boostedGraphAction(id string, action FineGrainedAction) bool {
	if action.ResourceKind.entity() {
		return !strings.Contains(id, ".embedding.")
	}
	switch id {
	case "database.token.label.create", "database.token.relationship_type.create", "database.token.property.create":
		return true
	}
	return false
}

// AllowsAllDatabases requires a wildcard database grant and rejects every
// applicable denial. It is used for operations whose target is the full
// database population, including databases added while background work runs.
func (a FineGrainedAccess) AllowsAllDatabases(actionID string) bool {
	action, known := a.actions[actionID]
	if !known || action.ResourceKind != FineGrainedDatabase || action.Property {
		return false
	}
	allowed := false
	for _, definition := range a.grants[actionID] {
		if definition.Deny {
			return false
		}
		allowed = allowed || (definition.Scope.DatabaseID == "" && !definition.Scope.HomeDatabase)
	}
	return allowed
}

func (k FineGrainedResourceKind) entity() bool {
	return k == FineGrainedNode || k == FineGrainedRelationship
}

func (a FineGrainedAction) valid() bool {
	if a.ResourceKind.entity() {
		return !a.Creates || !a.Property
	}
	return !a.Creates && (a.ResourceKind == FineGrainedDatabase || a.ResourceKind == FineGrainedDBMS)
}

func validSelectors(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		_, duplicate := seen[value]
		if value == "" || duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func (s FineGrainedScope) validFor(action FineGrainedAction) bool {
	if s.Kind != action.ResourceKind || (s.Kind == FineGrainedDBMS && (s.DatabaseID != "" || s.HomeDatabase)) ||
		(s.HomeDatabase && s.DatabaseID != "") {
		return false
	}
	if !s.Kind.entity() && (len(s.Types) > 0 || len(s.EntityIDs) > 0) {
		return false
	}
	if len(s.EntityIDs) > 0 && (s.DatabaseID == "" || action.Creates) {
		return false
	}
	if len(s.PropertyPatterns) != 0 {
		if !action.Property || action.ResourceKind != FineGrainedDBMS || len(s.Properties) != 0 {
			return false
		}
		for _, pattern := range s.PropertyPatterns {
			if pattern == "" {
				return false
			}
			if _, err := path.Match(pattern, ""); err != nil {
				return false
			}
		}
	}
	return (action.Property || len(s.Properties) == 0) && s.Predicate.validFor(action) &&
		validSelectors(s.Types) && validSelectors(s.EntityIDs) && validSelectors(s.Properties)
}

func (r FineGrainedResource) validFor(action FineGrainedAction) bool {
	if r.Kind != action.ResourceKind || (r.Kind == FineGrainedDBMS) != (r.DatabaseID == "") ||
		action.Property != (r.Property != "") {
		return false
	}
	if r.Kind == FineGrainedRelationship && len(r.Types) != 1 {
		return false
	}
	if !r.Kind.entity() {
		return r.EntityID == "" && len(r.Types) == 0
	}
	if r.CreatingEntity {
		return r.EntityID == "" && (action.Creates || action.Property)
	}
	return action.Creates == (r.EntityID == "")
}

func (s FineGrainedScope) matches(r FineGrainedResource, nativeABAC bool, nativeTags []string) bool {
	if (s.DatabaseID != "" && s.DatabaseID != r.DatabaseID) ||
		(s.HomeDatabase && r.HomeDatabaseID != r.DatabaseID) ||
		(len(s.EntityIDs) != 0 && !slices.Contains(s.EntityIDs, r.EntityID)) ||
		(len(s.Properties) != 0 && !slices.Contains(s.Properties, r.Property)) ||
		!s.Predicate.matches(r.EntityProperties, nativeABAC, nativeTags) {
		return false
	}
	if len(s.PropertyPatterns) != 0 && !slices.ContainsFunc(s.PropertyPatterns, func(pattern string) bool {
		matched, _ := path.Match(pattern, r.Property)
		return matched
	}) {
		return false
	}
	for _, name := range r.Types {
		if slices.Contains(s.Types, name) {
			return true
		}
	}
	return len(s.Types) == 0
}

// Rules returns a detached list of the effective principal's assigned rules,
// including applicable denials. A decision still requires a concrete target.
func (a FineGrainedAccess) Rules() []FineGrainedEntitlement {
	var rules []FineGrainedEntitlement
	for _, entries := range a.grants {
		for _, rule := range entries {
			rule.Scope = rule.Scope.clone()
			rules = append(rules, rule)
		}
	}
	slices.SortStableFunc(rules, func(first, second FineGrainedEntitlement) int {
		return cmp.Or(cmp.Compare(first.Action, second.Action), cmp.Compare(first.ID, second.ID), cmp.Compare(first.Scope.DatabaseID, second.Scope.DatabaseID))
	})
	return rules
}

func (s FineGrainedScope) clone() FineGrainedScope {
	s.Types = slices.Clone(s.Types)
	s.EntityIDs = slices.Clone(s.EntityIDs)
	s.Properties = slices.Clone(s.Properties)
	s.PropertyPatterns = slices.Clone(s.PropertyPatterns)
	s.Predicate = s.Predicate.clone()
	return s
}
