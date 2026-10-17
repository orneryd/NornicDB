package cypher

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// GraphVisibility is a restricted principal's read view of one database:
// which nodes its graph privileges let it see. A principal is restricted on
// a database when one of its roles holds a collection rule there; an
// unrestricted principal has no GraphVisibility and sees the whole graph,
// as before graph privileges existed.
//
// A node is visible when no DENY matches it and a GRANT does (Neo4j's
// combination of a principal's privileges over all its roles, PUBLIC
// included):
//   - a NODES or property-rule privilege matches a node by its labels and
//     properties;
//   - a collection rule matches a document of the scope's MEMBERSHIP label
//     in that collection; a document in several collections is granted by
//     any granted one and denied by any denied one; a document in none is
//     matched by no collection rule;
//   - a node the graph's access scope derives from documents or collections
//     (DERIVE) is visible when any of its sources is (sources combine by
//     ANY), and no DENY matches the node itself.
//
// A relationship is visible when both its ends are. Everything is decided
// from current data, through the storage the statement reads.
type GraphVisibility struct {
	database string
	scope    *accessScope
	grants   []graphSegment // NODES and property rules
	denies   []graphSegment
	// collection rules: those matching the scope's membership pattern
	// reach derived nodes; others match their own label only.
	grantRules []graphSegment
	denyRules  []graphSegment
}

// Restricted reports whether v restricts what its principal sees.
func (v *GraphVisibility) Restricted() bool { return v != nil }

// ResolveGraphVisibility reads roles' graph privileges on database (PUBLIC's
// included) and the database's access scope from the system database: nil
// when no role holds a collection rule there. A read that fails is an error;
// callers refuse the statement then.
func ResolveGraphVisibility(ctx context.Context, system storage.Engine, roles []string, database string) (*GraphVisibility, error) {
	if system == nil {
		return nil, nil
	}
	held := append(append(make([]string, 0, len(roles)+1), roles...), publicRole)
	privileges, err := auth.NewGraphPrivilegeStore(system).ForRoles(ctx, held, database)
	if err != nil {
		return nil, err
	}
	v := &GraphVisibility{database: database}
	restricted := false
	for _, privilege := range privileges {
		segment, err := parseGraphSegment(privilege.Segment)
		if err != nil {
			return nil, err
		}
		if segment.kind == segmentCollection {
			// PUBLIC's rules restrict nobody on their own: a login is
			// restricted only by a rule one of its own roles holds.
			if auth.NormalizeRoleName(privilege.Role) != publicRole {
				restricted = true
			}
			if privilege.Denied {
				v.denyRules = append(v.denyRules, segment)
			} else {
				v.grantRules = append(v.grantRules, segment)
			}
			continue
		}
		if privilege.Denied {
			v.denies = append(v.denies, segment)
		} else {
			v.grants = append(v.grants, segment)
		}
	}
	if !restricted {
		return nil, nil
	}
	if v.scope, err = accessScopeFor(ctx, auth.NewAccessScopeStore(system), database); err != nil {
		return nil, err
	}
	return v, nil
}

// inScope reports whether a collection rule matches the scope's membership
// pattern, so its collections reach derived nodes.
func (v *GraphVisibility) inScope(rule graphSegment) bool {
	s := v.scope
	return s != nil && rule.label == s.documentLabel && rule.relType == s.membershipRel &&
		rule.outgoing == s.membershipOut && rule.target == s.collectionLbl && rule.targetKey == s.collectionKey
}

// createdNodes are the nodes a restricted principal's statement created, or
// its explicit transaction's earlier statements did. They stay visible to it
// for the rest of the statement or transaction, whatever the scope decides:
// a statement always addresses what it created (MERGE … SET n += $map,
// before the map sets the properties a DERIVE entry matches), and so the
// relationships it creates, whose ends it can address, are visible too.
// Writes stay W1's: the per-database write switch governs them and nothing
// checks where a created node lands, so once the transaction ends one
// outside the principal's collections is hidden from it.
type createdNodes struct {
	mu  sync.Mutex
	ids map[storage.NodeID]bool
}

func newCreatedNodes() *createdNodes {
	return &createdNodes{ids: map[storage.NodeID]bool{}}
}

func (c *createdNodes) add(id storage.NodeID) {
	c.mu.Lock()
	c.ids[id] = true
	c.mu.Unlock()
}

func (c *createdNodes) has(id storage.NodeID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ids[id]
}

// visibilityEvaluator decides visibility for one statement, reading the
// graph through inner (the storage beneath the view) and remembering each
// node's answer. What the statement or its transaction created (created) is
// visible.
type visibilityEvaluator struct {
	v       *GraphVisibility
	inner   storage.Engine
	created *createdNodes

	grantedCollections map[string]bool
	deniedCollections  map[string]bool

	mu         sync.Mutex
	visible    map[storage.NodeID]bool
	inProgress map[storage.NodeID]bool
	hidden     []storage.NodeID            // nodes found hidden during one nodeVisible
	documents  map[string][]storage.NodeID // document key → documents, built on first use
}

func newVisibilityEvaluator(v *GraphVisibility, inner storage.Engine) *visibilityEvaluator {
	e := &visibilityEvaluator{
		v: v, inner: inner,
		grantedCollections: map[string]bool{}, deniedCollections: map[string]bool{},
		visible: map[storage.NodeID]bool{}, inProgress: map[storage.NodeID]bool{},
		created: newCreatedNodes(),
	}
	for _, rule := range v.grantRules {
		if v.inScope(rule) {
			e.grantedCollections[rule.targetValue] = true
		}
	}
	for _, rule := range v.denyRules {
		if v.inScope(rule) {
			e.deniedCollections[rule.targetValue] = true
		}
	}
	return e
}

// nodeVisible reports whether node is visible. A node the statement or its
// transaction created is visible itself, but is never a source another
// node's visibility derives from (nodeVisibleLocked): creating a node can't
// reveal one that exists already.
func (e *visibilityEvaluator) nodeVisible(node *storage.Node) bool {
	if node == nil {
		return false
	}
	if e.created.has(node.ID) {
		return true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.hidden = e.hidden[:0]
	visible := e.nodeVisibleLocked(node)
	if visible {
		// A node found hidden on the way may have been hidden only because
		// a node still being decided (a cycle back to it) counted as hidden,
		// and one of those turned out visible: they are decided again when
		// asked. When node is hidden, so was every node being decided, and
		// those answers stand; a visible answer rests on its own sources.
		for _, id := range e.hidden {
			delete(e.visible, id)
		}
	}
	e.hidden = e.hidden[:0]
	return visible
}

// nodeVisibleLocked decides node by following its sources however long the
// chain; a node already being decided (a cycle) counts as hidden for the
// nodes it is reached from, and nodeVisible discards those answers if the
// node it was asked for turns out visible.
func (e *visibilityEvaluator) nodeVisibleLocked(node *storage.Node) bool {
	if known, ok := e.visible[node.ID]; ok {
		return known
	}
	if e.inProgress[node.ID] {
		return false
	}
	e.inProgress[node.ID] = true
	visible := e.decide(node)
	delete(e.inProgress, node.ID)
	e.visible[node.ID] = visible
	if !visible {
		e.hidden = append(e.hidden, node.ID)
	}
	return visible
}

// decide is a node's visibility: no DENY matches it, and a GRANT matches it
// or one of its scope sources is visible.
func (e *visibilityEvaluator) decide(node *storage.Node) bool {
	for _, deny := range e.v.denies {
		if deny.matches(node.Labels, node.Properties) {
			return false
		}
	}
	for _, rule := range e.v.denyRules {
		if !e.v.inScope(rule) && e.directRuleMatches(rule, node) {
			return false
		}
	}
	// A document denied through one of its collections stays hidden
	// whatever grants it or whatever it derives from; one merely not
	// granted still follows its DERIVE sources below.
	document := e.isScopeDocument(node)
	if document && e.documentDenied(node) {
		return false
	}
	for _, grant := range e.v.grants {
		if grant.matches(node.Labels, node.Properties) {
			return true
		}
	}
	for _, rule := range e.v.grantRules {
		if !e.v.inScope(rule) && e.directRuleMatches(rule, node) {
			return true
		}
	}
	if document && e.documentVisible(node) {
		return true
	}
	scope := e.v.scope
	if scope == nil {
		return false
	}
	for _, entry := range e.applicableEntries(node) {
		for _, source := range entry.sources {
			if e.sourceVisible(node, source) {
				return true
			}
		}
	}
	return false
}

// applicableEntries are the DERIVE entries that govern node, most specific
// wins: of the entries whose labels the node all carries and whose WHERE
// holds, those whose label set no other such entry's strictly contains. A
// document (the MEMBERSHIP label) is governed by its membership, so only
// entries naming the document label also apply to it. When the most
// specific entries name different label sets, neither contains the other
// and no rule says which wins: the node gets no entry and stays hidden
// (fail closed); declaring an entry for the combined labels resolves it.
// Entries for the same label set exclude each other (checked when the scope
// is declared), so at most one of those applies.
func (e *visibilityEvaluator) applicableEntries(node *storage.Node) []deriveEntry {
	scope := e.v.scope
	carries := make(map[string]bool, len(node.Labels))
	for _, label := range node.Labels {
		carries[label] = true
	}
	document := carries[scope.documentLabel]
	var candidates []deriveEntry
	for _, entries := range scope.derived {
		for _, entry := range entries {
			all := true
			namesDocument := false
			for _, label := range entry.labels {
				all = all && carries[label]
				namesDocument = namesDocument || label == scope.documentLabel
			}
			if all && (!document || namesDocument) && entryApplies(entry, node.Properties) {
				candidates = append(candidates, entry)
			}
		}
	}
	var specific []deriveEntry
	for _, candidate := range candidates {
		dominated := false
		for _, other := range candidates {
			if strictLabelSuperset(other.labels, candidate.labels) {
				dominated = true
				break
			}
		}
		if !dominated {
			specific = append(specific, candidate)
		}
	}
	for _, entry := range specific[min(1, len(specific)):] {
		if entry.key() != specific[0].key() {
			return nil
		}
	}
	return specific
}

// strictLabelSuperset reports whether sorted label set a strictly contains b.
func strictLabelSuperset(a, b []string) bool {
	if len(a) <= len(b) {
		return false
	}
	have := make(map[string]bool, len(a))
	for _, label := range a {
		have[label] = true
	}
	for _, label := range b {
		if !have[label] {
			return false
		}
	}
	return true
}

func entryApplies(entry deriveEntry, properties map[string]interface{}) bool {
	for _, test := range entry.tests {
		if !test.holds(properties) {
			return false
		}
	}
	return true
}

func (e *visibilityEvaluator) isScopeDocument(node *storage.Node) bool {
	scope := e.v.scope
	if scope == nil {
		return false
	}
	for _, label := range node.Labels {
		if label == scope.documentLabel {
			return true
		}
	}
	return false
}

// collectionsOf are a document's collection keys, through the scope's
// membership relationship.
func (e *visibilityEvaluator) collectionsOf(document *storage.Node) []string {
	scope := e.v.scope
	var edges []*storage.Edge
	var err error
	if scope.membershipOut {
		edges, err = e.inner.GetOutgoingEdges(document.ID)
	} else {
		edges, err = e.inner.GetIncomingEdges(document.ID)
	}
	if err != nil {
		return nil
	}
	var keys []string
	for _, edge := range edges {
		if edge.Type != scope.membershipRel {
			continue
		}
		other := edge.EndNode
		if !scope.membershipOut {
			other = edge.StartNode
		}
		collection, err := e.inner.GetNode(other)
		if err != nil || !nodeHasLabel(collection, scope.collectionLbl) {
			continue
		}
		if key, ok := collection.Properties[scope.collectionKey].(string); ok && key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

// documentVisible: a document is visible when a granted collection holds
// it and no denied one does (ANY).
func (e *visibilityEvaluator) documentVisible(document *storage.Node) bool {
	granted := false
	for _, key := range e.collectionsOf(document) {
		if e.deniedCollections[key] {
			return false
		}
		if e.grantedCollections[key] {
			granted = true
		}
	}
	return granted
}

func (e *visibilityEvaluator) documentDenied(document *storage.Node) bool {
	for _, key := range e.collectionsOf(document) {
		if e.deniedCollections[key] {
			return true
		}
	}
	return false
}

func (e *visibilityEvaluator) collectionVisible(key string) bool {
	return key != "" && e.grantedCollections[key] && !e.deniedCollections[key]
}

// directRuleMatches decides a collection rule that doesn't match the scope's
// membership pattern on its own label: the node has the rule's relationship
// to the named collection.
func (e *visibilityEvaluator) directRuleMatches(rule graphSegment, node *storage.Node) bool {
	if !nodeHasLabel(node, rule.label) {
		return false
	}
	var edges []*storage.Edge
	var err error
	if rule.outgoing {
		edges, err = e.inner.GetOutgoingEdges(node.ID)
	} else {
		edges, err = e.inner.GetIncomingEdges(node.ID)
	}
	if err != nil {
		return false
	}
	for _, edge := range edges {
		if edge.Type != rule.relType {
			continue
		}
		other := edge.EndNode
		if !rule.outgoing {
			other = edge.StartNode
		}
		target, err := e.inner.GetNode(other)
		if err == nil && nodeHasLabel(target, rule.target) && target.Properties[rule.targetKey] == rule.targetValue {
			return true
		}
	}
	return false
}

// sourceVisible: one DERIVE source of node is visible.
func (e *visibilityEvaluator) sourceVisible(node *storage.Node, source scopeSource) bool {
	switch source.kind {
	case sourceDocumentKey:
		for _, key := range keyValues(node.Properties[source.property]) {
			for _, id := range e.documentsByKey(key) {
				if document, err := e.inner.GetNode(id); err == nil && e.nodeVisibleLocked(document) {
					return true
				}
			}
		}
		return false
	case sourceCollectionKey:
		for _, key := range keyValues(node.Properties[source.property]) {
			if e.collectionVisible(key) {
				return true
			}
		}
		return false
	}
	for _, end := range e.walk(node.ID, source.steps) {
		target, err := e.inner.GetNode(end)
		if err != nil {
			continue
		}
		if !nodeHasLabels(target, source.endLabels) {
			continue
		}
		if source.kind == sourceDocumentPath && !e.isScopeDocument(target) {
			continue
		}
		if e.nodeVisibleLocked(target) {
			return true
		}
	}
	return false
}

func nodeHasLabels(node *storage.Node, labels []string) bool {
	for _, label := range labels {
		if !nodeHasLabel(node, label) {
			return false
		}
	}
	return true
}

// keyValues reads a key property: a string or a list of strings.
func keyValues(value interface{}) []string {
	switch v := value.(type) {
	case string:
		if v != "" {
			return []string{v}
		}
	case []string:
		return v
	case []interface{}:
		var out []string
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// documentsByKey are the documents whose key is key: an index of the scope's
// documents by key, built on first use for the statement.
func (e *visibilityEvaluator) documentsByKey(key string) []storage.NodeID {
	if e.documents == nil {
		e.documents = map[string][]storage.NodeID{}
		nodes, err := e.inner.GetNodesByLabel(e.v.scope.documentLabel)
		if err == nil {
			for _, document := range nodes {
				if k, ok := document.Properties[e.v.scope.documentKey].(string); ok && k != "" {
					e.documents[k] = append(e.documents[k], document.ID)
				}
			}
		}
	}
	return e.documents[key]
}

// walk follows a fixed pattern from start and returns the nodes it ends at.
func (e *visibilityEvaluator) walk(start storage.NodeID, steps []patternStep) []storage.NodeID {
	frontier := []storage.NodeID{start}
	for _, step := range steps {
		var next []storage.NodeID
		seen := map[storage.NodeID]bool{}
		follow := func(edges []*storage.Edge, outgoing bool) {
			for _, edge := range edges {
				if edge.Type != step.relType || !relationshipPropertiesMatch(edge, step.relProps) {
					continue
				}
				other := edge.EndNode
				if !outgoing {
					other = edge.StartNode
				}
				if seen[other] {
					continue
				}
				if len(step.nodeLabels) > 0 {
					reached, err := e.inner.GetNode(other)
					if err != nil || !nodeHasLabels(reached, step.nodeLabels) {
						continue
					}
				}
				seen[other] = true
				next = append(next, other)
			}
		}
		for _, id := range frontier {
			if step.direction >= 0 {
				if out, err := e.inner.GetOutgoingEdges(id); err == nil {
					follow(out, true)
				}
			}
			if step.direction <= 0 {
				if in, err := e.inner.GetIncomingEdges(id); err == nil {
					follow(in, false)
				}
			}
		}
		frontier = next
		if len(frontier) == 0 {
			return nil
		}
	}
	return frontier
}

func relationshipPropertiesMatch(edge *storage.Edge, props map[string]interface{}) bool {
	for key, value := range props {
		if !compareValues(edge.Properties[key], value) {
			return false
		}
	}
	return true
}

// edgeVisible: a relationship is visible when both its ends are.
func (e *visibilityEvaluator) edgeVisible(edge *storage.Edge) bool {
	if edge == nil {
		return false
	}
	return e.nodeIDVisible(edge.StartNode) && e.nodeIDVisible(edge.EndNode)
}

// edgeIDVisible is edgeVisible by relationship ID, for index candidates.
func (e *visibilityEvaluator) edgeIDVisible(id storage.EdgeID) bool {
	edge, err := e.inner.GetEdge(id)
	if err != nil {
		return false
	}
	return e.edgeVisible(edge)
}

func (e *visibilityEvaluator) nodeIDVisible(id storage.NodeID) bool {
	if e.created.has(id) {
		return true
	}
	e.mu.Lock()
	known, ok := e.visible[id]
	e.mu.Unlock()
	if ok {
		return known
	}
	node, err := e.inner.GetNode(id)
	if err != nil {
		return false
	}
	return e.nodeVisible(node)
}
