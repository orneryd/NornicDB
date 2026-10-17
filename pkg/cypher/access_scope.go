package cypher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// The access scope: NornicDB's administration command declaring how a
// graph's collection membership reaches the nodes derived from a document.
// Neo4j has no equivalent, so it follows the CREATE / DROP / SHOW family:
//
//	CREATE [OR REPLACE] ACCESS SCOPE name [IF NOT EXISTS] ON GRAPH graph
//	  MEMBERSHIP (d:DocumentLabel)-[:REL]->(c:CollectionLabel) KEY d.key, c.key
//	  [DERIVE (x:Label) [WHERE test [AND test]…] FROM source [OR FROM source]…]…
//	DROP ACCESS SCOPE name [IF EXISTS] ON GRAPH graph
//	SHOW ACCESS SCOPES [ON GRAPH graph]
//
//	test   := x.p = literal | x.p <> literal | x.p IS NULL | x.p IS NOT NULL
//	source := DOCUMENT x.p        x.p holds a document's key (or a list of them)
//	        | COLLECTION x.p      x.p holds a collection's key (or a list of them)
//	        | (x)…(d)             a fixed-length pattern ending at a document
//	        | (x)…(t[:Label…])    a fixed-length pattern ending at any node, whose
//	                              visibility x follows
//
// A node's sources combine by ANY: it is visible when one of them is. A
// graph has at most one scope.

// accessScope is a parsed access scope.
type accessScope struct {
	graph, name    string
	documentLabel  string
	membershipRel  string
	membershipOut  bool
	collectionLbl  string
	documentKey    string
	collectionKey  string
	derived        map[string][]deriveEntry // by label set (deriveEntry.key)
	definitionText string
}

// deriveEntry is one DERIVE clause.
type deriveEntry struct {
	labels  []string // sorted; the entry is for nodes carrying all of them
	tests   []scopeTest
	sources []scopeSource
}

// key is the entry's label set, as the scope indexes its entries.
func (d deriveEntry) key() string { return strings.Join(d.labels, ":") }

// sortedUniqueLabels is labels sorted, without repeats.
func sortedUniqueLabels(labels []string) []string {
	out := append([]string(nil), labels...)
	sort.Strings(out)
	unique := out[:0]
	for i, label := range out {
		if i == 0 || label != out[i-1] {
			unique = append(unique, label)
		}
	}
	return unique
}

// scopeTest is a DERIVE discriminator.
type scopeTest struct {
	property string
	operator string // =, <>, IS NULL, IS NOT NULL
	value    interface{}
}

func (t scopeTest) holds(properties map[string]interface{}) bool {
	return propertyRuleHolds(t.operator, properties[t.property], t.value)
}

type scopeSourceKind int

const (
	sourceDocumentKey scopeSourceKind = iota
	sourceCollectionKey
	sourceDocumentPath
	sourceFollowPath
)

// scopeSource is one FROM source.
type scopeSource struct {
	kind      scopeSourceKind
	property  string        // key sources
	steps     []patternStep // path sources
	endLabels []string      // path sources: the end node's labels
}

// patternStep is one relationship of a source pattern and the node it
// reaches.
type patternStep struct {
	relType    string
	direction  int // 1 outgoing, -1 incoming, 0 either
	relProps   map[string]interface{}
	nodeLabels []string
}

func isAccessScopeCommand(cypher string) bool {
	words := leadingUpperWords(cypher, 5)
	switch {
	case len(words) >= 3 && words[0] == "CREATE" && words[1] == "ACCESS" && words[2] == "SCOPE":
		return true
	case len(words) >= 5 && words[0] == "CREATE" && words[1] == "OR" && words[2] == "REPLACE" && words[3] == "ACCESS" && words[4] == "SCOPE":
		return true
	case len(words) >= 3 && words[0] == "DROP" && words[1] == "ACCESS" && words[2] == "SCOPE":
		return true
	case len(words) >= 3 && words[0] == "SHOW" && words[1] == "ACCESS" && (words[2] == "SCOPES" || words[2] == "SCOPE"):
		return true
	}
	return false
}

// parseAccessScope parses CREATE [OR REPLACE] ACCESS SCOPE; replace and
// ifNotExists report the clauses.
func parseAccessScope(statement string) (scope *accessScope, replace, ifNotExists bool, err error) {
	tokens, err := lexPrivilegeStatement(statement)
	if err != nil {
		return nil, false, false, err
	}
	p := &privilegeParser{tokens: tokens}
	if !p.keyword("CREATE") {
		return nil, false, false, errors.New("expected CREATE")
	}
	if p.keyword("OR") {
		if !p.keyword("REPLACE") {
			return nil, false, false, errors.New("expected REPLACE")
		}
		replace = true
	}
	if !p.keyword("ACCESS") || !p.keyword("SCOPE") {
		return nil, false, false, errors.New("expected ACCESS SCOPE")
	}
	scope = &accessScope{derived: map[string][]deriveEntry{}, definitionText: strings.TrimSpace(statement)}
	if scope.name, err = p.name(); err != nil {
		return nil, false, false, err
	}
	if p.keyword("IF") {
		if !p.keyword("NOT") || !p.keyword("EXISTS") {
			return nil, false, false, errors.New("expected IF NOT EXISTS")
		}
		ifNotExists = true
	}
	if replace && ifNotExists {
		return nil, false, false, errors.New("OR REPLACE and IF NOT EXISTS cannot be used together")
	}
	if !p.keyword("ON") || !p.keyword("GRAPH") {
		return nil, false, false, errors.New("expected ON GRAPH")
	}
	if scope.graph, err = p.name(); err != nil {
		return nil, false, false, err
	}
	if err := scope.parseMembership(p); err != nil {
		return nil, false, false, err
	}
	for p.keyword("DERIVE") {
		entry, err := scope.parseDerive(p)
		if err != nil {
			return nil, false, false, err
		}
		// Entries for the same label set are equally specific: their
		// WHEREs must exclude each other (most specific wins otherwise,
		// see visibilityEvaluator.applicableEntries).
		key := entry.key()
		for _, existing := range scope.derived[key] {
			if !entriesExclusive(existing, entry) {
				return nil, false, false, fmt.Errorf("DERIVE entries for %s overlap; give each a WHERE that excludes the others", key)
			}
		}
		scope.derived[key] = append(scope.derived[key], entry)
	}
	if p.pos != len(p.tokens) {
		return nil, false, false, fmt.Errorf("unexpected %q", p.peek().text)
	}
	return scope, replace, ifNotExists, nil
}

// parseMembership reads MEMBERSHIP (d:L)-[:R]->(c:C) KEY d.k, c.k.
func (s *accessScope) parseMembership(p *privilegeParser) error {
	if !p.keyword("MEMBERSHIP") {
		return errors.New("expected MEMBERSHIP")
	}
	documentVar, documentLabels, err := p.patternNode()
	if err != nil {
		return err
	}
	if documentVar == "" || len(documentLabels) != 1 {
		return errors.New("MEMBERSHIP names the document as (d:Label)")
	}
	relType, direction, _, err := p.patternRelationship()
	if err != nil {
		return err
	}
	if direction == 0 {
		return errors.New("MEMBERSHIP needs a direction")
	}
	collectionVar, collectionLabels, err := p.patternNode()
	if err != nil {
		return err
	}
	if collectionVar == "" || len(collectionLabels) != 1 {
		return errors.New("MEMBERSHIP names the collection as (c:Label)")
	}
	if !p.keyword("KEY") {
		return errors.New("expected KEY")
	}
	readKey := func(variable string) (string, error) {
		owner, err := p.name()
		if err != nil {
			return "", err
		}
		if owner != variable {
			return "", fmt.Errorf("expected %s.<property>", variable)
		}
		if err := p.expectPunct("."); err != nil {
			return "", err
		}
		return p.name()
	}
	if s.documentKey, err = readKey(documentVar); err != nil {
		return err
	}
	if err := p.expectPunct(","); err != nil {
		return err
	}
	if s.collectionKey, err = readKey(collectionVar); err != nil {
		return err
	}
	s.documentLabel, s.membershipRel, s.membershipOut, s.collectionLbl = documentLabels[0], relType, direction > 0, collectionLabels[0]
	return nil
}

// patternNode reads ([variable][:Label…]).
func (p *privilegeParser) patternNode() (string, []string, error) {
	if err := p.expectPunct("("); err != nil {
		return "", nil, err
	}
	variable := ""
	if t := p.peek(); t.kind == 'w' || t.kind == 'q' {
		variable = t.text
		p.pos++
	}
	var labels []string
	for p.punct(":") {
		label, err := p.name()
		if err != nil {
			return "", nil, err
		}
		labels = append(labels, label)
	}
	return variable, labels, p.expectPunct(")")
}

// patternRelationship reads -[:R {k: v}]->, <-[:R]- or -[:R]-.
func (p *privilegeParser) patternRelationship() (string, int, map[string]interface{}, error) {
	incoming := false
	switch {
	case p.punct("<-"):
		incoming = true
	case p.punct("-"):
	default:
		return "", 0, nil, errors.New("expected a relationship")
	}
	if err := p.expectPunct("["); err != nil {
		return "", 0, nil, err
	}
	if err := p.expectPunct(":"); err != nil {
		return "", 0, nil, err
	}
	relType, err := p.name()
	if err != nil {
		return "", 0, nil, err
	}
	var props map[string]interface{}
	if p.punct("{") {
		props = map[string]interface{}{}
		for {
			key, err := p.name()
			if err != nil {
				return "", 0, nil, err
			}
			if err := p.expectPunct(":"); err != nil {
				return "", 0, nil, err
			}
			value, err := p.literal()
			if err != nil {
				return "", 0, nil, err
			}
			props[key] = value
			if p.punct("}") {
				break
			}
			if err := p.expectPunct(","); err != nil {
				return "", 0, nil, err
			}
		}
	}
	if err := p.expectPunct("]"); err != nil {
		return "", 0, nil, err
	}
	switch {
	case incoming && p.punct("-"):
		return relType, -1, props, nil
	case !incoming && p.punct("->"):
		return relType, 1, props, nil
	case !incoming && p.punct("-"):
		return relType, 0, props, nil
	}
	return "", 0, nil, errors.New("invalid relationship direction")
}

// parseDerive reads one DERIVE clause after the keyword.
func (s *accessScope) parseDerive(p *privilegeParser) (deriveEntry, error) {
	var entry deriveEntry
	variable, labels, err := p.patternNode()
	if err != nil {
		return entry, err
	}
	if variable == "" || len(labels) == 0 {
		return entry, errors.New("DERIVE names its node as (x:Label[:Label]...)")
	}
	entry.labels = sortedUniqueLabels(labels)
	if p.keyword("WHERE") {
		for {
			test, err := p.scopeTest(variable)
			if err != nil {
				return entry, err
			}
			entry.tests = append(entry.tests, test)
			if !p.keyword("AND") {
				break
			}
		}
	}
	for {
		if !p.keyword("FROM") {
			return entry, errors.New("expected FROM")
		}
		source, err := s.parseSource(p, variable)
		if err != nil {
			return entry, err
		}
		entry.sources = append(entry.sources, source)
		if !p.keyword("OR") {
			return entry, nil
		}
	}
}

func (p *privilegeParser) propertyOf(variable string) (string, error) {
	owner, err := p.name()
	if err != nil {
		return "", err
	}
	if owner != variable {
		return "", fmt.Errorf("expected %s.<property>", variable)
	}
	if err := p.expectPunct("."); err != nil {
		return "", err
	}
	return p.name()
}

func (p *privilegeParser) scopeTest(variable string) (scopeTest, error) {
	var test scopeTest
	var err error
	if test.property, err = p.propertyOf(variable); err != nil {
		return test, err
	}
	switch {
	case p.keyword("IS"):
		test.operator = "IS NULL"
		if p.keyword("NOT") {
			test.operator = "IS NOT NULL"
		}
		if !p.keyword("NULL") {
			return test, errors.New("expected NULL")
		}
		return test, nil
	case p.punct("="):
		test.operator = "="
	case p.punct("<>"):
		test.operator = "<>"
	default:
		return test, errors.New("a DERIVE test is =, <>, IS NULL or IS NOT NULL")
	}
	test.value, err = p.literal()
	return test, err
}

func (s *accessScope) parseSource(p *privilegeParser, variable string) (scopeSource, error) {
	var source scopeSource
	var err error
	switch {
	case p.keyword("DOCUMENT"):
		source.kind = sourceDocumentKey
		source.property, err = p.propertyOf(variable)
		return source, err
	case p.keyword("COLLECTION"):
		source.kind = sourceCollectionKey
		source.property, err = p.propertyOf(variable)
		return source, err
	}
	start, startLabels, err := p.patternNode()
	if err != nil {
		return source, err
	}
	if start != variable || len(startLabels) > 0 {
		return source, fmt.Errorf("a FROM pattern starts at (%s)", variable)
	}
	for {
		relType, direction, props, err := p.patternRelationship()
		if err != nil {
			return source, err
		}
		nodeVar, labels, err := p.patternNode()
		if err != nil {
			return source, err
		}
		source.steps = append(source.steps, patternStep{relType: relType, direction: direction, relProps: props, nodeLabels: labels})
		if nodeVar != "" {
			if nodeVar == variable {
				return source, errors.New("a FROM pattern can't return to its start")
			}
			source.endLabels = labels
			source.kind = sourceFollowPath
			if nodeVar == "d" {
				source.kind = sourceDocumentPath
			}
			return source, nil
		}
		if t := p.peek(); !(t.kind == 'o' && (t.text == "-" || t.text == "<-")) {
			return source, errors.New("a FROM pattern ends at a named node")
		}
		if len(source.steps) >= 6 {
			return source, errors.New("a FROM pattern has at most six relationships")
		}
	}
}

// entriesExclusive reports whether no node can satisfy both entries' tests:
// some property is tested to different values, or to a value and its
// negation or null.
func entriesExclusive(a, b deriveEntry) bool {
	for _, x := range a.tests {
		for _, y := range b.tests {
			if x.property != y.property {
				continue
			}
			switch {
			case x.operator == "IS NULL" && (y.operator == "IS NOT NULL" || y.operator == "="),
				y.operator == "IS NULL" && (x.operator == "IS NOT NULL" || x.operator == "="):
				return true
			case x.operator == "=" && y.operator == "=" && !compareValues(x.value, y.value):
				return true
			case x.operator == "=" && y.operator == "<>" && compareValues(x.value, y.value),
				x.operator == "<>" && y.operator == "=" && compareValues(x.value, y.value):
				return true
			}
		}
	}
	return false
}

// accessScopeFor reads graph's scope from the system database; nil when it
// has none.
func accessScopeFor(ctx context.Context, store *auth.AccessScopeStore, graph string) (*accessScope, error) {
	stored, found, err := store.Get(ctx, graph)
	if err != nil || !found {
		return nil, err
	}
	scope, _, _, err := parseAccessScope(stored.Definition)
	if err != nil {
		return nil, fmt.Errorf("stored access scope of %s is invalid: %w", graph, err)
	}
	return scope, nil
}

func accessScopeSyntaxError(cypher string, err error) error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
		localization.CypherAdminRoleCommandInvalid(strings.TrimSpace(cypher), err.Error()))
}

// executeAccessScopeCommand runs CREATE / DROP ACCESS SCOPE. CREATE fails
// when the graph has a scope, unless IF NOT EXISTS (no change) or OR
// REPLACE; DROP fails when the graph has no scope of that name, unless IF
// EXISTS. A changed scope applies from the next transaction.
func (e *StorageExecutor) executeAccessScopeCommand(ctx context.Context, cypher string) (*ExecuteResult, error) {
	system, err := e.privilegeStorage(true)
	if err != nil {
		return nil, err
	}
	store := auth.NewAccessScopeStore(system)
	empty := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
	if startsWithKeywordFold(strings.TrimSpace(cypher), "DROP") {
		tokens, err := lexPrivilegeStatement(cypher)
		if err != nil {
			return nil, accessScopeSyntaxError(cypher, err)
		}
		p := &privilegeParser{tokens: tokens}
		p.keyword("DROP")
		p.keyword("ACCESS")
		p.keyword("SCOPE")
		name, err := p.name()
		if err != nil {
			return nil, accessScopeSyntaxError(cypher, err)
		}
		ifExists := p.keyword("IF") && p.keyword("EXISTS")
		if !p.keyword("ON") || !p.keyword("GRAPH") {
			return nil, accessScopeSyntaxError(cypher, errors.New("expected ON GRAPH"))
		}
		graph, err := p.name()
		if err != nil || p.pos != len(p.tokens) {
			return nil, accessScopeSyntaxError(cypher, errors.New("expected a graph name"))
		}
		existing, found, err := store.Get(ctx, graph)
		if err != nil {
			return nil, err
		}
		if !found || existing.Name != name {
			if ifExists {
				return empty, nil
			}
			return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "AccessScopeNotFound",
				localization.CypherAdminAccessScopeNotFound(name, graph))
		}
		_, err = store.Delete(ctx, graph)
		return empty, err
	}
	scope, replace, ifNotExists, err := parseAccessScope(cypher)
	if err != nil {
		return nil, accessScopeSyntaxError(cypher, err)
	}
	_, found, err := store.Get(ctx, scope.graph)
	if err != nil {
		return nil, err
	}
	if found && !replace {
		if ifNotExists {
			return empty, nil
		}
		return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "AccessScopeExists",
			localization.CypherAdminAccessScopeExists(scope.graph))
	}
	return empty, store.Put(ctx, auth.AccessScope{Graph: scope.graph, Name: scope.name, Definition: scope.definitionText})
}

// executeShowAccessScopes lists the scopes (graph, name, definition).
func (e *StorageExecutor) executeShowAccessScopes(ctx context.Context, cypher string) (*ExecuteResult, error) {
	system, err := e.privilegeStorage(false)
	if err != nil {
		return nil, err
	}
	scopes, err := auth.NewAccessScopeStore(system).List(ctx)
	if err != nil {
		return nil, err
	}
	onGraph := ""
	if words := strings.Fields(cypher); len(words) >= 6 && strings.EqualFold(words[3], "ON") && strings.EqualFold(words[4], "GRAPH") {
		onGraph = auth.NormalizeGraphName(strings.Trim(words[5], "`"))
	}
	result := &ExecuteResult{Columns: []string{"graph", "name", "definition"}, Rows: [][]interface{}{}}
	for _, scope := range scopes {
		if onGraph != "" && scope.Graph != onGraph {
			continue
		}
		result.Rows = append(result.Rows, []interface{}{scope.Graph, scope.Name, scope.Definition})
	}
	return result, nil
}
