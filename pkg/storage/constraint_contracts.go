package storage

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// ============================================================================
// Allocation-free keyword scanning helpers (same style as pkg/cypher/keyword_scan.go).
// Written locally because pkg/storage must not import pkg/cypher.
// ============================================================================

// ccSkipSpaces advances past whitespace.
func ccSkipSpaces(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

// ccIsIdentStart returns true for [A-Za-z_].
func ccIsIdentStart(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || b == '_'
}

// ccIsIdentByte returns true for [A-Za-z0-9_].
func ccIsIdentByte(b byte) bool {
	return ccIsIdentStart(b) || (b >= '0' && b <= '9')
}

// ccScanIdent reads an identifier at position i. Returns ("", i) if none.
func ccScanIdent(s string, i int) (string, int) {
	if i >= len(s) || !ccIsIdentStart(s[i]) {
		return "", i
	}
	start := i
	for i < len(s) && ccIsIdentByte(s[i]) {
		i++
	}
	return s[start:i], i
}

// ccMatchKeywordAt checks for a case-insensitive keyword at position i with
// word-boundary checking. Returns the position after the keyword, or -1.
func ccMatchKeywordAt(s string, i int, kw string) int {
	if i+len(kw) > len(s) {
		return -1
	}
	for k := 0; k < len(kw); k++ {
		sc, kc := s[i+k], kw[k]
		if sc >= 'a' && sc <= 'z' {
			sc -= 'a' - 'A'
		}
		if kc >= 'a' && kc <= 'z' {
			kc -= 'a' - 'A'
		}
		if sc != kc {
			return -1
		}
	}
	// Right boundary: must not be followed by ident char.
	end := i + len(kw)
	if end < len(s) && ccIsIdentByte(s[end]) {
		return -1
	}
	return end
}

// ccScanComparator reads a comparison operator at position i.
func ccScanComparator(s string, i int) (string, int) {
	if i >= len(s) {
		return "", i
	}
	if i+1 < len(s) {
		two := s[i : i+2]
		switch two {
		case "<=", ">=", "<>", "!=":
			return two, i + 2
		}
	}
	switch s[i] {
	case '=', '<', '>':
		return s[i : i+1], i + 1
	}
	return "", i
}

// ccExpectByte checks s[i]==b, returns i+1 or -1.
func ccExpectByte(s string, i int, b byte) int {
	if i < len(s) && s[i] == b {
		return i + 1
	}
	return -1
}

// ccScanLabels reads zero or more `:Label` sequences starting at i.
func ccScanLabels(s string, i int) ([]string, int) {
	var labels []string
	for {
		p := ccSkipSpaces(s, i)
		if p >= len(s) || s[p] != ':' {
			return labels, i
		}
		p++ // skip ':'
		p = ccSkipSpaces(s, p)
		label, next := ccScanIdent(s, p)
		if label == "" {
			return labels, i // colon without ident — don't consume
		}
		labels = append(labels, label)
		i = next
	}
}

// ccScanNodeContents reads the inside of a node pattern (...) starting after '('.
// Returns any labels found and the position after ')'.
func ccScanNodeContents(s string, i int) ([]string, int, bool) {
	p := ccSkipSpaces(s, i)
	// Optional variable name
	if _, next := ccScanIdent(s, p); next > p {
		p = next
	}
	// Zero or more :Label
	labels, p := ccScanLabels(s, p)
	p = ccSkipSpaces(s, p)
	p = ccExpectByte(s, p, ')')
	if p == -1 {
		return nil, 0, false
	}
	return labels, p, true
}

// hasAllLabels checks that all required labels exist on the node.
func hasAllLabels(labels []string, required []string) bool {
	for _, req := range required {
		if !hasLabel(labels, req) {
			return false
		}
	}
	return true
}

const (
	ConstraintContractKindPrimitiveNode         = "primitive-node"
	ConstraintContractKindPrimitiveRelationship = "primitive-relationship"
	ConstraintContractKindBooleanNode           = "boolean-node"
	ConstraintContractKindBooleanRelationship   = "boolean-relationship"
)

type ConstraintContract struct {
	Name              string                    `json:"name"`
	TargetEntityType  string                    `json:"target_entity_type"`
	TargetLabelOrType string                    `json:"target_label_or_type"`
	Definition        string                    `json:"definition"`
	Entries           []ConstraintContractEntry `json:"entries,omitempty"`
}

type ConstraintContractEntry struct {
	Kind          string   `json:"kind"`
	PrimitiveType string   `json:"primitive_type,omitempty"`
	Properties    []string `json:"properties,omitempty"`
	Property      string   `json:"property,omitempty"`
	ExpectedType  string   `json:"expected_type,omitempty"`
	Expression    string   `json:"expression,omitempty"`
}

func cloneConstraintContractEntry(entry ConstraintContractEntry) ConstraintContractEntry {
	cloned := entry
	if len(entry.Properties) > 0 {
		cloned.Properties = append([]string(nil), entry.Properties...)
	}
	return cloned
}

func cloneConstraintContract(contract ConstraintContract) ConstraintContract {
	cloned := contract
	if len(contract.Entries) > 0 {
		cloned.Entries = make([]ConstraintContractEntry, 0, len(contract.Entries))
		for _, entry := range contract.Entries {
			cloned.Entries = append(cloned.Entries, cloneConstraintContractEntry(entry))
		}
	}
	return cloned
}

func constraintContractEqual(a, b ConstraintContract) bool {
	if a.Name != b.Name ||
		a.TargetEntityType != b.TargetEntityType ||
		a.TargetLabelOrType != b.TargetLabelOrType ||
		a.Definition != b.Definition ||
		len(a.Entries) != len(b.Entries) {
		return false
	}
	for i := range a.Entries {
		left := a.Entries[i]
		right := b.Entries[i]
		if left.Kind != right.Kind ||
			left.PrimitiveType != right.PrimitiveType ||
			left.Property != right.Property ||
			left.ExpectedType != right.ExpectedType ||
			left.Expression != right.Expression ||
			len(left.Properties) != len(right.Properties) {
			return false
		}
		for j := range left.Properties {
			if left.Properties[j] != right.Properties[j] {
				return false
			}
		}
	}
	return true
}

func (sm *SchemaManager) GetAllConstraintContracts() []ConstraintContract {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	contracts := make([]ConstraintContract, 0, len(sm.constraintContracts))
	for _, contract := range sm.constraintContracts {
		contracts = append(contracts, cloneConstraintContract(contract))
	}
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].Name < contracts[j].Name
	})
	return contracts
}

// HasAnyConstraintContract reports whether this schema has at least one
// constraint contract registered. Callers use it to short-circuit work in
// the per-commit validator (the expensive adjacency walk is pointless when
// no contract exists).
func (sm *SchemaManager) HasAnyConstraintContract() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.constraintContracts) > 0
}

func (sm *SchemaManager) GetConstraintContractsForTarget(entityType ConstraintEntityType, labelOrType string) []ConstraintContract {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	contracts := make([]ConstraintContract, 0)
	for _, contract := range sm.constraintContracts {
		if contract.TargetEntityType != string(entityType) || contract.TargetLabelOrType != labelOrType {
			continue
		}
		contracts = append(contracts, cloneConstraintContract(contract))
	}
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].Name < contracts[j].Name
	})
	return contracts
}

func (sm *SchemaManager) AddConstraintContractBundle(contract ConstraintContract, compiledConstraints []Constraint, compiledTypes []PropertyTypeConstraint, ifNotExists bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snapshot := sm.exportDefinitionLocked()

	if existing, exists := sm.constraintContracts[contract.Name]; exists {
		if ifNotExists && constraintContractEqual(existing, contract) {
			return nil
		}
		return localizedError(localization.StorageSchemaConstraintContractAlreadyExists(contract.Name), nil)
	}
	if _, exists := sm.constraints[contract.Name]; exists {
		return localizedError(localization.StorageSchemaConstraintContractNameConflict(contract.Name), nil)
	}
	if _, exists := sm.propertyTypeConstraints[contract.Name]; exists {
		return localizedError(localization.StorageSchemaConstraintContractNameConflict(contract.Name), nil)
	}

	for _, compiled := range compiledConstraints {
		if err := sm.addConstraintLocked(compiled, false); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}
	for _, compiled := range compiledTypes {
		if err := sm.addPropertyTypeConstraintValueLocked(compiled, false); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}

	sm.constraintContracts[contract.Name] = cloneConstraintContract(contract)

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}

	return nil
}

func ValidateConstraintContractOnCreationForEngine(engine Engine, contract ConstraintContract) error {
	entityType := ConstraintEntityType(contract.TargetEntityType)
	switch entityType {
	case ConstraintEntityNode:
		nodes, err := engine.GetNodesByLabel(contract.TargetLabelOrType)
		if err != nil {
			return localizedError(localization.StorageSchemaScanNodesFailed(err), err)
		}
		for _, node := range nodes {
			if err := validateConstraintContractForNodeEngine(engine, contract, node); err != nil {
				return err
			}
		}
	case ConstraintEntityRelationship:
		edges, err := engine.GetEdgesByType(contract.TargetLabelOrType)
		if err != nil {
			return localizedError(localization.StorageSchemaScanRelationshipsFailed(err), err)
		}
		for _, edge := range edges {
			if err := validateConstraintContractForEdgeEngine(engine, contract, edge); err != nil {
				return err
			}
		}
	default:
		return localizedError(localization.StorageSchemaUnsupportedContractTargetEntityType(contract.TargetEntityType), nil)
	}
	return nil
}

func validateConstraintContractForNodeEngine(engine Engine, contract ConstraintContract, node *Node) error {
	for _, entry := range contract.Entries {
		if entry.Kind != ConstraintContractKindBooleanNode {
			continue
		}
		ok, err := evaluateNodeConstraintContractExpressionEngine(engine, node, entry.Expression)
		if err != nil {
			return localizedError(localization.StorageSchemaConstraintContractInvalid(contract.Name, entry.Expression, err), err)
		}
		if !ok {
			return localizedError(localization.StorageSchemaConstraintContractViolated(contract.Name, entry.Expression), nil)
		}
	}
	return nil
}

func validateConstraintContractForEdgeEngine(engine Engine, contract ConstraintContract, edge *Edge) error {
	for _, entry := range contract.Entries {
		if entry.Kind != ConstraintContractKindBooleanRelationship {
			continue
		}
		ok, err := evaluateRelationshipConstraintContractExpressionEngine(engine, edge, entry.Expression)
		if err != nil {
			return localizedError(localization.StorageSchemaConstraintContractInvalid(contract.Name, entry.Expression, err), err)
		}
		if !ok {
			return localizedError(localization.StorageSchemaConstraintContractViolated(contract.Name, entry.Expression), nil)
		}
	}
	return nil
}

func evaluateNodeConstraintContractExpressionEngine(engine Engine, node *Node, expr string) (bool, error) {
	if matched, values, property, err := parsePropertyInExpression(expr); err != nil {
		return false, err
	} else if matched {
		return evaluatePropertyInExpression(node.Properties[property], values), nil
	}

	if matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression(expr); err != nil {
		return false, err
	} else if matched {
		return compareConstraintExpressionValue(node.Properties[property], comparator, value), nil
	}

	if matched, pattern, comparator, threshold, err := parseCountPatternExpression(expr); err != nil {
		return false, err
	} else if matched {
		count, err := countMatchingPatternEdgesEngine(engine, node, pattern)
		if err != nil {
			return false, err
		}
		return compareInt(count, comparator, threshold), nil
	}

	if matched, pattern, err := parseNotExistsPatternExpression(expr); err != nil {
		return false, err
	} else if matched {
		count, err := countMatchingPatternEdgesEngine(engine, node, pattern)
		if err != nil {
			return false, err
		}
		return count == 0, nil
	}

	return false, localizedError(localization.StorageSchemaUnsupportedNodePredicate(), nil)
}

func evaluateRelationshipConstraintContractExpressionEngine(engine Engine, edge *Edge, expr string) (bool, error) {
	if matched, property, values, err := parseRelationshipPropertyInExpression(expr); err != nil {
		return false, err
	} else if matched {
		return evaluatePropertyInExpression(edge.Properties[property], values), nil
	}

	if isDistinctEndpointsExpression(expr) {
		return edge.StartNode != edge.EndNode, nil
	}

	if matched, leftProp, rightProp := parseEndpointPropertyEqualityExpression(expr); matched {
		startNode, err := engine.GetNode(edge.StartNode)
		if err != nil {
			return false, err
		}
		endNode, err := engine.GetNode(edge.EndNode)
		if err != nil {
			return false, err
		}
		if startNode == nil || endNode == nil {
			return false, localizedError(localization.StorageSchemaMissingRelationshipEndpoint(), nil)
		}
		return compareValues(startNode.Properties[leftProp], endNode.Properties[rightProp]), nil
	}

	if matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression(expr); err != nil {
		return false, err
	} else if matched {
		return compareConstraintExpressionValue(edge.Properties[property], comparator, value), nil
	}

	return false, localizedError(localization.StorageSchemaUnsupportedRelationshipPredicate(), nil)
}

type contractPattern struct {
	Direction    string
	RelationType string
	TargetLabels []string
}

// ============================================================================
// Expression parsers — keyword scanning, no regex.
// ============================================================================

// parsePropertyInExpression: ident.property IN [values]
func parsePropertyInExpression(expr string) (bool, []interface{}, string, error) {
	s := strings.TrimSpace(expr)
	p := 0

	// ident (variable name)
	_, p = ccScanIdent(s, p)
	if p == 0 {
		return false, nil, "", nil
	}
	// .
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '.') == -1 {
		return false, nil, "", nil
	}
	p++
	p = ccSkipSpaces(s, p)
	// property name
	property, next := ccScanIdent(s, p)
	if property == "" {
		return false, nil, "", nil
	}
	p = next
	// IN keyword
	p = ccSkipSpaces(s, p)
	if end := ccMatchKeywordAt(s, p, "IN"); end == -1 {
		return false, nil, "", nil
	} else {
		p = end
	}
	// [
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '[') == -1 {
		return false, nil, "", nil
	}
	p++
	// Find matching ]
	bracketStart := p
	depth := 1
	for p < len(s) && depth > 0 {
		switch s[p] {
		case '[':
			depth++
		case ']':
			depth--
		}
		if depth > 0 {
			p++
		}
	}
	if depth != 0 {
		return false, nil, "", nil
	}
	listContent := s[bracketStart:p]
	p++ // skip ']'
	// Must be at end
	if ccSkipSpaces(s, p) != len(s) {
		return false, nil, "", nil
	}
	values, err := parseContractLiteralList(listContent)
	if err != nil {
		return false, nil, "", err
	}
	return true, values, property, nil
}

// parseRelationshipPropertyInExpression delegates to parsePropertyInExpression
// with swapped return order for callers that expect (matched, property, values, err).
func parseRelationshipPropertyInExpression(expr string) (bool, string, []interface{}, error) {
	matched, values, property, err := parsePropertyInExpression(expr)
	if !matched || err != nil {
		return matched, property, values, err
	}
	return true, property, values, nil
}

// parseCountPatternExpression: COUNT { [MATCH] pattern } comparator threshold
func parseCountPatternExpression(expr string) (bool, contractPattern, string, int, error) {
	s := strings.TrimSpace(expr)
	p := 0

	// COUNT
	if end := ccMatchKeywordAt(s, p, "COUNT"); end == -1 {
		return false, contractPattern{}, "", 0, nil
	} else {
		p = end
	}
	// {
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '{') == -1 {
		return false, contractPattern{}, "", 0, nil
	}
	p++
	// Find matching }
	braceStart := p
	depth := 1
	for p < len(s) && depth > 0 {
		switch s[p] {
		case '{':
			depth++
		case '}':
			depth--
		}
		if depth > 0 {
			p++
		}
	}
	if depth != 0 {
		return false, contractPattern{}, "", 0, nil
	}
	inner := strings.TrimSpace(s[braceStart:p])
	p++ // skip '}'

	// Optional MATCH keyword inside braces
	if end := ccMatchKeywordAt(inner, 0, "MATCH"); end != -1 {
		inner = strings.TrimSpace(inner[end:])
	}

	pattern, err := parseConstraintPattern(inner)
	if err != nil {
		return false, contractPattern{}, "", 0, err
	}

	// comparator
	p = ccSkipSpaces(s, p)
	comparator, next := ccScanComparator(s, p)
	if comparator == "" {
		return false, contractPattern{}, "", 0, nil
	}
	p = next

	// threshold (integer, possibly negative)
	p = ccSkipSpaces(s, p)
	numStart := p
	if p < len(s) && s[p] == '-' {
		p++
	}
	for p < len(s) && s[p] >= '0' && s[p] <= '9' {
		p++
	}
	if p == numStart {
		return false, contractPattern{}, "", 0, nil
	}
	threshold, err := strconv.Atoi(s[numStart:p])
	if err != nil {
		return false, contractPattern{}, "", 0, err
	}
	if ccSkipSpaces(s, p) != len(s) {
		return false, contractPattern{}, "", 0, nil
	}
	return true, pattern, comparator, threshold, nil
}

// parseNotExistsPatternExpression: NOT EXISTS { [MATCH] pattern }
func parseNotExistsPatternExpression(expr string) (bool, contractPattern, error) {
	s := strings.TrimSpace(expr)
	p := 0

	// NOT
	if end := ccMatchKeywordAt(s, p, "NOT"); end == -1 {
		return false, contractPattern{}, nil
	} else {
		p = end
	}
	// EXISTS
	p = ccSkipSpaces(s, p)
	if end := ccMatchKeywordAt(s, p, "EXISTS"); end == -1 {
		return false, contractPattern{}, nil
	} else {
		p = end
	}
	// {
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '{') == -1 {
		return false, contractPattern{}, nil
	}
	p++
	// Find matching }
	braceStart := p
	depth := 1
	for p < len(s) && depth > 0 {
		switch s[p] {
		case '{':
			depth++
		case '}':
			depth--
		}
		if depth > 0 {
			p++
		}
	}
	if depth != 0 {
		return false, contractPattern{}, nil
	}
	inner := strings.TrimSpace(s[braceStart:p])
	p++ // skip '}'

	if ccSkipSpaces(s, p) != len(s) {
		return false, contractPattern{}, nil
	}

	// Optional MATCH keyword
	if end := ccMatchKeywordAt(inner, 0, "MATCH"); end != -1 {
		inner = strings.TrimSpace(inner[end:])
	}

	pattern, err := parseConstraintPattern(inner)
	if err != nil {
		return false, contractPattern{}, err
	}
	return true, pattern, nil
}

// parseConstraintPattern: (node)-[:TYPE]->(node) or (node)<-[:TYPE]-(node)
// Supports n-arity labels on both source and target: (n:A:B)-[:R]->(:X:Y:Z)
func parseConstraintPattern(raw string) (contractPattern, error) {
	s := strings.TrimSpace(raw)
	p := 0

	// Source node: ( ... )
	if ccExpectByte(s, p, '(') == -1 {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}
	p++
	_, p, ok := ccScanNodeContents(s, p)
	if !ok {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}

	// Arrow start: - or <-
	p = ccSkipSpaces(s, p)
	direction := ""
	if p+1 < len(s) && s[p] == '<' && s[p+1] == '-' {
		direction = "INCOMING"
		p += 2
	} else if p < len(s) && s[p] == '-' {
		p++
	} else {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}

	// Relationship: [ :TYPE ]
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '[') == -1 {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}
	p++
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, ':') == -1 {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}
	p++
	p = ccSkipSpaces(s, p)
	relType, next := ccScanIdent(s, p)
	if relType == "" {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}
	p = next
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, ']') == -1 {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}
	p++

	// Arrow end: -> or -
	p = ccSkipSpaces(s, p)
	if direction == "" {
		// Must be outgoing: ->
		if p+1 < len(s) && s[p] == '-' && s[p+1] == '>' {
			direction = "OUTGOING"
			p += 2
		} else {
			return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
		}
	} else {
		// INCOMING: expect -
		if ccExpectByte(s, p, '-') == -1 {
			return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
		}
		p++
	}

	// Target node: ( ... )
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '(') == -1 {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}
	p++
	targetLabels, p, ok := ccScanNodeContents(s, p)
	if !ok {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}

	if ccSkipSpaces(s, p) != len(s) {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}

	return contractPattern{
		Direction:    direction,
		RelationType: relType,
		TargetLabels: targetLabels,
	}, nil
}

// isDistinctEndpointsExpression: startNode(r) <> endNode(r)
func isDistinctEndpointsExpression(expr string) bool {
	s := strings.TrimSpace(expr)
	p := 0

	// startNode
	if end := ccMatchKeywordAt(s, p, "startNode"); end == -1 {
		return false
	} else {
		p = end
	}
	// ( ident )
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '(') == -1 {
		return false
	}
	p++
	p = ccSkipSpaces(s, p)
	if _, next := ccScanIdent(s, p); next == p {
		return false
	} else {
		p = next
	}
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, ')') == -1 {
		return false
	}
	p++
	// <>
	p = ccSkipSpaces(s, p)
	comp, next := ccScanComparator(s, p)
	if comp != "<>" {
		return false
	}
	p = next
	// endNode
	p = ccSkipSpaces(s, p)
	if end := ccMatchKeywordAt(s, p, "endNode"); end == -1 {
		return false
	} else {
		p = end
	}
	// ( ident )
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '(') == -1 {
		return false
	}
	p++
	p = ccSkipSpaces(s, p)
	if _, next := ccScanIdent(s, p); next == p {
		return false
	} else {
		p = next
	}
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, ')') == -1 {
		return false
	}
	p++
	return ccSkipSpaces(s, p) == len(s)
}

// parseEndpointPropertyEqualityExpression: startNode(r).prop = endNode(r).prop
func parseEndpointPropertyEqualityExpression(expr string) (bool, string, string) {
	s := strings.TrimSpace(expr)
	p := 0

	// startNode ( ident ) . property
	if end := ccMatchKeywordAt(s, p, "startNode"); end == -1 {
		return false, "", ""
	} else {
		p = end
	}
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '(') == -1 {
		return false, "", ""
	}
	p++
	p = ccSkipSpaces(s, p)
	if _, next := ccScanIdent(s, p); next == p {
		return false, "", ""
	} else {
		p = next
	}
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, ')') == -1 {
		return false, "", ""
	}
	p++
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '.') == -1 {
		return false, "", ""
	}
	p++
	p = ccSkipSpaces(s, p)
	leftProp, next := ccScanIdent(s, p)
	if leftProp == "" {
		return false, "", ""
	}
	p = next

	// =
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '=') == -1 {
		return false, "", ""
	}
	p++

	// endNode ( ident ) . property
	p = ccSkipSpaces(s, p)
	if end := ccMatchKeywordAt(s, p, "endNode"); end == -1 {
		return false, "", ""
	} else {
		p = end
	}
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '(') == -1 {
		return false, "", ""
	}
	p++
	p = ccSkipSpaces(s, p)
	if _, next := ccScanIdent(s, p); next == p {
		return false, "", ""
	} else {
		p = next
	}
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, ')') == -1 {
		return false, "", ""
	}
	p++
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '.') == -1 {
		return false, "", ""
	}
	p++
	p = ccSkipSpaces(s, p)
	rightProp, next := ccScanIdent(s, p)
	if rightProp == "" {
		return false, "", ""
	}
	p = next

	if ccSkipSpaces(s, p) != len(s) {
		return false, "", ""
	}
	return true, leftProp, rightProp
}

// parseRelationshipPropertyComparisonExpression: ident.property comparator value
// Excludes IN expressions (those are handled by parsePropertyInExpression).
func parseRelationshipPropertyComparisonExpression(expr string) (bool, string, string, interface{}, error) {
	s := strings.TrimSpace(expr)
	p := 0

	// ident (variable)
	_, next := ccScanIdent(s, p)
	if next == p {
		return false, "", "", nil, nil
	}
	p = next
	// .
	p = ccSkipSpaces(s, p)
	if ccExpectByte(s, p, '.') == -1 {
		return false, "", "", nil, nil
	}
	p++
	p = ccSkipSpaces(s, p)
	// property
	property, next := ccScanIdent(s, p)
	if property == "" {
		return false, "", "", nil, nil
	}
	p = next
	// comparator
	p = ccSkipSpaces(s, p)
	comparator, next := ccScanComparator(s, p)
	if comparator == "" {
		return false, "", "", nil, nil
	}
	p = next

	// Guard: if it's an IN expression, don't match as comparison
	if _, _, prop, err := parsePropertyInExpression(expr); err == nil && prop != "" {
		return false, "", "", nil, nil
	}

	// remaining = value
	rest := strings.TrimSpace(s[p:])
	if rest == "" {
		return false, "", "", nil, nil
	}
	value, err := parseContractLiteral(rest)
	if err != nil {
		return false, "", "", nil, err
	}
	return true, property, comparator, value, nil
}

func parseContractLiteralList(raw string) ([]interface{}, error) {
	parts := splitTopLevelCSV(raw)
	values := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		value, err := parseContractLiteral(part)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func parseContractLiteral(raw string) (interface{}, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 {
		if (raw[0] == '\'' && raw[len(raw)-1] == '\'') || (raw[0] == '"' && raw[len(raw)-1] == '"') {
			return raw[1 : len(raw)-1], nil
		}
	}
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, nil
	}
	if strings.EqualFold(raw, "true") {
		return true, nil
	}
	if strings.EqualFold(raw, "false") {
		return false, nil
	}
	if strings.EqualFold(raw, "null") {
		return nil, nil
	}
	return nil, localizedError(localization.StorageSchemaUnsupportedConstraintLiteral(raw), nil)
}

func splitTopLevelCSV(raw string) []string {
	parts := make([]string, 0)
	var builder strings.Builder
	depth := 0
	quote := rune(0)
	for _, ch := range raw {
		switch {
		case quote != 0:
			builder.WriteRune(ch)
			if ch == quote {
				quote = 0
			}
		case ch == '\'' || ch == '"':
			quote = ch
			builder.WriteRune(ch)
		case ch == '[' || ch == '(' || ch == '{':
			depth++
			builder.WriteRune(ch)
		case ch == ']' || ch == ')' || ch == '}':
			if depth > 0 {
				depth--
			}
			builder.WriteRune(ch)
		case ch == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(builder.String()))
			builder.Reset()
		default:
			builder.WriteRune(ch)
		}
	}
	if builder.Len() > 0 {
		parts = append(parts, strings.TrimSpace(builder.String()))
	}
	return parts
}

func evaluatePropertyInExpression(actual interface{}, values []interface{}) bool {
	if actual == nil {
		return false
	}
	for _, value := range values {
		if compareValues(actual, value) {
			return true
		}
	}
	return false
}

func compareConstraintExpressionValue(actual interface{}, comparator string, expected interface{}) bool {
	switch comparator {
	case "=":
		return compareValues(actual, expected)
	case "<>", "!=":
		return !compareValues(actual, expected)
	case ">":
		return compareGreaterConstraint(actual, expected)
	case ">=":
		return compareGreaterConstraint(actual, expected) || compareValues(actual, expected)
	case "<":
		return compareLessConstraint(actual, expected)
	case "<=":
		return compareLessConstraint(actual, expected) || compareValues(actual, expected)
	default:
		return false
	}
}

func compareGreaterConstraint(actual interface{}, expected interface{}) bool {
	actualFloat, actualOK := convert.ToFloat64(actual)
	expectedFloat, expectedOK := convert.ToFloat64(expected)
	if actualOK && expectedOK {
		return actualFloat > expectedFloat
	}
	return fmt.Sprintf("%v", actual) > fmt.Sprintf("%v", expected)
}

func compareLessConstraint(actual interface{}, expected interface{}) bool {
	actualFloat, actualOK := convert.ToFloat64(actual)
	expectedFloat, expectedOK := convert.ToFloat64(expected)
	if actualOK && expectedOK {
		return actualFloat < expectedFloat
	}
	return fmt.Sprintf("%v", actual) < fmt.Sprintf("%v", expected)
}

func compareInt(actual int, comparator string, expected int) bool {
	switch comparator {
	case "=":
		return actual == expected
	case "<>", "!=":
		return actual != expected
	case ">":
		return actual > expected
	case ">=":
		return actual >= expected
	case "<":
		return actual < expected
	case "<=":
		return actual <= expected
	default:
		return false
	}
}

func countMatchingPatternEdgesEngine(engine Engine, node *Node, pattern contractPattern) (int, error) {
	var edges []*Edge
	var err error
	if pattern.Direction == "INCOMING" {
		edges, err = engine.GetIncomingEdges(node.ID)
	} else {
		edges, err = engine.GetOutgoingEdges(node.ID)
	}
	if err != nil {
		return 0, err
	}
	count := 0
	for _, edge := range edges {
		if edge.Type != pattern.RelationType {
			continue
		}
		if len(pattern.TargetLabels) > 0 {
			otherID := edge.EndNode
			if pattern.Direction == "INCOMING" {
				otherID = edge.StartNode
			}
			otherNode, err := engine.GetNode(otherID)
			if err != nil {
				return 0, err
			}
			if otherNode == nil || !hasAllLabels(otherNode.Labels, pattern.TargetLabels) {
				continue
			}
		}
		count++
	}
	return count, nil
}

func (tx *BadgerTransaction) validateConstraintContracts() error {
	// Early-out: if NO namespace referenced by the transaction has any
	// constraint contracts declared, there is nothing to validate and we
	// can skip the entire expensive adjacency-walk. Previously we always
	// called currentAdjacentEdgesLocked(node.ID) for every affected node,
	// which does a per-node Badger read even when no contract needed the
	// edges. That accounted for the CPU time stuck in skl.findNear during
	// bulk seed commits — see the bulk-write profile in RESEARCH notes.
	//
	// We check each namespace that the transaction touches (derived from
	// node/edge ID prefixes) and short-circuit when all of them report
	// zero contracts. Constraint contracts are rare in practice —
	// typical workloads declare only unique constraints + range indexes,
	// not boolean predicates.
	if !tx.transactionTouchesConstraintContracts() {
		return nil
	}

	affectedNodes := make(map[NodeID]struct{})
	affectedEdges := make(map[EdgeID]struct{})

	for nodeID := range tx.pendingNodes {
		affectedNodes[nodeID] = struct{}{}
	}
	for edgeID, edge := range tx.pendingEdges {
		affectedEdges[edgeID] = struct{}{}
		affectedNodes[edge.StartNode] = struct{}{}
		affectedNodes[edge.EndNode] = struct{}{}
	}
	for _, op := range tx.operations {
		if op.OldEdge != nil {
			affectedNodes[op.OldEdge.StartNode] = struct{}{}
			affectedNodes[op.OldEdge.EndNode] = struct{}{}
		}
	}

	for nodeID := range affectedNodes {
		node, err := tx.currentNodeLocked(nodeID)
		if err != nil {
			return err
		}
		if node == nil {
			continue
		}
		if err := tx.validateConstraintContractsForNodeLocked(node); err != nil {
			return err
		}
		adjacent, err := tx.currentAdjacentEdgesLocked(node.ID)
		if err != nil {
			return err
		}
		for _, edge := range adjacent {
			if err := tx.validateConstraintContractsForEdgeLocked(edge); err != nil {
				return err
			}
		}
	}
	for edgeID := range affectedEdges {
		edge, err := tx.currentEdgeLocked(edgeID)
		if err != nil {
			return err
		}
		if edge == nil {
			continue
		}
		if err := tx.validateConstraintContractsForEdgeLocked(edge); err != nil {
			return err
		}
	}

	return nil
}

// transactionTouchesConstraintContracts reports whether any namespace
// referenced by the pending transaction has at least one constraint
// contract declared. When false, validateConstraintContracts has no work
// to do and can short-circuit.
func (tx *BadgerTransaction) transactionTouchesConstraintContracts() bool {
	namespaces := make(map[string]struct{}, 2)
	for id := range tx.pendingNodes {
		if ns, ok := constraintContractNamespaceFromID(string(id)); ok {
			namespaces[ns] = struct{}{}
		}
	}
	for _, edge := range tx.pendingEdges {
		if ns, ok := constraintContractNamespaceFromID(string(edge.ID)); ok {
			namespaces[ns] = struct{}{}
		}
	}
	for _, op := range tx.operations {
		if op.OldEdge != nil {
			if ns, ok := constraintContractNamespaceFromID(string(op.OldEdge.ID)); ok {
				namespaces[ns] = struct{}{}
			}
		}
	}
	for ns := range namespaces {
		schema := tx.engine.GetSchemaForNamespace(ns)
		if schema == nil {
			continue
		}
		if schema.HasAnyConstraintContract() {
			return true
		}
	}
	return false
}

// constraintContractNamespaceFromID returns the namespace encoded in a
// prefixed ID (e.g. "nornic:abc" → "nornic"). Returns false when the ID
// has no recognisable prefix, matching constraintContractNamespaceForNode's
// behaviour for that edge case.
func constraintContractNamespaceFromID(id string) (string, bool) {
	ns, _, ok := ParseDatabasePrefix(id)
	return ns, ok
}

func (tx *BadgerTransaction) validateConstraintContractsForNodeLocked(node *Node) error {
	dbName, ok := constraintContractNamespaceForNode(node)
	if !ok {
		return nil
	}
	schema := tx.engine.GetSchemaForNamespace(dbName)
	if schema == nil {
		return nil
	}
	contractsByName := make(map[string]ConstraintContract)
	for _, label := range node.Labels {
		for _, contract := range schema.GetConstraintContractsForTarget(ConstraintEntityNode, label) {
			contractsByName[contract.Name] = contract
		}
	}
	for _, contract := range contractsByName {
		for _, entry := range contract.Entries {
			if entry.Kind != ConstraintContractKindBooleanNode {
				continue
			}
			ok, err := tx.evaluateNodeConstraintContractExpressionLocked(node, entry.Expression)
			if err != nil {
				return localizedError(localization.StorageSchemaConstraintContractInvalid(contract.Name, entry.Expression, err), err)
			}
			if !ok {
				return localizedError(localization.StorageSchemaConstraintContractViolated(contract.Name, entry.Expression), nil)
			}
		}
	}
	return nil
}

func (tx *BadgerTransaction) validateConstraintContractsForEdgeLocked(edge *Edge) error {
	dbName, ok := constraintContractNamespaceForEdge(edge)
	if !ok {
		return nil
	}
	schema := tx.engine.GetSchemaForNamespace(dbName)
	if schema == nil {
		return nil
	}
	for _, contract := range schema.GetConstraintContractsForTarget(ConstraintEntityRelationship, edge.Type) {
		for _, entry := range contract.Entries {
			if entry.Kind != ConstraintContractKindBooleanRelationship {
				continue
			}
			ok, err := tx.evaluateRelationshipConstraintContractExpressionLocked(edge, entry.Expression)
			if err != nil {
				return localizedError(localization.StorageSchemaConstraintContractInvalid(contract.Name, entry.Expression, err), err)
			}
			if !ok {
				return localizedError(localization.StorageSchemaConstraintContractViolated(contract.Name, entry.Expression), nil)
			}
		}
	}
	return nil
}

func (tx *BadgerTransaction) evaluateNodeConstraintContractExpressionLocked(node *Node, expr string) (bool, error) {
	if matched, values, property, err := parsePropertyInExpression(expr); err != nil {
		return false, err
	} else if matched {
		return evaluatePropertyInExpression(node.Properties[property], values), nil
	}
	if matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression(expr); err != nil {
		return false, err
	} else if matched {
		return compareConstraintExpressionValue(node.Properties[property], comparator, value), nil
	}
	if matched, pattern, comparator, threshold, err := parseCountPatternExpression(expr); err != nil {
		return false, err
	} else if matched {
		count, err := tx.countMatchingPatternEdgesLocked(node, pattern)
		if err != nil {
			return false, err
		}
		return compareInt(count, comparator, threshold), nil
	}
	if matched, pattern, err := parseNotExistsPatternExpression(expr); err != nil {
		return false, err
	} else if matched {
		count, err := tx.countMatchingPatternEdgesLocked(node, pattern)
		if err != nil {
			return false, err
		}
		return count == 0, nil
	}
	return false, localizedError(localization.StorageSchemaUnsupportedNodePredicate(), nil)
}

func (tx *BadgerTransaction) evaluateRelationshipConstraintContractExpressionLocked(edge *Edge, expr string) (bool, error) {
	if matched, property, values, err := parseRelationshipPropertyInExpression(expr); err != nil {
		return false, err
	} else if matched {
		return evaluatePropertyInExpression(edge.Properties[property], values), nil
	}
	if isDistinctEndpointsExpression(expr) {
		return edge.StartNode != edge.EndNode, nil
	}
	if matched, leftProp, rightProp := parseEndpointPropertyEqualityExpression(expr); matched {
		startNode, err := tx.currentNodeLocked(edge.StartNode)
		if err != nil {
			return false, err
		}
		endNode, err := tx.currentNodeLocked(edge.EndNode)
		if err != nil {
			return false, err
		}
		if startNode == nil || endNode == nil {
			return false, localizedError(localization.StorageSchemaMissingRelationshipEndpoint(), nil)
		}
		return compareValues(startNode.Properties[leftProp], endNode.Properties[rightProp]), nil
	}
	if matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression(expr); err != nil {
		return false, err
	} else if matched {
		return compareConstraintExpressionValue(edge.Properties[property], comparator, value), nil
	}
	return false, localizedError(localization.StorageSchemaUnsupportedRelationshipPredicate(), nil)
}

func (tx *BadgerTransaction) countMatchingPatternEdgesLocked(node *Node, pattern contractPattern) (int, error) {
	var edges []*Edge
	var err error
	if pattern.Direction == "INCOMING" {
		edges, err = tx.currentIncomingEdgesLocked(node.ID)
	} else {
		edges, err = tx.currentOutgoingEdgesLocked(node.ID)
	}
	if err != nil {
		return 0, err
	}
	count := 0
	for _, edge := range edges {
		if edge.Type != pattern.RelationType {
			continue
		}
		if len(pattern.TargetLabels) > 0 {
			otherID := edge.EndNode
			if pattern.Direction == "INCOMING" {
				otherID = edge.StartNode
			}
			otherNode, err := tx.currentNodeLocked(otherID)
			if err != nil {
				return 0, err
			}
			if otherNode == nil || !hasAllLabels(otherNode.Labels, pattern.TargetLabels) {
				continue
			}
		}
		count++
	}
	return count, nil
}

func (tx *BadgerTransaction) currentNodeLocked(nodeID NodeID) (*Node, error) {
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil, nil
	}
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		return copyNode(pending), nil
	}
	node, err := tx.getCommittedNodeLocked(nodeID)
	if err == ErrNotFound {
		return nil, nil
	}
	return node, err
}

func (tx *BadgerTransaction) currentEdgeLocked(edgeID EdgeID) (*Edge, error) {
	if _, deleted := tx.deletedEdges[edgeID]; deleted {
		return nil, nil
	}
	if pending, exists := tx.pendingEdges[edgeID]; exists {
		return copyEdge(pending), nil
	}
	edge, err := tx.getCommittedEdgeLocked(edgeID)
	if err == ErrNotFound {
		return nil, nil
	}
	return edge, err
}

func (tx *BadgerTransaction) currentOutgoingEdgesLocked(nodeID NodeID) ([]*Edge, error) {
	committed, err := tx.engine.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	edges := make(map[EdgeID]*Edge)
	for _, edge := range committed {
		if _, deleted := tx.deletedEdges[edge.ID]; deleted {
			continue
		}
		if pending, exists := tx.pendingEdges[edge.ID]; exists {
			if pending.StartNode == nodeID {
				edges[edge.ID] = copyEdge(pending)
			}
			continue
		}
		if edge.StartNode == nodeID {
			edges[edge.ID] = edge
		}
	}
	for edgeID, edge := range tx.pendingEdges {
		if edge.StartNode == nodeID {
			edges[edgeID] = copyEdge(edge)
		}
	}
	result := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		result = append(result, edge)
	}
	return result, nil
}

func constraintContractNamespaceForNode(node *Node) (string, bool) {
	if node == nil {
		return "", false
	}
	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	return dbName, ok
}

func constraintContractNamespaceForEdge(edge *Edge) (string, bool) {
	if edge == nil {
		return "", false
	}
	if dbName, _, ok := ParseDatabasePrefix(string(edge.ID)); ok {
		return dbName, true
	}
	if dbName, _, ok := ParseDatabasePrefix(string(edge.StartNode)); ok {
		return dbName, true
	}
	if dbName, _, ok := ParseDatabasePrefix(string(edge.EndNode)); ok {
		return dbName, true
	}
	return "", false
}

func (tx *BadgerTransaction) currentIncomingEdgesLocked(nodeID NodeID) ([]*Edge, error) {
	committed, err := tx.engine.GetIncomingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	edges := make(map[EdgeID]*Edge)
	for _, edge := range committed {
		if _, deleted := tx.deletedEdges[edge.ID]; deleted {
			continue
		}
		if pending, exists := tx.pendingEdges[edge.ID]; exists {
			if pending.EndNode == nodeID {
				edges[edge.ID] = copyEdge(pending)
			}
			continue
		}
		if edge.EndNode == nodeID {
			edges[edge.ID] = edge
		}
	}
	for edgeID, edge := range tx.pendingEdges {
		if edge.EndNode == nodeID {
			edges[edgeID] = copyEdge(edge)
		}
	}
	result := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		result = append(result, edge)
	}
	return result, nil
}

func (tx *BadgerTransaction) currentAdjacentEdgesLocked(nodeID NodeID) ([]*Edge, error) {
	outgoing, err := tx.currentOutgoingEdgesLocked(nodeID)
	if err != nil {
		return nil, err
	}
	incoming, err := tx.currentIncomingEdgesLocked(nodeID)
	if err != nil {
		return nil, err
	}
	edges := make(map[EdgeID]*Edge)
	for _, edge := range outgoing {
		edges[edge.ID] = edge
	}
	for _, edge := range incoming {
		edges[edge.ID] = edge
	}
	result := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		result = append(result, edge)
	}
	return result, nil
}
