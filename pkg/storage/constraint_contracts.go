package storage

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"

	"sort"
	"strconv"
	"strings"
	"sync/atomic"

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
	return cyphertext.IsIdentifierStart(b)
}

// ccIsIdentByte returns true for [A-Za-z0-9_].
func ccIsIdentByte(b byte) bool {
	return cyphertext.IsIdentifierByte(b)
}

// ccScanIdent reads an identifier at position i. Returns ("", i) if none.
func ccScanIdent(text string, start int) (string, int) {
	name, end, _ := cyphertext.ScanIdentifier(text, start)
	return name, end
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

const (
	ConstraintContractKindPrimitiveNode         = "primitive-node"
	ConstraintContractKindPrimitiveRelationship = "primitive-relationship"
	ConstraintContractKindBooleanNode           = "boolean-node"
	ConstraintContractKindBooleanRelationship   = "boolean-relationship"
)

// ConstraintContractEntryName is the name of the primitive constraint compiled
// from the entry at index idx (0-based) of a contract.
func ConstraintContractEntryName(contractName string, idx int) string {
	return fmt.Sprintf("%s__entry_%02d", contractName, idx+1)
}

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

// constraintContractRevision is one immutable revision of a contract: its own
// copy of the description and the plan compiled from each of its entries.
// Nothing changes it after it is built. The schema stores revisions; outside
// storage they are reachable only through CompiledConstraintContract, so the
// description a revision evaluates is always the one it compiled, and schema
// getters and exports return copies of that description.
type constraintContractRevision struct {
	contract ConstraintContract
	plans    []constraintContractEntryPlan // one per entry
}

// CompiledConstraintContract is an opaque handle to an admitted revision. It
// is a value with no exported fields: assigning to a handle changes only the
// caller's copy, never a revision or the schema that installed it. The zero
// handle holds no contract.
type CompiledConstraintContract struct {
	revision *constraintContractRevision
}

// constraintContractEntryPlan is a boolean entry's compiled predicate, or the
// error compiling it, which every evaluation of the entry reports.
type constraintContractEntryPlan struct {
	node         contractNodePredicate
	relationship contractRelationshipPredicate
	err          error
}

// constraintContractCompileHook lets storage tests count predicate
// compilations, so validation cannot regress to compiling per entity.
var constraintContractCompileHook atomic.Pointer[func()]

// compileConstraintContractEntryPlan compiles one entry's predicate. Only a
// compiled revision and admission's validation call it; ok is false for a
// non-boolean entry.
func compileConstraintContractEntryPlan(entry ConstraintContractEntry) (plan constraintContractEntryPlan, ok bool) {
	switch entry.Kind {
	case ConstraintContractKindBooleanNode:
		plan.node, plan.err = compileNodeConstraintContractExpression(entry.Expression)
		return plan, true
	case ConstraintContractKindBooleanRelationship:
		plan.relationship, plan.err = compileRelationshipConstraintContractExpression(entry.Expression)
		return plan, true
	}
	return plan, false
}

// compileConstraintContractRevision is the one producer of contract plans. It
// copies contract, so later edits to the caller's value cannot reach the
// revision. An entry that does not compile keeps its error in its plan.
func compileConstraintContractRevision(contract ConstraintContract) *constraintContractRevision {
	revision := &constraintContractRevision{contract: cloneConstraintContract(contract)}
	revision.plans = make([]constraintContractEntryPlan, len(revision.contract.Entries))
	for index, entry := range revision.contract.Entries {
		revision.plans[index], _ = compileConstraintContractEntryPlan(entry)
	}
	return revision
}

// firstError is the error admission reports for the revision: its first entry
// that does not compile.
func (revision *constraintContractRevision) firstError() error {
	for index, plan := range revision.plans {
		if plan.err != nil {
			return constraintContractInvalidError(revision.contract, revision.contract.Entries[index], plan.err)
		}
	}
	return nil
}

// CompileConstraintContract compiles contract once for admission, its scan of
// existing data and the schema revision that stores it.
func CompileConstraintContract(contract ConstraintContract) (CompiledConstraintContract, error) {
	revision := compileConstraintContractRevision(contract)
	if err := revision.firstError(); err != nil {
		return CompiledConstraintContract{}, err
	}
	return CompiledConstraintContract{revision: revision}, nil
}

// Contract returns a copy of the description the handle's revision compiled;
// the zero handle returns the zero contract.
func (compiled CompiledConstraintContract) Contract() ConstraintContract {
	if compiled.revision == nil {
		return ConstraintContract{}
	}
	return compiled.revision.description()
}

// admittedRevision returns the handle's revision; the zero handle holds no
// contract, so it names no supported target.
func (compiled CompiledConstraintContract) admittedRevision() (*constraintContractRevision, error) {
	if compiled.revision == nil {
		return nil, localizedError(localization.StorageSchemaUnsupportedContractTargetEntityType(""), nil)
	}
	return compiled.revision, nil
}

// description returns a copy of the description this revision compiled.
func (revision *constraintContractRevision) description() ConstraintContract {
	return cloneConstraintContract(revision.contract)
}

func constraintContractInvalidError(contract ConstraintContract, entry ConstraintContractEntry, err error) error {
	return localizedError(localization.StorageSchemaConstraintContractInvalid(contract.Name, entry.Expression, err), err)
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
	for _, revision := range sm.constraintContracts {
		contracts = append(contracts, revision.description())
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
	contracts := make([]ConstraintContract, 0)
	for _, revision := range sm.constraintContractRevisionsForTarget(entityType, labelOrType) {
		contracts = append(contracts, revision.description())
	}
	return contracts
}

// constraintContractRevisionsForTarget returns the stored revisions for a
// target, in name order. Revisions are immutable and shared, not copied.
func (sm *SchemaManager) constraintContractRevisionsForTarget(entityType ConstraintEntityType, labelOrType string) []*constraintContractRevision {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	revisions := make([]*constraintContractRevision, 0)
	for _, revision := range sm.constraintContracts {
		if revision.contract.TargetEntityType != string(entityType) || revision.contract.TargetLabelOrType != labelOrType {
			continue
		}
		revisions = append(revisions, revision)
	}
	sort.Slice(revisions, func(i, j int) bool {
		return revisions[i].contract.Name < revisions[j].contract.Name
	})
	return revisions
}

// AddConstraintContractBundle stores contract as a new compiled revision. An
// entry that does not compile is stored and fails where it is evaluated, as
// before; CREATE CONSTRAINT admits contracts through CompileConstraintContract.
func (sm *SchemaManager) AddConstraintContractBundle(contract ConstraintContract, compiledConstraints []Constraint, compiledTypes []PropertyTypeConstraint, ifNotExists bool) error {
	return sm.addConstraintContractRevision(compileConstraintContractRevision(contract), compiledConstraints, compiledTypes, ifNotExists)
}

// AddCompiledConstraintContractBundle installs the revision an admitted handle
// refers to. The schema keeps the revision, never the caller's handle.
func (sm *SchemaManager) AddCompiledConstraintContractBundle(compiled CompiledConstraintContract, compiledConstraints []Constraint, compiledTypes []PropertyTypeConstraint, ifNotExists bool) error {
	revision, err := compiled.admittedRevision()
	if err != nil {
		return err
	}
	return sm.addConstraintContractRevision(revision, compiledConstraints, compiledTypes, ifNotExists)
}

// CompiledConstraintContracts returns handles to the installed revisions, in
// name order, for checks that reuse them (such as import validation).
func (sm *SchemaManager) CompiledConstraintContracts() []CompiledConstraintContract {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	handles := make([]CompiledConstraintContract, 0, len(sm.constraintContracts))
	for _, revision := range sm.constraintContracts {
		handles = append(handles, CompiledConstraintContract{revision: revision})
	}
	sort.Slice(handles, func(i, j int) bool {
		return handles[i].revision.contract.Name < handles[j].revision.contract.Name
	})
	return handles
}

func (sm *SchemaManager) addConstraintContractRevision(revision *constraintContractRevision, compiledConstraints []Constraint, compiledTypes []PropertyTypeConstraint, ifNotExists bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	contract := revision.contract
	snapshot := sm.exportDefinitionLocked()

	if existing, exists := sm.constraintContracts[contract.Name]; exists {
		if ifNotExists && constraintContractEqual(existing.contract, contract) {
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
		if _, err := sm.addConstraintLocked(compiled, false); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}
	for _, compiled := range compiledTypes {
		if _, err := sm.addPropertyTypeConstraintValueLocked(compiled, false); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}

	sm.constraintContracts[contract.Name] = revision

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
	compiled, err := CompileConstraintContract(contract)
	if err != nil {
		return err
	}
	return ValidateCompiledConstraintContractOnCreationForEngine(engine, compiled)
}

// ValidateCompiledConstraintContractOnCreationForEngine checks existing data
// against an admitted revision: one plan per entry serves the whole scan.
func ValidateCompiledConstraintContractOnCreationForEngine(engine Engine, compiled CompiledConstraintContract) error {
	revision, err := compiled.admittedRevision()
	if err != nil {
		return err
	}
	contract := revision.contract
	entityType := ConstraintEntityType(contract.TargetEntityType)
	switch entityType {
	case ConstraintEntityNode:
		nodes, err := engine.GetNodesByLabel(contract.TargetLabelOrType)
		if err != nil {
			return localizedError(localization.StorageSchemaScanNodesFailed(err), err)
		}
		for _, node := range nodes {
			if err := validateConstraintContractForNodeEngine(engine, revision, node); err != nil {
				return err
			}
		}
	case ConstraintEntityRelationship:
		// Stream the relationship type (scoped to the engine's database)
		// instead of loading every edge of the type into memory.
		var violation error
		err := StreamEdgesByType(context.Background(), engine, contract.TargetLabelOrType, func(edge *Edge) error {
			if edge == nil {
				return nil
			}
			if err := validateConstraintContractForEdgeEngine(engine, revision, edge); err != nil {
				violation = err
				return err
			}
			return nil
		})
		if violation != nil {
			return violation
		}
		if err != nil {
			return localizedError(localization.StorageSchemaScanRelationshipsFailed(err), err)
		}
	default:
		return localizedError(localization.StorageSchemaUnsupportedContractTargetEntityType(contract.TargetEntityType), nil)
	}
	return nil
}

func validateConstraintContractForNodeEngine(engine Engine, revision *constraintContractRevision, node *Node) error {
	contract := revision.contract
	for index := range contract.Entries {
		entry := &contract.Entries[index]
		if entry.Kind != ConstraintContractKindBooleanNode {
			continue
		}
		ok, err := evaluateNodeConstraintContractPlan(engineConstraintView{engine}, node, &revision.plans[index])
		if err != nil {
			return constraintContractInvalidError(contract, *entry, err)
		}
		if !ok {
			return localizedError(localization.StorageSchemaConstraintContractViolated(contract.Name, entry.Expression), nil)
		}
	}
	return nil
}

func validateConstraintContractForEdgeEngine(engine Engine, revision *constraintContractRevision, edge *Edge) error {
	contract := revision.contract
	for index := range contract.Entries {
		entry := &contract.Entries[index]
		if entry.Kind != ConstraintContractKindBooleanRelationship {
			continue
		}
		ok, err := evaluateRelationshipConstraintContractPlan(engineConstraintView{engine}, edge, &revision.plans[index])
		if err != nil {
			return constraintContractInvalidError(contract, *entry, err)
		}
		if !ok {
			return localizedError(localization.StorageSchemaConstraintContractViolated(contract.Name, entry.Expression), nil)
		}
	}
	return nil
}

// constraintGraphView is the read surface the constraint-contract evaluators
// need: adjacency and node lookup. Both an Engine and a locked Badger
// transaction provide it, so the evaluator logic is written once and the two
// call paths cannot drift apart.
type constraintGraphView interface {
	OutgoingEdges(id NodeID) ([]*Edge, error)
	IncomingEdges(id NodeID) ([]*Edge, error)
	Node(id NodeID) (*Node, error)
}

type engineConstraintView struct{ engine Engine }

func (v engineConstraintView) OutgoingEdges(id NodeID) ([]*Edge, error) {
	return v.engine.GetOutgoingEdges(id)
}
func (v engineConstraintView) IncomingEdges(id NodeID) ([]*Edge, error) {
	return v.engine.GetIncomingEdges(id)
}
func (v engineConstraintView) Node(id NodeID) (*Node, error) {
	return v.engine.GetNode(id)
}

type txConstraintView struct{ tx *BadgerTransaction }

func (v txConstraintView) OutgoingEdges(id NodeID) ([]*Edge, error) {
	return v.tx.currentOutgoingEdgesLocked(id)
}
func (v txConstraintView) IncomingEdges(id NodeID) ([]*Edge, error) {
	return v.tx.currentIncomingEdgesLocked(id)
}
func (v txConstraintView) Node(id NodeID) (*Node, error) {
	return v.tx.currentNodeLocked(id)
}

// Evaluation reads a revision's plan; it never compiles.
func evaluateNodeConstraintContractPlan(view constraintGraphView, node *Node, plan *constraintContractEntryPlan) (bool, error) {
	if plan.err != nil {
		return false, plan.err
	}
	return plan.node.evaluate(view, node)
}

// Admission and runtime consume the same immutable value plan. Creating a
// contract must validate every supported predicate, even on an empty graph,
// but need not allocate a runtime function and discard it after validation.
type contractPredicateKind uint8

const (
	contractPropertyIn contractPredicateKind = iota
	contractPropertyComparison
	contractPatternCount
	contractPatternAbsent
	contractDistinctEndpoints
	contractEndpointProperties
)

type contractNodePredicate struct {
	kind                 contractPredicateKind
	property, comparator string
	values               []interface{}
	value                interface{}
	pattern              contractPattern
	threshold            int
}

// Property predicates retain SQL CHECK semantics: an absent property satisfies
// them; presence is required separately with IS NOT NULL.
func (predicate *contractNodePredicate) evaluate(view constraintGraphView, node *Node) (bool, error) {
	switch predicate.kind {
	case contractPropertyIn:
		return evaluatePropertyInExpression(node.Properties[predicate.property], predicate.values), nil
	case contractPropertyComparison:
		return compareConstraintExpressionValue(node.Properties[predicate.property], predicate.comparator, predicate.value), nil
	case contractPatternCount, contractPatternAbsent:
		count, err := countMatchingPatternEdges(view, node, predicate.pattern)
		if err != nil {
			return false, err
		}
		if predicate.kind == contractPatternAbsent {
			return count == 0, nil
		}
		return compareInt(count, predicate.comparator, predicate.threshold), nil
	default:
		panic("invalid node constraint predicate plan")
	}
}

func compileNodeConstraintContractExpression(expr string) (contractNodePredicate, error) {
	if hook := constraintContractCompileHook.Load(); hook != nil {
		(*hook)()
	}
	if matched, values, property, err := parsePropertyInExpression(expr); err != nil {
		return contractNodePredicate{}, err
	} else if matched {
		return contractNodePredicate{kind: contractPropertyIn, property: property, values: values}, nil
	}
	if matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression(expr); err != nil {
		return contractNodePredicate{}, err
	} else if matched {
		return contractNodePredicate{kind: contractPropertyComparison, property: property, comparator: comparator, value: value}, nil
	}
	if matched, pattern, comparator, threshold, err := parseCountPatternExpression(expr); err != nil {
		return contractNodePredicate{}, err
	} else if matched {
		return contractNodePredicate{kind: contractPatternCount, pattern: pattern, comparator: comparator, threshold: threshold}, nil
	}
	if matched, pattern, err := parseNotExistsPatternExpression(expr); err != nil {
		return contractNodePredicate{}, err
	} else if matched {
		return contractNodePredicate{kind: contractPatternAbsent, pattern: pattern}, nil
	}
	return contractNodePredicate{}, localizedError(localization.StorageSchemaUnsupportedNodePredicate(), nil)
}

func evaluateRelationshipConstraintContractPlan(view constraintGraphView, edge *Edge, plan *constraintContractEntryPlan) (bool, error) {
	if plan.err != nil {
		return false, plan.err
	}
	return plan.relationship.evaluate(view, edge)
}

type contractRelationshipPredicate struct {
	kind                 contractPredicateKind
	property, comparator string
	leftProp, rightProp  string
	values               []interface{}
	value                interface{}
}

func (predicate *contractRelationshipPredicate) evaluate(view constraintGraphView, edge *Edge) (bool, error) {
	switch predicate.kind {
	case contractPropertyIn:
		return evaluatePropertyInExpression(edge.Properties[predicate.property], predicate.values), nil
	case contractPropertyComparison:
		return compareConstraintExpressionValue(edge.Properties[predicate.property], predicate.comparator, predicate.value), nil
	case contractDistinctEndpoints:
		return edge.StartNode != edge.EndNode, nil
	case contractEndpointProperties:
		startNode, err := view.Node(edge.StartNode)
		if err != nil {
			return false, err
		}
		endNode, err := view.Node(edge.EndNode)
		if err != nil {
			return false, err
		}
		if startNode == nil || endNode == nil {
			return false, localizedError(localization.StorageSchemaMissingRelationshipEndpoint(), nil)
		}
		return compareValues(startNode.Properties[predicate.leftProp], endNode.Properties[predicate.rightProp]), nil
	default:
		panic("invalid relationship constraint predicate plan")
	}
}

func compileRelationshipConstraintContractExpression(expr string) (contractRelationshipPredicate, error) {
	if hook := constraintContractCompileHook.Load(); hook != nil {
		(*hook)()
	}
	if matched, property, values, err := parseRelationshipPropertyInExpression(expr); err != nil {
		return contractRelationshipPredicate{}, err
	} else if matched {
		return contractRelationshipPredicate{kind: contractPropertyIn, property: property, values: values}, nil
	}
	if isDistinctEndpointsExpression(expr) {
		return contractRelationshipPredicate{kind: contractDistinctEndpoints}, nil
	}
	if matched, leftProp, rightProp := parseEndpointPropertyEqualityExpression(expr); matched {
		return contractRelationshipPredicate{kind: contractEndpointProperties, leftProp: leftProp, rightProp: rightProp}, nil
	}
	if matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression(expr); err != nil {
		return contractRelationshipPredicate{}, err
	} else if matched {
		return contractRelationshipPredicate{kind: contractPropertyComparison, property: property, comparator: comparator, value: value}, nil
	}
	return contractRelationshipPredicate{}, localizedError(localization.StorageSchemaUnsupportedRelationshipPredicate(), nil)
}

// ValidateConstraintContractExpressions compiles every boolean entry of a
// contract so unsupported predicates fail at creation, even on an empty graph.
// Validation alone retains no plan: it compiles each entry with the same
// owner and keeps only the first error.
func ValidateConstraintContractExpressions(contract ConstraintContract) error {
	for _, entry := range contract.Entries {
		if plan, ok := compileConstraintContractEntryPlan(entry); ok && plan.err != nil {
			return constraintContractInvalidError(contract, entry, plan.err)
		}
	}
	return nil
}

type contractPattern = cyphertext.ConstraintPattern

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
	close := cyphertext.FindMatchingDelimiter(s, p, '[', ']')
	if close < 0 {
		return false, nil, "", nil
	}
	listContent := s[p+1 : close]
	p = close + 1
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
	close := cyphertext.FindMatchingDelimiter(s, p, '{', '}')
	if close < 0 {
		return false, contractPattern{}, "", 0, nil
	}
	inner := strings.TrimSpace(s[p+1 : close])
	p = close + 1

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
	close := cyphertext.FindMatchingDelimiter(s, p, '{', '}')
	if close < 0 {
		return false, contractPattern{}, nil
	}
	inner := strings.TrimSpace(s[p+1 : close])
	p = close + 1

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
	pattern, ok := cyphertext.CompileConstraintPattern(raw)
	if !ok {
		return contractPattern{}, localizedError(localization.StorageSchemaUnsupportedConstraintPattern(raw), nil)
	}
	return pattern, nil
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
		return true
	}
	for _, value := range values {
		if compareValues(actual, value) {
			return true
		}
	}
	return false
}

func compareConstraintExpressionValue(actual interface{}, comparator string, expected interface{}) bool {
	if actual == nil {
		return true
	}
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
	return countMatchingPatternEdges(engineConstraintView{engine}, node, pattern)
}

func countMatchingPatternEdges(view constraintGraphView, node *Node, pattern contractPattern) (int, error) {
	var edges []*Edge
	var err error
	if pattern.Direction() == "INCOMING" {
		edges, err = view.IncomingEdges(node.ID)
	} else {
		edges, err = view.OutgoingEdges(node.ID)
	}
	if err != nil {
		return 0, err
	}
	count := 0
	for _, edge := range edges {
		if !pattern.MatchesType(edge.Type) {
			continue
		}
		if pattern.TargetLabelCount() > 0 {
			otherID := edge.EndNode
			if pattern.Direction() == "INCOMING" {
				otherID = edge.StartNode
			}
			otherNode, err := view.Node(otherID)
			if err != nil {
				return 0, err
			}
			if otherNode == nil || !pattern.MatchesTargetLabels(otherNode.Labels) {
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
	return tx.validateConstraintContractsLocked(dbName, ConstraintEntityNode, node.Labels, ConstraintContractKindBooleanNode, node, nil)
}

// validateConstraintContractsLocked is the shared validation core for node and
// edge contract checks: namespace/schema resolution, contract collection
// (name-deduplicated across the entity's targets in target order, with each
// target's contracts in the schema's name-sorted order — deterministic, and
// the single-target edge path preserves its previous evaluation order),
// boolean-kind filtering, evaluation and the invalid/violated error wrapping
// are one implementation. Exactly one of node/edge is non-nil and selects the
// entity-specific evaluator; the per-entry dispatch keeps the kernel
// allocation-free on the commit path.
func (tx *BadgerTransaction) validateConstraintContractsLocked(dbName string, entity ConstraintEntityType, targets []string, kind string, node *Node, edge *Edge) error {
	schema := tx.engine.GetSchemaForNamespace(dbName)
	if schema == nil {
		return nil
	}
	// Single pass: collect and validate in target order (label order for
	// nodes, the single relationship type for edges) with each target's
	// contracts in the schema's name-sorted order — deterministic, and the
	// single-target edge path preserves its previous evaluation order.
	// Name dedup keeps the first occurrence of a contract that applies to
	// several of the entity's targets.
	seen := make(map[string]struct{})
	for _, target := range targets {
		for _, revision := range schema.constraintContractRevisionsForTarget(entity, target) {
			contract := revision.contract
			if _, dup := seen[contract.Name]; dup {
				continue
			}
			seen[contract.Name] = struct{}{}
			for index := range contract.Entries {
				entry := &contract.Entries[index]
				if entry.Kind != kind {
					continue
				}
				var ok bool
				var err error
				if node != nil {
					ok, err = evaluateNodeConstraintContractPlan(txConstraintView{tx}, node, &revision.plans[index])
				} else {
					ok, err = evaluateRelationshipConstraintContractPlan(txConstraintView{tx}, edge, &revision.plans[index])
				}
				if err != nil {
					return constraintContractInvalidError(contract, *entry, err)
				}
				if !ok {
					return localizedError(localization.StorageSchemaConstraintContractViolated(contract.Name, entry.Expression), nil)
				}
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
	return tx.validateConstraintContractsLocked(dbName, ConstraintEntityRelationship, []string{edge.Type}, ConstraintContractKindBooleanRelationship, nil, edge)
}

func (tx *BadgerTransaction) countMatchingPatternEdgesLocked(node *Node, pattern contractPattern) (int, error) {
	return countMatchingPatternEdges(txConstraintView{tx}, node, pattern)
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
