package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// schemaReferenceGrouping is the grouping a schema grammar admits around one
// property reference. It is explicit so the two schema grammars keep their own
// admission while sharing one reader: a constraint createStatement may enclose
// its single property — (n.prop) — while an index ON list item is already one
// member of a list and must be a direct reference.
type schemaReferenceGrouping bool

const (
	// schemaReferenceGrouped admits the parentheses a constraint
	// createStatement writes around one property.
	schemaReferenceGrouped schemaReferenceGrouping = true
	// schemaReferenceUngrouped admits only the direct reference of a list
	// item.
	schemaReferenceUngrouped schemaReferenceGrouping = false
)

// schemaPropertyReference is the one owner of a schema declaration's property
// reference: a variable and a property, both decoded identities, read with the
// shared quote-aware boundary reader so a dot or comma inside a symbolic name
// stays data and a quoted FOR variable matches the reference that names it.
func schemaPropertyReference(expr string, grouping schemaReferenceGrouping) (qualifier, property string, ok bool) {
	expr = strings.TrimSpace(expr)
	if grouping == schemaReferenceGrouped {
		if inner, wrapped := stripEnclosingExpressionParentheses(expr); wrapped {
			expr = strings.TrimSpace(inner)
		}
	}
	qualifier, property, ok = splitPostfixPropertyAccess(expr)
	if !ok || !isPatternVariableName(qualifier) || property == "" {
		return "", "", false
	}
	return cyphertext.SymbolicNameValue(qualifier), property, true
}

// constraintRequirementKind is the property predicate a constraint's REQUIRE
// expression spells. The scalar forms name one property; the list forms group
// the properties their constraint covers.
type constraintRequirementKind string

const (
	constraintRequirementUnique          constraintRequirementKind = "unique"
	constraintRequirementExists          constraintRequirementKind = "exists"
	constraintRequirementPropertyType    constraintRequirementKind = "property type"
	constraintRequirementDomain          constraintRequirementKind = "domain"
	constraintRequirementNodeKey         constraintRequirementKind = "node key"
	constraintRequirementRelationshipKey constraintRequirementKind = "relationship key"
	constraintRequirementTemporal        constraintRequirementKind = "temporal"
)

// constraintPropertyRequirement is the typed product of reading one constraint
// REQUIRE property predicate against the FOR target it belongs to: every
// property reference is already bound to that target's variable, so a consumer
// cannot register, index or enforce a property the declaration did not name.
// A scalar predicate keeps its one property in property; a list predicate keeps
// the slice its grouping needs. The type and allowed-value payloads travel with
// their requirement, so the property type and domain owners no longer read them
// again.
type constraintPropertyRequirement struct {
	kind         constraintRequirementKind
	property     string
	properties   []string
	expectedType storage.PropertyType
	allowedRaw   string
}

// constraintReferenceMismatchError is the one error a recognized property
// predicate returns when a reference names another variable, another shape of
// expression, or a property twice in a list that forbids it: the reference
// binds to nothing the FOR pattern declares, so the declaration is rejected
// before any schema or data change.
func constraintReferenceMismatchError() error {
	return localizedError(localization.CypherSchemaUnsupportedPredicate("constraint property references"), nil)
}

// constraintBoundPropertyReference reads one property reference of a constraint
// predicate and binds it to the FOR variable the pattern declares.
func constraintBoundPropertyReference(expr, variable string) (property string, reference bool, err error) {
	qualifier, property, ok := schemaPropertyReference(expr, schemaReferenceGrouped)
	if !ok {
		return "", false, nil
	}
	if qualifier != variable {
		return "", true, constraintReferenceMismatchError()
	}
	return property, true, nil
}

// constraintBoundPropertyList reads a constraint's parenthesized property list:
// every item is one reference bound to the FOR variable, read with the shared
// comma and property readers, so a quoted comma or dot stays part of its name.
// An empty list is a list of no properties; the shape that needs one reports
// its own arity error.
func constraintBoundPropertyList(list, variable string) ([]string, error) {
	parts := splitTopLevelCommaKeepEmpty(list)
	properties := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			if strings.TrimSpace(list) == "" {
				continue
			}
			return nil, constraintReferenceMismatchError()
		}
		qualifier, property, ok := schemaPropertyReference(part, schemaReferenceUngrouped)
		if !ok || qualifier != variable {
			return nil, constraintReferenceMismatchError()
		}
		properties = append(properties, property)
	}
	return properties, nil
}

// constraintListRequirementKind reads the suffix of a parenthesized property
// list: which constraint the list spells, with the shared keyword reader and no
// residual text.
func constraintListRequirementKind(ctx context.Context, suffix string) (constraintRequirementKind, bool) {
	suffix = strings.TrimSpace(suffix)
	if suffix == "" {
		return "", false
	}
	for _, candidate := range []struct {
		keyword string
		kind    constraintRequirementKind
	}{
		{keyword: "IS NODE KEY", kind: constraintRequirementNodeKey},
		{keyword: "IS RELATIONSHIP KEY", kind: constraintRequirementRelationshipKey},
		{keyword: "IS UNIQUE", kind: constraintRequirementUnique},
		{keyword: "IS TEMPORAL NO OVERLAP", kind: constraintRequirementTemporal},
		{keyword: "IS TEMPORAL", kind: constraintRequirementTemporal},
	} {
		if keywordIndexFrom(ctx, suffix, candidate.keyword, 0, defaultKeywordScanOpts()) != 0 {
			continue
		}
		end, ok := keywordSpanAt(suffix, 0, candidate.keyword)
		if !ok || strings.TrimSpace(suffix[end:]) != "" {
			continue
		}
		return candidate.kind, true
	}
	return "", false
}

// splitConstraintRequirementReference splits a scalar predicate into the one
// property reference before keyword and requires keyword to end the expression.
func splitConstraintRequirementReference(ctx context.Context, requireExpr, keyword string) (string, bool) {
	idx := keywordIndexFrom(ctx, requireExpr, keyword, 0, defaultKeywordScanOpts())
	if idx <= 0 {
		return "", false
	}
	end, ok := keywordSpanAt(requireExpr, idx, keyword)
	if !ok || strings.TrimSpace(requireExpr[end:]) != "" {
		return "", false
	}
	return strings.TrimSpace(requireExpr[:idx]), true
}

// parseConstraintPropertyTypeRequirement reads the property type predicate
// `n.p IS :: T` / `n.p IS TYPED T` and resolves T here, so the native
// declaration and the contract primitive read one type from one place.
func parseConstraintPropertyTypeRequirement(ctx context.Context, requireExpr, variable string) (constraintPropertyRequirement, bool, error) {
	idx := keywordIndexFrom(ctx, requireExpr, "IS", 0, defaultKeywordScanOpts())
	if idx <= 0 {
		return constraintPropertyRequirement{}, false, nil
	}
	end, ok := keywordSpanAt(requireExpr, idx, "IS")
	if !ok {
		return constraintPropertyRequirement{}, false, nil
	}
	rhs := strings.TrimSpace(requireExpr[end:])
	var typeName string
	switch {
	case strings.HasPrefix(rhs, "::"):
		typeName = strings.TrimSpace(rhs[len("::"):])
	case startsWithKeywordFold(rhs, "TYPED"):
		typeName = strings.TrimSpace(rhs[len("TYPED"):])
	default:
		return constraintPropertyRequirement{}, false, nil
	}
	if typeName == "" {
		return constraintPropertyRequirement{}, false, nil
	}
	property, reference, err := constraintBoundPropertyReference(strings.TrimSpace(requireExpr[:idx]), variable)
	if err != nil {
		return constraintPropertyRequirement{}, true, err
	}
	if !reference {
		return constraintPropertyRequirement{}, false, nil
	}
	expectedType, err := parsePropertyType(typeName)
	if err != nil {
		return constraintPropertyRequirement{}, true, err
	}
	return constraintPropertyRequirement{kind: constraintRequirementPropertyType, property: property, expectedType: expectedType}, true, nil
}

// parseConstraintPropertyRequirement is the one reader of a constraint's
// REQUIRE property predicates, for the native declaration and the contract
// primitive alike. variable is the FOR pattern's decoded variable, and every
// reference the predicate names is bound to it here.
//
// recognized is false when requireExpr is not a property predicate at all: a
// contract boolean expression, MAX COUNT, ALLOWED, or any other expression. A
// recognized predicate that does not bind, or whose type is inadmissible,
// returns its error with recognized set, so no consumer falls through to
// another reading of a malformed primitive.
func parseConstraintPropertyRequirement(ctx context.Context, variable, requireExpr string) (requirement constraintPropertyRequirement, recognized bool, err error) {
	requireExpr = strings.TrimSpace(requireExpr)
	if requireExpr == "" {
		return constraintPropertyRequirement{}, false, nil
	}

	// Grouped forms: one parenthesized property list and the suffix that says
	// what the list means.
	if inside, rest, grouped := extractParenSection(requireExpr); grouped {
		if kind, ok := constraintListRequirementKind(ctx, rest); ok {
			properties, err := constraintBoundPropertyList(inside, variable)
			if err != nil {
				return constraintPropertyRequirement{}, true, err
			}
			// (n.p) IS UNIQUE is one property, and stays the single-property
			// form every consumer already handles without a list.
			if kind == constraintRequirementUnique && len(properties) == 1 {
				return constraintPropertyRequirement{kind: kind, property: properties[0]}, true, nil
			}
			return constraintPropertyRequirement{kind: kind, properties: properties}, true, nil
		}
	}

	// Scalar forms: one property reference before the suffix.
	for _, candidate := range []struct {
		keyword string
		kind    constraintRequirementKind
	}{
		{keyword: "IS UNIQUE", kind: constraintRequirementUnique},
		{keyword: "IS NOT NULL", kind: constraintRequirementExists},
		{keyword: "IS RELATIONSHIP KEY", kind: constraintRequirementRelationshipKey},
	} {
		reference, ok := splitConstraintRequirementReference(ctx, requireExpr, candidate.keyword)
		if !ok {
			continue
		}
		property, isReference, err := constraintBoundPropertyReference(reference, variable)
		if err != nil {
			return constraintPropertyRequirement{}, true, err
		}
		if !isReference {
			if candidate.kind == constraintRequirementExists {
				return constraintPropertyRequirement{}, false, nil
			}
			return constraintPropertyRequirement{}, true, constraintReferenceMismatchError()
		}
		return constraintPropertyRequirement{kind: candidate.kind, property: property}, true, nil
	}

	// Existence written as the prefix call: EXISTS(n.p).
	if matchKeywordAt(requireExpr, 0, "EXISTS") {
		rest := strings.TrimSpace(requireExpr[len("EXISTS"):])
		inside, trailing, ok := extractParenSection(rest)
		if !ok || strings.TrimSpace(trailing) != "" {
			return constraintPropertyRequirement{}, false, nil
		}
		property, reference, err := constraintBoundPropertyReference(inside, variable)
		if err != nil {
			return constraintPropertyRequirement{}, true, err
		}
		if !reference {
			return constraintPropertyRequirement{}, false, nil
		}
		return constraintPropertyRequirement{kind: constraintRequirementExists, property: property}, true, nil
	}

	if requirement, ok, err := parseConstraintPropertyTypeRequirement(ctx, requireExpr, variable); ok || err != nil {
		return requirement, ok, err
	}

	// Domain: one property reference and the value list that bounds it.
	if inPos := keywordIndexFrom(ctx, requireExpr, "IN", 0, defaultKeywordScanOpts()); inPos > 0 {
		if inEnd, ok := keywordSpanAt(requireExpr, inPos, "IN"); ok {
			inside, rest, bracketed := extractBracketSection(strings.TrimSpace(requireExpr[inEnd:]))
			if bracketed && strings.TrimSpace(rest) == "" {
				property, reference, err := constraintBoundPropertyReference(strings.TrimSpace(requireExpr[:inPos]), variable)
				if err != nil {
					return constraintPropertyRequirement{}, true, err
				}
				if !reference {
					return constraintPropertyRequirement{}, false, nil
				}
				return constraintPropertyRequirement{kind: constraintRequirementDomain, property: property, allowedRaw: inside}, true, nil
			}
		}
	}

	return constraintPropertyRequirement{}, false, nil
}

// parsedConstraintPropertyDeclaration is one CREATE CONSTRAINT … FOR …
// REQUIRE property declaration: the shared FOR/REQUIRE product plus the typed
// requirement that product's expression spells, bound to its pattern variable.
type parsedConstraintPropertyDeclaration struct {
	parsedConstraintForRequireDDL
	requirement constraintPropertyRequirement
}

// parseCreateConstraintPropertyDDL is the one declaration owner for a
// constraint whose REQUIRE is a property predicate: it reads the shared
// FOR/REQUIRE product and the typed requirement bound to it, so no shape
// re-reads the prefix or its own classifier.
//
// recognized is false when the statement is not this shape: its prefix is not a
// CREATE CONSTRAINT … FOR … REQUIRE head (err carries why, and a caller that
// owns another shape ignores it) or its REQUIRE is not a property predicate.
func (e *StorageExecutor) parseCreateConstraintPropertyDDL(ctx context.Context, cypher string) (parsedConstraintPropertyDeclaration, bool, error) {
	parsed, err := e.parseCreateConstraintForRequireDDL(ctx, cypher)
	if err != nil {
		return parsedConstraintPropertyDeclaration{}, false, err
	}
	requirement, recognized, err := parseConstraintPropertyRequirement(ctx, parsed.variable, parsed.requireExpr)
	if !recognized {
		return parsedConstraintPropertyDeclaration{}, false, err
	}
	if err != nil {
		return parsedConstraintPropertyDeclaration{}, true, err
	}
	return parsedConstraintPropertyDeclaration{parsedConstraintForRequireDDL: parsed, requirement: requirement}, true, nil
}

// executeCreateConstraintProperty creates the constraint a recognized property
// declaration names. The requirement decides the constraint, its entity and its
// default name; each kind's admission and error order are those the shape
// parsers produced.
func (e *StorageExecutor) executeCreateConstraintProperty(ctx context.Context, decl parsedConstraintPropertyDeclaration, ifNotExists bool) (*ExecuteResult, error) {
	switch decl.requirement.kind {
	case constraintRequirementUnique:
		if decl.requirement.properties == nil {
			return e.executeUniquePropertyConstraint(ctx, decl, ifNotExists)
		}
		return e.executeCompositeUniqueConstraint(ctx, decl, ifNotExists)

	case constraintRequirementExists:
		constraintName := decl.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_exists", decl.label, decl.requirement.property)
		}
		if _, err := e.schemaOptionMap(ctx, decl.options, "existence constraint"); err != nil {
			return nil, err
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			Label:      decl.label,
			Properties: []string{decl.requirement.property},
		}
		if decl.isRelationship {
			constraint.EntityType = storage.ConstraintEntityRelationship
		}
		if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil

	case constraintRequirementPropertyType:
		constraintName := decl.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_type", decl.label, decl.requirement.property)
		}
		if _, err := e.schemaOptionMap(ctx, decl.options, "property type constraint"); err != nil {
			return nil, err
		}
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			Label:        decl.label,
			Property:     decl.requirement.property,
			ExpectedType: decl.requirement.expectedType,
		}
		opts := storage.PropertyTypeConstraintOptions{IfNotExists: ifNotExists}
		if decl.isRelationship {
			ptc.EntityType = storage.ConstraintEntityRelationship
			opts.EntityType = storage.ConstraintEntityRelationship
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(ctx, e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraint(constraintName, decl.label, decl.requirement.property, decl.requirement.expectedType, opts); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil

	case constraintRequirementDomain:
		allowedValues, err := parseDomainValueList(decl.requirement.allowedRaw)
		if err != nil {
			return nil, localizedError(localization.CypherSchemaDomainValueListInvalid(err), err)
		}
		if len(allowedValues) == 0 {
			return nil, localizedError(localization.CypherSchemaDomainAllowedValueRequired(), nil)
		}
		constraintName := decl.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_domain", decl.label, decl.requirement.property)
		}
		if _, err := e.schemaOptionMap(ctx, decl.options, "domain constraint"); err != nil {
			return nil, err
		}
		constraint := storage.Constraint{
			Name:          constraintName,
			Type:          storage.ConstraintDomain,
			Label:         decl.label,
			Properties:    []string{decl.requirement.property},
			AllowedValues: allowedValues,
		}
		if decl.isRelationship {
			constraint.EntityType = storage.ConstraintEntityRelationship
		}
		if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil

	case constraintRequirementNodeKey:
		if decl.isRelationship {
			return nil, localizedError(localization.CypherSchemaInvalidPattern("NODE KEY"), nil)
		}
		if len(decl.requirement.properties) == 0 {
			return nil, localizedError(localization.CypherSchemaConstraintPropertiesRequired("NODE KEY"), nil)
		}
		if _, err := e.schemaOptionMap(ctx, decl.options, "node key constraint"); err != nil {
			return nil, err
		}
		constraintName := decl.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_node_key", decl.label, strings.Join(decl.requirement.properties, "_"))
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintNodeKey,
			Label:      decl.label,
			Properties: decl.requirement.properties,
		}
		if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil

	case constraintRequirementTemporal:
		// A temporal constraint is (key…, valid_from, valid_to) with a 1..n
		// property grouping key, for nodes and relationships alike.
		if len(decl.requirement.properties) < 3 {
			if decl.isRelationship {
				return nil, localizedError(localization.CypherSchemaTemporalRelationshipArityRequired(), nil)
			}
			return nil, localizedError(localization.CypherSchemaTemporalNodeArityRequired(), nil)
		}
		constraintName := decl.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_temporal", decl.label, strings.Join(decl.requirement.properties, "_"))
		}
		if _, err := e.schemaOptionMap(ctx, decl.options, "temporal constraint"); err != nil {
			return nil, err
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintTemporal,
			Label:      decl.label,
			Properties: decl.requirement.properties,
		}
		if decl.isRelationship {
			constraint.EntityType = storage.ConstraintEntityRelationship
		}
		if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil

	case constraintRequirementRelationshipKey:
		if !decl.isRelationship {
			return nil, localizedError(localization.CypherSchemaUnsupportedPredicate("relationship key/unique"), nil)
		}
		// The unparenthesized r.k spelling is the single-property form of the
		// same requirement.
		properties := decl.requirement.properties
		if properties == nil {
			properties = []string{decl.requirement.property}
		}
		seen := make(map[string]struct{}, len(properties))
		for _, property := range properties {
			if _, duplicate := seen[property]; duplicate {
				return nil, constraintReferenceMismatchError()
			}
			seen[property] = struct{}{}
		}
		if len(properties) == 0 {
			return nil, localizedError(localization.CypherSchemaConstraintPropertiesRequired("RELATIONSHIP KEY"), nil)
		}
		if _, err := e.schemaOptionMap(ctx, decl.options, "constraint"); err != nil {
			return nil, err
		}
		constraintName := decl.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_rel_key", decl.label, strings.Join(properties, "_"))
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintRelationshipKey,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      decl.label,
			Properties: properties,
		}
		if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return nil, localizedError(localization.CypherSchemaUnsupportedPredicate("constraint"), nil)
}

// executeUniquePropertyConstraint creates a uniqueness constraint over the one
// property a scalar declaration names. The node form fills the value cache its
// seeks and validation use; the relationship form keeps the pass-through the
// shape parser had.
func (e *StorageExecutor) executeUniquePropertyConstraint(ctx context.Context, decl parsedConstraintPropertyDeclaration, ifNotExists bool) (*ExecuteResult, error) {
	if _, err := e.schemaOptionMap(ctx, decl.options, "uniqueness constraint"); err != nil {
		return nil, err
	}
	constraint := storage.Constraint{
		Name:       decl.name,
		Type:       storage.ConstraintUnique,
		Label:      decl.label,
		Properties: []string{decl.requirement.property},
	}
	if decl.isRelationship {
		constraint.EntityType = storage.ConstraintEntityRelationship
		if constraint.Name == "" {
			constraint.Name = fmt.Sprintf("constraint_%s_%s_unique", decl.label, decl.requirement.property)
		}
		if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	if constraint.Name == "" {
		constraint.Name = fmt.Sprintf("constraint_%s_%s", decl.label, decl.requirement.property)
	}
	if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
		return nil, err
	}
	schema := e.storage.GetSchema()
	if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
		return nil, err
	}
	if err := storage.RefreshUniqueConstraintValuesForEngine(ctx, e.storage, schema); err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// executeCompositeUniqueConstraint creates a uniqueness constraint over the
// property list a declaration groups, for nodes and relationships alike.
func (e *StorageExecutor) executeCompositeUniqueConstraint(ctx context.Context, decl parsedConstraintPropertyDeclaration, ifNotExists bool) (*ExecuteResult, error) {
	// One composite schema cannot name the same property twice.
	seen := make(map[string]struct{}, len(decl.requirement.properties))
	for _, property := range decl.requirement.properties {
		if _, duplicate := seen[property]; duplicate {
			return nil, newSemanticError("Neo.ClientError.Schema.RepeatedPropertyInCompositeSchema", "RepeatedPropertyInCompositeSchema",
				"A composite schema cannot contain the same property more than once: "+property)
		}
		seen[property] = struct{}{}
	}
	if len(decl.requirement.properties) == 0 {
		return nil, localizedError(localization.CypherSchemaConstraintPropertiesRequired("UNIQUE"), nil)
	}
	if _, err := e.schemaOptionMap(ctx, decl.options, "constraint"); err != nil {
		return nil, err
	}
	constraint := storage.Constraint{
		Name:       decl.name,
		Type:       storage.ConstraintUnique,
		Label:      decl.label,
		Properties: decl.requirement.properties,
	}
	if decl.isRelationship {
		constraint.EntityType = storage.ConstraintEntityRelationship
	}
	if constraint.Name == "" {
		constraint.Name = fmt.Sprintf("constraint_%s_%s_unique", decl.label, strings.Join(decl.requirement.properties, "_"))
	}
	if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
		return nil, err
	}
	if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
