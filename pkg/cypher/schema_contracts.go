package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type parsedCreateConstraintContractDDL struct {
	name        string
	entityType  storage.ConstraintEntityType
	variable    string
	labelOrType string
	body        string
}

// parseConstraintContractForClause reads a constraint contract's FOR clause
// with the shared FOR pattern reader and maps the entity it names to the
// contract's entity type: the contract owns that mapping and its own rejection,
// while the pattern reading itself stays in one place.
func parseConstraintContractForClause(forClause string) (entityType storage.ConstraintEntityType, variable string, labelOrType string, ok bool) {
	target, ok := parseForPatternTarget(forClause)
	if !ok {
		return "", "", "", false
	}
	variable = cyphertext.SymbolicNameValue(target.variableSource)
	if target.isRelationship {
		return storage.ConstraintEntityRelationship, variable, target.name, true
	}
	return storage.ConstraintEntityNode, variable, target.name, true
}

func (e *StorageExecutor) parseCreateConstraintContractDDL(ctx context.Context, cypher string) (parsedCreateConstraintContractDDL, error) {
	// The contract statement shares the CREATE CONSTRAINT [name] FOR <pattern>
	// REQUIRE prefix with every other shape, so it reads that prefix from the
	// shared owner. Its REQUIRE is a block rather than a predicate, so it reads
	// the REQUIRE tail itself: the whole tail must be the block, and a contract
	// statement therefore admits no OPTIONS after it.
	name, patternSpan, requireText, err := e.parseCreateConstraintForRequireParts(ctx, cypher)
	if err != nil {
		return parsedCreateConstraintContractDDL{}, err
	}

	entityType, variable, labelOrType, ok := parseConstraintContractForClause(patternSpan)
	if !ok {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidPattern("FOR"), nil)
	}

	body, tail, ok := extractDelimitedSection(strings.TrimSpace(requireText), '{', '}')
	if !ok || strings.TrimSpace(tail) != "" {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidClause("REQUIRE block"), nil)
	}

	if name == "" {
		name = fmt.Sprintf("constraint_%s_contract", labelOrType)
	}
	return parsedCreateConstraintContractDDL{
		name:        name,
		entityType:  entityType,
		variable:    variable,
		labelOrType: labelOrType,
		body:        body,
	}, nil
}

// executeCreateConstraintContract executes the contract statement whose shape
// parseCreateConstraintContractDDL admitted: admission is the parse's, so this
// runs only for a statement the contract owner parses and decides no syntax of
// its own.
func (e *StorageExecutor) executeCreateConstraintContract(ctx context.Context, definition string, parsed parsedCreateConstraintContractDDL, ifNotExists bool) (*ExecuteResult, error) {
	name, entityType, variable, labelOrType, body := parsed.name, parsed.entityType, parsed.variable, parsed.labelOrType, parsed.body
	entriesRaw, err := splitConstraintContractEntries(body)
	if err != nil {
		return nil, err
	}
	if len(entriesRaw) == 0 {
		return nil, localizedError(localization.CypherSchemaContractEntryRequired(), nil)
	}

	contract := storage.ConstraintContract{
		Name:              name,
		TargetEntityType:  string(entityType),
		TargetLabelOrType: labelOrType,
		Definition:        strings.TrimSpace(definition),
		Entries:           make([]storage.ConstraintContractEntry, 0, len(entriesRaw)),
	}
	compiledConstraints := make([]storage.Constraint, 0)
	compiledTypes := make([]storage.PropertyTypeConstraint, 0)

	for idx, rawEntry := range entriesRaw {
		entry, compiledConstraint, compiledType, err := e.parseConstraintContractEntry(ctx, rawEntry, variable, entityType, labelOrType, name, idx)
		if err != nil {
			return nil, err
		}
		contract.Entries = append(contract.Entries, entry)
		if compiledConstraint != nil {
			compiledConstraints = append(compiledConstraints, *compiledConstraint)
		}
		if compiledType != nil {
			compiledTypes = append(compiledTypes, *compiledType)
		}
	}

	for _, constraint := range compiledConstraints {
		if err := storage.ValidateConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
			return nil, err
		}
	}
	for _, constraint := range compiledTypes {
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(ctx, e.storage, constraint); err != nil {
			return nil, err
		}
	}
	if err := storage.ValidateConstraintContractOnCreationForEngine(ctx, e.storage, contract); err != nil {
		return nil, err
	}
	schema := e.storage.GetSchema()
	newTemporal := make([]storage.Constraint, 0)
	newNames := make([]string, 0, len(compiledConstraints))
	for _, constraint := range compiledConstraints {
		if schema.HasConstraintNamed(constraint.Name) {
			continue
		}
		newNames = append(newNames, constraint.Name)
		if constraint.Type == storage.ConstraintTemporal {
			newTemporal = append(newTemporal, constraint)
		}
	}
	if err := schema.AddConstraintContractBundle(contract, compiledConstraints, compiledTypes, ifNotExists); err != nil {
		return nil, err
	}
	// See addSchemaConstraint: new temporal entries are authoritative on an
	// empty label (#1126).
	if err := storage.MarkNewTemporalConstraintsForEngine(e.storage, schema, newTemporal); err != nil {
		return nil, err
	}
	// Fill the equality indexes the contract's UNIQUE / NODE KEY entries own,
	// as a standalone constraint's DDL does (addSchemaConstraint): until an
	// owned index is filled, validation and seeks fall back to scanning the
	// label.
	for _, name := range newNames {
		if index, owned := schema.ConstraintPropertyIndex(name); owned {
			if err := e.backfillCompositeIndex(ctx, index.Name, index.Label, index.Properties); err != nil {
				return nil, err
			}
		}
	}
	// Like a standalone UNIQUE constraint, a contract's IS UNIQUE entries need
	// their value cache built and marked complete; without it every write to
	// the label falls back to scanning all of its nodes (#1120).
	if err := storage.RefreshUniqueConstraintValuesForConstraintsForEngine(ctx, e.storage, schema, compiledConstraints); err != nil {
		return nil, err
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

func splitConstraintContractEntries(body string) ([]string, error) {
	entries := make([]string, 0)
	var current strings.Builder
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0

	flush := func() {
		entry := strings.TrimSpace(current.String())
		if entry != "" {
			entries = append(entries, entry)
		}
		current.Reset()
	}

	for index := 0; index < len(body); index++ {
		if end, skipped := skipCypherLexicalToken(body, index); skipped {
			current.WriteString(body[index:end])
			index = end - 1
			continue
		}
		ch := body[index]
		switch {
		case ch == '{':
			braceDepth++
			current.WriteByte(ch)
		case ch == '}':
			if braceDepth == 0 {
				return nil, localizedError(localization.CypherSchemaMalformedRequireBlock(), nil)
			}
			braceDepth--
			current.WriteByte(ch)
		case ch == '[':
			bracketDepth++
			current.WriteByte(ch)
		case ch == ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			current.WriteByte(ch)
		case ch == '(':
			parenDepth++
			current.WriteByte(ch)
		case ch == ')':
			if parenDepth > 0 {
				parenDepth--
			}
			current.WriteByte(ch)
		case ch == '\n' || ch == ';':
			if braceDepth == 0 && bracketDepth == 0 && parenDepth == 0 {
				flush()
				continue
			}
			current.WriteByte(ch)
		default:
			current.WriteByte(ch)
		}
	}
	if braceDepth != 0 || bracketDepth != 0 || parenDepth != 0 {
		return nil, localizedError(localization.CypherSchemaMalformedRequireBlock(), nil)
	}
	flush()
	return entries, nil
}

func isNestedConstraintContractEntry(entryText string) bool {
	trimmed := strings.TrimSpace(entryText)
	if !matchKeywordAt(trimmed, 0, "FOR") {
		return false
	}
	return hasKeywordFollowedByBrace(trimmed, "REQUIRE")
}

func nestedConstraintContractEntryError(entryText string) error {
	return localizedError(localization.CypherSchemaNestedContractEntryUnsupported(strings.TrimSpace(entryText)), nil)
}

func (e *StorageExecutor) parseConstraintContractEntry(ctx context.Context, rawEntry, variable string, entityType storage.ConstraintEntityType, labelOrType, contractName string, index int) (storage.ConstraintContractEntry, *storage.Constraint, *storage.PropertyTypeConstraint, error) {
	entryText := strings.TrimSpace(rawEntry)
	if isNestedConstraintContractEntry(entryText) {
		return storage.ConstraintContractEntry{}, nil, nil, nestedConstraintContractEntryError(entryText)
	}

	entryName := storage.ConstraintContractEntryName(contractName, index)
	// The entry reads its REQUIRE expression with the one property requirement
	// owner a native declaration uses, so the properties a contract compiles are
	// bound to the contract's FOR variable exactly as a declaration binds them.
	requirement, recognized, err := parseConstraintPropertyRequirement(ctx, variable, entryText)
	if err != nil {
		return storage.ConstraintContractEntry{}, nil, nil, err
	}
	if recognized {
		entry, constraint, propertyType, compiled, compileErr := compileConstraintContractPrimitive(requirement, entryText, entityType, labelOrType, entryName)
		if compiled || compileErr != nil {
			return entry, constraint, propertyType, compileErr
		}
	}

	kind := storage.ConstraintContractKindBooleanNode
	if entityType == storage.ConstraintEntityRelationship {
		kind = storage.ConstraintContractKindBooleanRelationship
	}
	return storage.ConstraintContractEntry{Kind: kind, Expression: entryText}, nil, nil, nil
}

// compileConstraintContractPrimitive turns one recognized requirement into the
// constraint a contract entry compiles. compiled is false for a requirement the
// contract does not compile — a domain value list, whose block form the
// contract evaluates as its own boolean expression, a grouped list of a scalar
// form, and a list the contract's entity does not admit — so the entry keeps
// the boolean expression it was written as. Every compiled primitive keeps its
// own admission and its own error.
func compileConstraintContractPrimitive(requirement constraintPropertyRequirement, entryText string, entityType storage.ConstraintEntityType, labelOrType, entryName string) (storage.ConstraintContractEntry, *storage.Constraint, *storage.PropertyTypeConstraint, bool, error) {
	primitiveKind := storage.ConstraintContractKindPrimitiveNode
	if entityType == storage.ConstraintEntityRelationship {
		primitiveKind = storage.ConstraintContractKindPrimitiveRelationship
	}

	switch requirement.kind {
	case constraintRequirementUnique, constraintRequirementExists:
		// A contract compiles the single-property form only; a grouped list
		// stays the expression the contract evaluates itself.
		if requirement.properties != nil {
			return storage.ConstraintContractEntry{}, nil, nil, false, nil
		}
		constraintType := storage.ConstraintUnique
		if requirement.kind == constraintRequirementExists {
			constraintType = storage.ConstraintExists
		}
		constraint := &storage.Constraint{
			Name:       entryName,
			Type:       constraintType,
			EntityType: entityType,
			Label:      labelOrType,
			Properties: []string{requirement.property},
		}
		return storage.ConstraintContractEntry{Kind: primitiveKind, PrimitiveType: string(constraintType), Property: requirement.property, Properties: []string{requirement.property}, Expression: entryText}, constraint, nil, true, nil

	case constraintRequirementPropertyType:
		propertyType := &storage.PropertyTypeConstraint{Name: entryName, EntityType: entityType, Label: labelOrType, Property: requirement.property, ExpectedType: requirement.expectedType}
		return storage.ConstraintContractEntry{Kind: primitiveKind, PrimitiveType: string(storage.ConstraintPropertyType), Property: requirement.property, Properties: []string{requirement.property}, ExpectedType: string(requirement.expectedType), Expression: entryText}, nil, propertyType, true, nil

	case constraintRequirementNodeKey:
		if entityType != storage.ConstraintEntityNode {
			return storage.ConstraintContractEntry{}, nil, nil, false, nil
		}
		if len(requirement.properties) == 0 {
			return storage.ConstraintContractEntry{}, nil, nil, true, localizedError(localization.CypherSchemaConstraintPropertiesRequired("NODE KEY"), nil)
		}
		constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintNodeKey, EntityType: entityType, Label: labelOrType, Properties: requirement.properties}
		return storage.ConstraintContractEntry{Kind: primitiveKind, PrimitiveType: string(storage.ConstraintNodeKey), Properties: requirement.properties, Expression: entryText}, constraint, nil, true, nil

	case constraintRequirementTemporal:
		if entityType != storage.ConstraintEntityNode {
			return storage.ConstraintContractEntry{}, nil, nil, false, nil
		}
		if len(requirement.properties) < 3 {
			return storage.ConstraintContractEntry{}, nil, nil, true, localizedError(localization.CypherSchemaTemporalNodeArityRequired(), nil)
		}
		constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintTemporal, EntityType: entityType, Label: labelOrType, Properties: requirement.properties}
		return storage.ConstraintContractEntry{Kind: primitiveKind, PrimitiveType: string(storage.ConstraintTemporal), Properties: requirement.properties, Expression: entryText}, constraint, nil, true, nil

	case constraintRequirementRelationshipKey:
		// A contract entry compiles the parenthesized list only: the
		// unparenthesized r.k IS RELATIONSHIP KEY spelling stays the boolean
		// expression the contract evaluates itself.
		if entityType != storage.ConstraintEntityRelationship || requirement.properties == nil {
			return storage.ConstraintContractEntry{}, nil, nil, false, nil
		}
		if len(requirement.properties) == 0 {
			return storage.ConstraintContractEntry{}, nil, nil, true, localizedError(localization.CypherSchemaConstraintPropertiesRequired("RELATIONSHIP KEY"), nil)
		}
		constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintRelationshipKey, EntityType: entityType, Label: labelOrType, Properties: requirement.properties}
		return storage.ConstraintContractEntry{Kind: primitiveKind, PrimitiveType: string(storage.ConstraintRelationshipKey), Properties: requirement.properties, Expression: entryText}, constraint, nil, true, nil
	}

	return storage.ConstraintContractEntry{}, nil, nil, false, nil
}
