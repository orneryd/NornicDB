package cypher

import (
	"context"
	"fmt"
	"strings"

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
	isRelationship, variable, labelOrType, ok := parseForPatternTarget(forClause)
	if !ok {
		return "", "", "", false
	}
	if isRelationship {
		return storage.ConstraintEntityRelationship, variable, labelOrType, true
	}
	return storage.ConstraintEntityNode, variable, labelOrType, true
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
	if compiledConstraint, contractEntry, ok, err := e.parseConstraintContractPrimitive(ctx, entryText, variable, entityType, labelOrType, entryName); ok || err != nil {
		return contractEntry, compiledConstraint, nil, err
	}
	if compiledType, contractEntry, ok, err := e.parseConstraintContractPropertyType(ctx, entryText, variable, entityType, labelOrType, entryName); ok || err != nil {
		return contractEntry, nil, compiledType, err
	}

	kind := storage.ConstraintContractKindBooleanNode
	if entityType == storage.ConstraintEntityRelationship {
		kind = storage.ConstraintContractKindBooleanRelationship
	}
	return storage.ConstraintContractEntry{Kind: kind, Expression: entryText}, nil, nil, nil
}

func (e *StorageExecutor) parseConstraintContractPrimitive(ctx context.Context, entryText, variable string, entityType storage.ConstraintEntityType, labelOrType, entryName string) (*storage.Constraint, storage.ConstraintContractEntry, bool, error) {
	if kind, property, ok := parseConstraintPredicate(ctx, entryText); ok {
		if kind == "unique" {
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintUnique, EntityType: entityType, Label: labelOrType, Properties: []string{property}}
			primitiveKind := storage.ConstraintContractKindPrimitiveNode
			if entityType == storage.ConstraintEntityRelationship {
				primitiveKind = storage.ConstraintContractKindPrimitiveRelationship
			}
			return constraint, storage.ConstraintContractEntry{Kind: primitiveKind, PrimitiveType: string(storage.ConstraintUnique), Property: property, Properties: []string{property}, Expression: entryText}, true, nil
		}
		if kind == "exists" {
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintExists, EntityType: entityType, Label: labelOrType, Properties: []string{property}}
			primitiveKind := storage.ConstraintContractKindPrimitiveNode
			if entityType == storage.ConstraintEntityRelationship {
				primitiveKind = storage.ConstraintContractKindPrimitiveRelationship
			}
			return constraint, storage.ConstraintContractEntry{Kind: primitiveKind, PrimitiveType: string(storage.ConstraintExists), Property: property, Properties: []string{property}, Expression: entryText}, true, nil
		}
	}

	if entityType == storage.ConstraintEntityNode {
		if properties, ok := e.parseNodeKeyPropertyList(ctx, entryText); ok {
			if len(properties) == 0 {
				return nil, storage.ConstraintContractEntry{}, true, localizedError(localization.CypherSchemaConstraintPropertiesRequired("NODE KEY"), nil)
			}
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintNodeKey, EntityType: entityType, Label: labelOrType, Properties: properties}
			return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintNodeKey), Properties: properties, Expression: entryText}, true, nil
		}
		if properties, ok := e.parseTemporalConstraintPredicate(ctx, entryText); ok {
			if len(properties) < 3 {
				return nil, storage.ConstraintContractEntry{}, true, localizedError(localization.CypherSchemaTemporalNodeArityRequired(), nil)
			}
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintTemporal, EntityType: entityType, Label: labelOrType, Properties: properties}
			return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintTemporal), Properties: properties, Expression: entryText}, true, nil
		}
	}

	if entityType == storage.ConstraintEntityRelationship {
		// The shared relationship-key predicate also admits the unparenthesized
		// r.k IS RELATIONSHIP KEY spelling. A contract entry admits only the
		// parenthesized list, so it guards that shape first and then reads the
		// predicate itself from the one owner.
		if _, _, parenthesized := extractParenSection(strings.TrimSpace(entryText)); parenthesized {
			if kind, properties, ok := e.parseRelationshipKeyOrCompositeUniquePredicate(ctx, entryText); ok && kind == "rel_key" {
				if len(properties) == 0 {
					return nil, storage.ConstraintContractEntry{}, true, localizedError(localization.CypherSchemaConstraintPropertiesRequired("RELATIONSHIP KEY"), nil)
				}
				constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintRelationshipKey, EntityType: entityType, Label: labelOrType, Properties: properties}
				return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveRelationship, PrimitiveType: string(storage.ConstraintRelationshipKey), Properties: properties, Expression: entryText}, true, nil
			}
		}
	}

	return nil, storage.ConstraintContractEntry{}, false, nil
}

func (e *StorageExecutor) parseConstraintContractPropertyType(ctx context.Context, entryText, variable string, entityType storage.ConstraintEntityType, labelOrType, entryName string) (*storage.PropertyTypeConstraint, storage.ConstraintContractEntry, bool, error) {
	property, typeName, ok := parseConstraintTypePredicate(ctx, entryText)
	if !ok {
		return nil, storage.ConstraintContractEntry{}, false, nil
	}
	expectedType, err := parsePropertyType(typeName)
	if err != nil {
		return nil, storage.ConstraintContractEntry{}, true, err
	}
	kind := storage.ConstraintContractKindPrimitiveNode
	if entityType == storage.ConstraintEntityRelationship {
		kind = storage.ConstraintContractKindPrimitiveRelationship
	}
	constraint := &storage.PropertyTypeConstraint{Name: entryName, EntityType: entityType, Label: labelOrType, Property: property, ExpectedType: expectedType}
	return constraint, storage.ConstraintContractEntry{Kind: kind, PrimitiveType: string(storage.ConstraintPropertyType), Property: property, Properties: []string{property}, ExpectedType: string(expectedType), Expression: entryText}, true, nil
}
