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

func parseConstraintContractForClause(forClause string) (entityType storage.ConstraintEntityType, variable string, labelOrType string, ok bool) {
	forClause = strings.TrimSpace(forClause)
	if forClause == "" {
		return "", "", "", false
	}

	if strings.Contains(forClause, "[") {
		bracketStart := strings.Index(forClause, "[")
		if bracketStart < 0 {
			return "", "", "", false
		}
		inside, rest, ok := extractBracketSection(forClause[bracketStart:])
		if !ok {
			return "", "", "", false
		}
		prefix := strings.TrimSpace(forClause[:bracketStart])
		suffix := strings.TrimSpace(rest)
		if !strings.Contains(prefix, "(") || !strings.Contains(suffix, "(") {
			return "", "", "", false
		}
		colon := strings.Index(inside, ":")
		if colon < 0 {
			return "", "", "", false
		}
		variable = normalizeIdentifierToken(strings.TrimSpace(inside[:colon]))
		labelOrType = normalizeIdentifierToken(strings.TrimSpace(inside[colon+1:]))
		if labelOrType == "" {
			return "", "", "", false
		}
		return storage.ConstraintEntityRelationship, variable, labelOrType, true
	}

	inside, rest, ok := extractParenSection(forClause)
	if !ok || strings.TrimSpace(rest) != "" {
		return "", "", "", false
	}
	colon := strings.Index(inside, ":")
	if colon < 0 {
		return "", "", "", false
	}
	variable = normalizeIdentifierToken(strings.TrimSpace(inside[:colon]))
	labelOrType = normalizeIdentifierToken(strings.TrimSpace(inside[colon+1:]))
	if labelOrType == "" {
		return "", "", "", false
	}
	return storage.ConstraintEntityNode, variable, labelOrType, true
}

func parseCurlySection(s string) (inside string, rest string, ok bool) {
	s = strings.TrimSpace(s)
	if s == "" || s[0] != '{' {
		return "", s, false
	}
	depth := 0
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if quote != 0 {
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == '{' {
			depth++
			continue
		}
		if ch == '}' {
			depth--
			if depth == 0 {
				return s[1:i], strings.TrimSpace(s[i+1:]), true
			}
		}
	}
	return "", s, false
}

func (e *StorageExecutor) parseCreateConstraintContractDDL(cypher string) (parsedCreateConstraintContractDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaEmptyStatement(), nil)
	}
	prefixPos := keywordIndexFrom(q, "CREATE CONSTRAINT", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE CONSTRAINT")
	if !ok {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}
	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaMissingKeyword("FOR"), nil)
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("FOR"), nil)
	}
	reqPos := keywordIndexFrom(q, "REQUIRE", forEnd, defaultKeywordScanOpts())
	if reqPos < 0 {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaMissingKeyword("REQUIRE"), nil)
	}
	reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
	if !ok {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("REQUIRE"), nil)
	}

	name, err := parseOptionalDDLName(q[prefixEnd:forPos])
	if err != nil {
		return parsedCreateConstraintContractDDL{}, err
	}

	entityType, variable, labelOrType, ok := parseConstraintContractForClause(q[forEnd:reqPos])
	if !ok {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidPattern("FOR"), nil)
	}

	body, tail, ok := parseCurlySection(q[reqEnd:])
	if !ok || tail != "" {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidClause("REQUIRE block"), nil)
	}

	if name == "" {
		name = fmt.Sprintf("constraint_%s_contract", strings.ToLower(labelOrType))
	}
	return parsedCreateConstraintContractDDL{
		name:        name,
		entityType:  entityType,
		variable:    variable,
		labelOrType: labelOrType,
		body:        body,
	}, nil
}

func (e *StorageExecutor) executeCreateConstraintContract(ctx context.Context, cypher string, ifNotExists bool) (*ExecuteResult, bool, error) {
	parsed, err := e.parseCreateConstraintContractDDL(cypher)
	if err != nil {
		return nil, false, nil
	}
	return e.createConstraintContractForTarget(ctx, cypher, parsed.name, parsed.entityType, parsed.variable, parsed.labelOrType, parsed.body, ifNotExists)
}

func (e *StorageExecutor) createConstraintContractForTarget(ctx context.Context, definition, name string, entityType storage.ConstraintEntityType, variable, labelOrType, body string, ifNotExists bool) (*ExecuteResult, bool, error) {
	entriesRaw, err := splitConstraintContractEntries(body)
	if err != nil {
		return nil, true, err
	}
	if len(entriesRaw) == 0 {
		return nil, true, localizedError(localization.CypherSchemaContractEntryRequired(), nil)
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
		entry, compiledConstraint, compiledType, err := e.parseConstraintContractEntry(rawEntry, variable, entityType, labelOrType, name, idx)
		if err != nil {
			return nil, true, err
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
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, true, err
		}
	}
	for _, constraint := range compiledTypes {
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, true, err
		}
	}
	if err := storage.ValidateConstraintContractOnCreationForEngine(e.storage, contract); err != nil {
		return nil, true, err
	}
	if err := e.storage.GetSchema().AddConstraintContractBundle(contract, compiledConstraints, compiledTypes, ifNotExists); err != nil {
		return nil, true, err
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, true, nil
}

func splitConstraintContractEntries(body string) ([]string, error) {
	entries := make([]string, 0)
	var current strings.Builder
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0
	quote := rune(0)

	flush := func() {
		entry := strings.TrimSpace(current.String())
		if entry != "" {
			entries = append(entries, entry)
		}
		current.Reset()
	}

	for _, ch := range body {
		switch {
		case quote != 0:
			current.WriteRune(ch)
			if ch == quote {
				quote = 0
			}
		case ch == '\'' || ch == '"':
			quote = ch
			current.WriteRune(ch)
		case ch == '{':
			braceDepth++
			current.WriteRune(ch)
		case ch == '}':
			if braceDepth == 0 {
				return nil, localizedError(localization.CypherSchemaMalformedRequireBlock(), nil)
			}
			braceDepth--
			current.WriteRune(ch)
		case ch == '[':
			bracketDepth++
			current.WriteRune(ch)
		case ch == ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			current.WriteRune(ch)
		case ch == '(':
			parenDepth++
			current.WriteRune(ch)
		case ch == ')':
			if parenDepth > 0 {
				parenDepth--
			}
			current.WriteRune(ch)
		case ch == '\n' || ch == ';':
			if braceDepth == 0 && bracketDepth == 0 && parenDepth == 0 {
				flush()
				continue
			}
			current.WriteRune(ch)
		default:
			current.WriteRune(ch)
		}
	}
	if quote != 0 || braceDepth != 0 || bracketDepth != 0 {
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

func (e *StorageExecutor) parseConstraintContractEntry(rawEntry, variable string, entityType storage.ConstraintEntityType, labelOrType, contractName string, index int) (storage.ConstraintContractEntry, *storage.Constraint, *storage.PropertyTypeConstraint, error) {
	entryText := strings.TrimSpace(rawEntry)
	if isNestedConstraintContractEntry(entryText) {
		return storage.ConstraintContractEntry{}, nil, nil, nestedConstraintContractEntryError(entryText)
	}

	entryName := fmt.Sprintf("%s__entry_%02d", contractName, index+1)
	if compiledConstraint, contractEntry, ok, err := e.parseConstraintContractPrimitive(entryText, variable, entityType, labelOrType, entryName); ok || err != nil {
		return contractEntry, compiledConstraint, nil, err
	}
	if compiledType, contractEntry, ok, err := e.parseConstraintContractPropertyType(entryText, variable, entityType, labelOrType, entryName); ok || err != nil {
		return contractEntry, nil, compiledType, err
	}

	kind := storage.ConstraintContractKindBooleanNode
	if entityType == storage.ConstraintEntityRelationship {
		kind = storage.ConstraintContractKindBooleanRelationship
	}
	return storage.ConstraintContractEntry{Kind: kind, Expression: entryText}, nil, nil, nil
}

func (e *StorageExecutor) parseConstraintContractPrimitive(entryText, variable string, entityType storage.ConstraintEntityType, labelOrType, entryName string) (*storage.Constraint, storage.ConstraintContractEntry, bool, error) {
	if kind, property, ok := parseConstraintPredicate(entryText); ok {
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
		if properties, ok := e.parseNodeKeyPropertyList(entryText); ok {
			if len(properties) == 0 {
				return nil, storage.ConstraintContractEntry{}, true, localizedError(localization.CypherSchemaConstraintPropertiesRequired("NODE KEY"), nil)
			}
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintNodeKey, EntityType: entityType, Label: labelOrType, Properties: properties}
			return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintNodeKey), Properties: properties, Expression: entryText}, true, nil
		}
		if properties, ok := e.parseTemporalConstraintPredicate(entryText); ok {
			if len(properties) != 3 {
				return nil, storage.ConstraintContractEntry{}, true, localizedError(localization.CypherSchemaTemporalNodeArityRequired(), nil)
			}
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintTemporal, EntityType: entityType, Label: labelOrType, Properties: properties}
			return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintTemporal), Properties: properties, Expression: entryText}, true, nil
		}
	}

	if entityType == storage.ConstraintEntityRelationship {
		inside, rest, ok := extractParenSection(strings.TrimSpace(entryText))
		if ok {
			rest = strings.TrimSpace(rest)
			if idx := keywordIndexFrom(rest, "IS RELATIONSHIP KEY", 0, defaultKeywordScanOpts()); idx == 0 {
				end, spanOK := keywordSpanAt(rest, 0, "IS RELATIONSHIP KEY")
				if spanOK && strings.TrimSpace(rest[end:]) == "" {
					properties := e.parseConstraintProperties(inside)
					if len(properties) == 0 {
						return nil, storage.ConstraintContractEntry{}, true, localizedError(localization.CypherSchemaConstraintPropertiesRequired("RELATIONSHIP KEY"), nil)
					}
					constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintRelationshipKey, EntityType: entityType, Label: labelOrType, Properties: properties}
					return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveRelationship, PrimitiveType: string(storage.ConstraintRelationshipKey), Properties: properties, Expression: entryText}, true, nil
				}
			}
		}
	}

	return nil, storage.ConstraintContractEntry{}, false, nil
}

func (e *StorageExecutor) parseConstraintContractPropertyType(entryText, variable string, entityType storage.ConstraintEntityType, labelOrType, entryName string) (*storage.PropertyTypeConstraint, storage.ConstraintContractEntry, bool, error) {
	property, typeName, ok := parseConstraintTypePredicate(entryText)
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
