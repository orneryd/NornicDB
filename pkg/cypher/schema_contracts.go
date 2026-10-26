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
	body        braceView
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

func parseCurlySection(view braceView) (inside, rest braceView, ok bool) {
	view = view.trim()
	if view.empty() || !cyphertext.IsOpenBrace(view.text()[0]) {
		return braceView{}, view, false
	}
	closing, complete := view.close(0)
	if !complete {
		return braceView{}, view, false
	}
	return view.window(1, closing), view.window(closing+1, len(view.text())).trim(), true
}

func (e *StorageExecutor) parseCreateConstraintContractDDL(view braceView) (parsedCreateConstraintContractDDL, error) {
	qView := view.trim()
	q := qView.text()
	if q == "" {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaEmptyStatement(), nil)
	}
	prefixPos := keywordIndexFrom(q, "CREATE CONSTRAINT", 0, defaultKeywordScanOpts(qView))
	if prefixPos != 0 {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE CONSTRAINT")
	if !ok {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("prefix"), nil)
	}
	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts(qView))
	if forPos < 0 {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaMissingKeyword("FOR"), nil)
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("FOR"), nil)
	}
	reqPos := keywordIndexFrom(q, "REQUIRE", forEnd, defaultKeywordScanOpts(qView))
	if reqPos < 0 {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaMissingKeyword("REQUIRE"), nil)
	}
	reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
	if !ok {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidKeyword("REQUIRE"), nil)
	}

	name, err := parseOptionalDDLName(qView.window(prefixEnd, forPos))
	if err != nil {
		return parsedCreateConstraintContractDDL{}, err
	}

	entityType, variable, labelOrType, ok := parseConstraintContractForClause(q[forEnd:reqPos])
	if !ok {
		return parsedCreateConstraintContractDDL{}, localizedError(localization.CypherSchemaInvalidPattern("FOR"), nil)
	}

	body, tail, ok := parseCurlySection(qView.window(reqEnd, len(q)))
	if !ok || !tail.empty() {
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

func (e *StorageExecutor) executeCreateConstraintContract(ctx context.Context, cypher string, ifNotExists bool) (*ExecuteResult, bool, error) {
	view := statementBraceView(ctx, cypher)
	parsed, err := e.parseCreateConstraintContractDDL(view)
	if err != nil {
		return nil, false, nil
	}
	return e.createConstraintContractForTarget(ctx, cypher, parsed.name, parsed.entityType, parsed.variable, parsed.labelOrType, parsed.body, ifNotExists)
}

func (e *StorageExecutor) createConstraintContractForTarget(ctx context.Context, definition, name string, entityType storage.ConstraintEntityType, variable, labelOrType string, body braceView, ifNotExists bool) (*ExecuteResult, bool, error) {
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
		return nil, true, err
	}
	// See addSchemaConstraint: new temporal entries are authoritative on an
	// empty label (#1126).
	if err := storage.MarkNewTemporalConstraintsForEngine(e.storage, schema, newTemporal); err != nil {
		return nil, true, err
	}
	// Fill the equality indexes the contract's UNIQUE / NODE KEY entries own,
	// as a standalone constraint's DDL does (addSchemaConstraint): until an
	// owned index is filled, validation and seeks fall back to scanning the
	// label.
	for _, name := range newNames {
		if index, owned := schema.ConstraintPropertyIndex(name); owned {
			if err := e.backfillCompositeIndex(index.Name, index.Label, index.Properties); err != nil {
				return nil, true, err
			}
		}
	}
	// Like a standalone UNIQUE constraint, a contract's IS UNIQUE entries need
	// their value cache built and marked complete; without it every write to
	// the label falls back to scanning all of its nodes (#1120).
	if err := storage.RefreshUniqueConstraintValuesForConstraintsForEngine(e.storage, schema, compiledConstraints); err != nil {
		return nil, true, err
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, true, nil
}

func splitConstraintContractEntries(view braceView) ([]braceView, error) {
	body := view.text()
	var entries []braceView
	start, brackets, parens := 0, 0, 0
	appendEntry := func(end int) {
		entry := view.window(start, end).trim()
		if !entry.empty() {
			entries = append(entries, entry)
		}
	}
	for at := 0; at < len(body); at++ {
		if end, opaque := skipCypherLexicalToken(body, at); opaque {
			at = end - 1
			continue
		}
		switch body[at] {
		case '{':
			close, complete := view.close(at)
			if !complete {
				return nil, localizedError(localization.CypherSchemaMalformedRequireBlock(), nil)
			}
			at = close
		case '}':
			return nil, localizedError(localization.CypherSchemaMalformedRequireBlock(), nil)
		case '[':
			brackets++
		case ']':
			if brackets > 0 {
				brackets--
			}
		case '(':
			parens++
		case ')':
			if parens > 0 {
				parens--
			}
		case '\n', ';':
			if brackets == 0 && parens == 0 {
				appendEntry(at)
				start = at + 1
			}
		}
	}
	if brackets != 0 || parens != 0 {
		return nil, localizedError(localization.CypherSchemaMalformedRequireBlock(), nil)
	}
	appendEntry(len(body))
	return entries, nil
}

func isNestedConstraintContractEntry(view braceView) bool {
	view = view.trim()
	if !matchKeywordAt(view.text(), 0, "FOR") {
		return false
	}
	require := keywordIndex(view, "REQUIRE")
	if require < 0 {
		return false
	}
	opening := queryGapEnd(view.text(), require+len("REQUIRE"))
	_, found := view.at(opening)
	return found
}

func nestedConstraintContractEntryError(entryText string) error {
	return localizedError(localization.CypherSchemaNestedContractEntryUnsupported(strings.TrimSpace(entryText)), nil)
}

func (e *StorageExecutor) parseConstraintContractEntry(rawEntry braceView, variable string, entityType storage.ConstraintEntityType, labelOrType, contractName string, index int) (storage.ConstraintContractEntry, *storage.Constraint, *storage.PropertyTypeConstraint, error) {
	entryView := rawEntry.trim()
	entryText := entryView.text()
	if isNestedConstraintContractEntry(entryView) {
		return storage.ConstraintContractEntry{}, nil, nil, nestedConstraintContractEntryError(entryText)
	}

	entryName := storage.ConstraintContractEntryName(contractName, index)
	if compiledConstraint, contractEntry, ok, err := e.parseConstraintContractPrimitive(entryView, variable, entityType, labelOrType, entryName); ok || err != nil {
		return contractEntry, compiledConstraint, nil, err
	}
	if compiledType, contractEntry, ok, err := e.parseConstraintContractPropertyType(entryView, variable, entityType, labelOrType, entryName); ok || err != nil {
		return contractEntry, nil, compiledType, err
	}

	kind := storage.ConstraintContractKindBooleanNode
	if entityType == storage.ConstraintEntityRelationship {
		kind = storage.ConstraintContractKindBooleanRelationship
	}
	return storage.ConstraintContractEntry{Kind: kind, Expression: entryText}, nil, nil, nil
}

func (e *StorageExecutor) parseConstraintContractPrimitive(view braceView, variable string, entityType storage.ConstraintEntityType, labelOrType, entryName string) (*storage.Constraint, storage.ConstraintContractEntry, bool, error) {
	view = view.trim()
	entryText := view.text()
	if kind, property, ok := parseConstraintPredicate(view); ok {
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
		if properties, ok := e.parseNodeKeyPropertyList(view); ok {
			if len(properties) == 0 {
				return nil, storage.ConstraintContractEntry{}, true, localizedError(localization.CypherSchemaConstraintPropertiesRequired("NODE KEY"), nil)
			}
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintNodeKey, EntityType: entityType, Label: labelOrType, Properties: properties}
			return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintNodeKey), Properties: properties, Expression: entryText}, true, nil
		}
		if properties, ok := e.parseTemporalConstraintPredicate(view); ok {
			if len(properties) < 3 {
				return nil, storage.ConstraintContractEntry{}, true, localizedError(localization.CypherSchemaTemporalNodeArityRequired(), nil)
			}
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintTemporal, EntityType: entityType, Label: labelOrType, Properties: properties}
			return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintTemporal), Properties: properties, Expression: entryText}, true, nil
		}
	}

	if entityType == storage.ConstraintEntityRelationship {
		inside, rest, ok := extractParenSection(strings.TrimSpace(entryText))
		if ok {
			restView := view.window(len(view.text())-len(rest), len(view.text())).trim()
			rest = restView.text()
			if idx := keywordIndexFrom(rest, "IS RELATIONSHIP KEY", 0, defaultKeywordScanOpts(restView)); idx == 0 {
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

func (e *StorageExecutor) parseConstraintContractPropertyType(view braceView, variable string, entityType storage.ConstraintEntityType, labelOrType, entryName string) (*storage.PropertyTypeConstraint, storage.ConstraintContractEntry, bool, error) {
	view = view.trim()
	entryText := view.text()
	property, typeName, ok := parseConstraintTypePredicate(view)
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
