package cypher

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"strings"
)

// preparedSchemaMutation owns one native schema declaration. Preparation reads
// and validates its arguments; only run validates stored data and applies effects.
// Metadata consumes the declared no-record schema without running that continuation.
type preparedSchemaMutation struct {
	run func(context.Context) (*ExecuteResult, error)
}

func (e *StorageExecutor) prepareSchemaCommand(ctx context.Context, source string) (*preparedSchemaMutation, error) {
	switch classifyStatement(source).schemaCommand {
	case schemaCreateConstraint:
		return e.prepareCreateConstraint(ctx, source)
	case schemaDropConstraint:
		return e.prepareDropConstraint(ctx, source)
	case schemaDropIndex:
		return e.prepareDropIndex(ctx, source)
	case schemaCreateFulltextIndex:
		return e.prepareCreateFulltextIndex(ctx, source)
	case schemaCreateVectorIndex, schemaCreateRangeIndex, schemaCreateIndex:
		return e.prepareCreateIndex(ctx, source)
	case schemaCreateTypedPropertyIndex:
		return e.prepareCreateTypedPropertyIndex(ctx, source)
	case schemaCreateLookupIndex:
		return e.prepareCreateLookupIndex(ctx, source)
	default:
		return nil, localizedError(localization.CypherSchemaUnknownCommand(source), nil)
	}
}
func (e *StorageExecutor) prepareCreateConstraint(ctx context.Context, cypher string) (*preparedSchemaMutation, error) {
	onPosition := keywordIndexFrom(cypher, "ON", 0, defaultKeywordScanOpts())
	forPosition := keywordIndexFrom(cypher, "FOR", 0, defaultKeywordScanOpts())
	if onPosition >= 0 && (forPosition < 0 || onPosition < forPosition) {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax("CREATE CONSTRAINT"), nil)
	}
	// Every spelling of a uniqueness or key requirement becomes the one the
	// shapes below read (canonicalConstraintRequirement).
	cypher, err := e.canonicalConstraintRequirement(cypher)
	if err != nil {
		return nil, err
	}
	if parsed, err := e.parseCreateConstraintForRequireDDL(cypher); err == nil {
		if kind, properties, ok := e.parseRelationshipKeyOrCompositeUniquePredicate(parsed.requireExpr); ok && kind == "unique" {
			seen := make(map[string]struct{}, len(properties))
			for _, property := range properties {
				if _, duplicate := seen[property]; duplicate {
					return nil, newSemanticError("Neo.ClientError.Schema.RepeatedPropertyInCompositeSchema", "RepeatedPropertyInCompositeSchema",
						"A composite schema cannot contain the same property more than once: "+property)
				}
				seen[property] = struct{}{}
			}
		}
	}
	// Detect IF NOT EXISTS to pass through to AddConstraint for duplicate-schema handling.
	ifNotExists := strings.Contains(upperASCII(cypher), "IF NOT EXISTS")
	if command, handled, err := e.prepareCreateConstraintContract(ctx, cypher, ifNotExists); handled {
		return command, err
	}

	if parsed, err := e.parseCreateConstraintNodeKeyDDL(cypher); err == nil {
		if len(parsed.properties) == 0 {
			return nil, localizedError(localization.CypherSchemaConstraintPropertiesRequired("NODE KEY"), nil)
		}
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_node_key", parsed.label, strings.Join(parsed.properties, "_"))
		}

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintNodeKey,
			Label:      parsed.label,
			Properties: parsed.properties,
		}

		return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
			if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
				return nil, err
			}
			if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}}, nil
	}

	if parsed, err := e.parseCreateConstraintTemporalDDL(cypher); err == nil {
		if parsed.isRelationship {
			if len(parsed.properties) < 3 {
				return nil, localizedError(localization.CypherSchemaTemporalRelationshipArityRequired(), nil)
			}
			constraintName := parsed.name
			if constraintName == "" {
				constraintName = fmt.Sprintf("constraint_%s_%s_temporal", parsed.label, strings.Join(parsed.properties, "_"))
			}
			constraint := storage.Constraint{
				Name:       constraintName,
				Type:       storage.ConstraintTemporal,
				EntityType: storage.ConstraintEntityRelationship,
				Label:      parsed.label,
				Properties: parsed.properties,
			}
			return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
				if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
					return nil, err
				}
				if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
					return nil, err
				}
				return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
			}}, nil
		}

		// Like relationships: (key..., valid_from, valid_to) with a 1..n property grouping key.
		if len(parsed.properties) < 3 {
			return nil, localizedError(localization.CypherSchemaTemporalNodeArityRequired(), nil)
		}
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_temporal", parsed.label, strings.Join(parsed.properties, "_"))
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintTemporal,
			Label:      parsed.label,
			Properties: parsed.properties,
		}
		return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
			if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
				return nil, err
			}
			if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}}, nil
	}

	if parsed, err := e.parseCreateConstraintDomainDDL(cypher); err == nil {
		allowedValues, err := parseDomainValueList(parsed.allowedRaw)
		if err != nil {
			return nil, localizedError(localization.CypherSchemaDomainValueListInvalid(err), err)
		}
		if len(allowedValues) == 0 {
			return nil, localizedError(localization.CypherSchemaDomainAllowedValueRequired(), nil)
		}
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_domain", parsed.label, parsed.property)
		}
		constraint := storage.Constraint{
			Name:          constraintName,
			Type:          storage.ConstraintDomain,
			Label:         parsed.label,
			Properties:    []string{parsed.property},
			AllowedValues: allowedValues,
		}
		if parsed.isRelationship {
			constraint.EntityType = storage.ConstraintEntityRelationship
		}
		return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
			if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
				return nil, err
			}
			if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}}, nil
	}

	if parsed, err := e.parseCreateConstraintSimplePropertyDDL(cypher); err == nil {
		if parsed.kind == "unique" {
			if parsed.isRelationship {
				constraintName := parsed.name
				if constraintName == "" {
					constraintName = fmt.Sprintf("constraint_%s_%s_unique", parsed.label, parsed.property)
				}
				constraint := storage.Constraint{
					Name:       constraintName,
					Type:       storage.ConstraintUnique,
					EntityType: storage.ConstraintEntityRelationship,
					Label:      parsed.label,
					Properties: []string{parsed.property},
				}
				return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
					if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
						return nil, err
					}
					if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
						return nil, err
					}
					return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
				}}, nil
			}

			constraintName := parsed.name
			if constraintName == "" {
				constraintName = fmt.Sprintf("constraint_%s_%s", parsed.label, parsed.property)
			}
			constraint := storage.Constraint{
				Name:       constraintName,
				Type:       storage.ConstraintUnique,
				Label:      parsed.label,
				Properties: []string{parsed.property},
			}
			return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
				if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
					return nil, err
				}
				schema := e.storage.GetSchema()
				if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
					return nil, err
				}
				if err := storage.RefreshUniqueConstraintValuesForEngine(e.storage, schema); err != nil {
					return nil, err
				}
				return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
			}}, nil
		}

		if parsed.kind == "exists" {
			constraintName := parsed.name
			if constraintName == "" {
				suffix := "exists"
				if parsed.isRelationship {
					constraintName = fmt.Sprintf("constraint_%s_%s_%s", parsed.label, parsed.property, suffix)
				} else {
					constraintName = fmt.Sprintf("constraint_%s_%s_%s", parsed.label, parsed.property, suffix)
				}
			}
			constraint := storage.Constraint{
				Name:       constraintName,
				Type:       storage.ConstraintExists,
				Label:      parsed.label,
				Properties: []string{parsed.property},
			}
			if parsed.isRelationship {
				constraint.EntityType = storage.ConstraintEntityRelationship
			}
			return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
				if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
					return nil, err
				}
				if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
					return nil, err
				}
				return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
			}}, nil
		}
	}

	if parsed, err := e.parseCreateConstraintTypeDDL(cypher); err == nil {
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_type", parsed.label, parsed.property)
		}
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			Label:        parsed.label,
			Property:     parsed.property,
			ExpectedType: parsed.expectedType,
		}
		opts := storage.PropertyTypeConstraintOptions{IfNotExists: ifNotExists}
		if parsed.isRelationship {
			ptc.EntityType = storage.ConstraintEntityRelationship
			opts.EntityType = storage.ConstraintEntityRelationship
		}
		return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
			if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
				return nil, err
			}
			if err := e.storage.GetSchema().AddPropertyTypeConstraint(constraintName, parsed.label, parsed.property, parsed.expectedType, opts); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}}, nil
	} else if startsWithKeywordFold(strings.TrimSpace(cypher), "CREATE CONSTRAINT") &&
		(strings.Contains(upperASCII(cypher), " IS ::") || strings.Contains(upperASCII(cypher), " IS TYPED")) &&
		strings.Contains(upperASCII(err.Error()), "UNSUPPORTED PROPERTY TYPE") {
		return nil, err
	}

	// Single-property UNIQUE/EXISTS branches are handled by parseCreateConstraintSimplePropertyDDL.

	// =========================================================================
	// Relationship constraint patterns
	// =========================================================================

	if parsed, err := e.parseCreateConstraintCardinalityDDL(cypher); err == nil {
		constraintName := parsed.name
		if constraintName == "" {
			directionSuffix := "outgoing"
			if parsed.direction == "INCOMING" {
				directionSuffix = "incoming"
			}
			constraintName = fmt.Sprintf("constraint_%s_max_%s_%d", parsed.relType, directionSuffix, parsed.maxCount)
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintCardinality,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      parsed.relType,
			MaxCount:   parsed.maxCount,
			Direction:  parsed.direction,
		}
		return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
			if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
				return nil, err
			}
			if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}}, nil
	} else if startsWithKeywordFold(strings.TrimSpace(cypher), "CREATE CONSTRAINT") &&
		strings.Contains(upperASCII(cypher), "REQUIRE MAX COUNT") {
		return nil, err
	}

	if parsed, err := e.parseCreateConstraintPolicyDDL(cypher); err == nil {
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_%s_%s", parsed.sourceLabel, parsed.relType, parsed.targetLabel, lowerASCII(parsed.policyMode))
		}
		constraint := storage.Constraint{
			Name:        constraintName,
			Type:        storage.ConstraintPolicy,
			EntityType:  storage.ConstraintEntityRelationship,
			Label:       parsed.relType,
			SourceLabel: parsed.sourceLabel,
			TargetLabel: parsed.targetLabel,
			PolicyMode:  parsed.policyMode,
		}
		return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
			if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
				return nil, err
			}
			if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}}, nil
	} else if startsWithKeywordFold(strings.TrimSpace(cypher), "CREATE CONSTRAINT") &&
		(strings.Contains(upperASCII(cypher), "REQUIRE ALLOWED") || strings.Contains(upperASCII(cypher), "REQUIRE DISALLOWED")) {
		return nil, err
	}

	// Relationship temporal/domain branches are handled by parseCreateConstraintTemporalDDL and parseCreateConstraintDomainDDL.

	if parsed, err := e.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL(cypher); err == nil {
		if len(parsed.properties) == 0 {
			if parsed.kind == "rel_key" {
				return nil, localizedError(localization.CypherSchemaConstraintPropertiesRequired("RELATIONSHIP KEY"), nil)
			}
			return nil, localizedError(localization.CypherSchemaConstraintPropertiesRequired("UNIQUE"), nil)
		}

		constraint := storage.Constraint{
			Label:      parsed.label,
			Properties: parsed.properties,
		}
		if parsed.isRelationship {
			constraint.EntityType = storage.ConstraintEntityRelationship
		}
		if parsed.kind == "rel_key" {
			constraint.Type = storage.ConstraintRelationshipKey
			constraint.Name = parsed.name
			if constraint.Name == "" {
				constraint.Name = fmt.Sprintf("constraint_%s_%s_rel_key", parsed.label, strings.Join(parsed.properties, "_"))
			}
		} else {
			constraint.Type = storage.ConstraintUnique
			constraint.Name = parsed.name
			if constraint.Name == "" {
				constraint.Name = fmt.Sprintf("constraint_%s_%s_unique", parsed.label, strings.Join(parsed.properties, "_"))
			}
		}

		return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
			if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
				return nil, err
			}
			if err := e.addSchemaConstraint(ctx, constraint, ifNotExists); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}}, nil
	}

	// Relationship single-property UNIQUE/EXISTS branches are handled by parseCreateConstraintSimplePropertyDDL.

	// Relationship property type branches are handled by parseCreateConstraintTypeDDL.

	return nil, localizedError(localization.CypherSchemaInvalidSyntax("CREATE CONSTRAINT"), nil)
}

func (e *StorageExecutor) prepareDropIndex(ctx context.Context, cypher string) (*preparedSchemaMutation, error) {
	if isCompositeRoot(e.storage) {
		return nil, localizedError(localization.CypherSchemaCompositeDDLNotAllowed(), nil)
	}

	rest := strings.TrimSpace(strings.TrimSpace(cypher)[len("DROP INDEX"):])
	if rest == "" {
		return nil, localizedError(localization.CypherSchemaNameRequired("DROP INDEX", "index"), nil)
	}
	name, ifExists, ok := parseDropSchemaName(rest)
	if !ok {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherSchemaInvalidSyntax("DROP INDEX"))
	}

	return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
		if err := e.dropIndexByName(name, ifExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}}, nil
}

func (e *StorageExecutor) prepareDropConstraint(ctx context.Context, cypher string) (*preparedSchemaMutation, error) {
	trimmed := strings.TrimSpace(cypher)
	upper := upperASCII(trimmed)
	if !strings.HasPrefix(upper, "DROP CONSTRAINT") {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax("DROP CONSTRAINT"), nil)
	}

	rest := strings.TrimSpace(trimmed[len("DROP CONSTRAINT"):])
	if rest == "" {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax("DROP CONSTRAINT"), nil)
	}

	name, ifExists, ok := parseDropSchemaName(rest)
	if !ok {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherSchemaInvalidSyntax("DROP CONSTRAINT"))
	}

	return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
		if err := e.storage.GetSchema().DropConstraint(name); err != nil {
			var missing *localization.LocalizedError
			if errors.As(err, &missing) && missing.Message.ID == localization.MessageStorageSchemaConstraintNotFound {
				if ifExists {
					return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
				}
				return nil, newSemanticError("Neo.DatabaseError.Schema.ConstraintDropFailed", "MissingConstraint", err.Error())
			}
			return nil, err
		}

		if e.cache != nil {
			e.cache.Invalidate()
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}}, nil
}

func (e *StorageExecutor) prepareCreateFulltextIndex(ctx context.Context, cypher string) (*preparedSchemaMutation, error) {
	parsed, err := e.parseCreateFulltextIndexDDL(cypher)
	if err != nil {
		return nil, err
	}

	entityType := storage.ConstraintEntityNode
	targets := parsed.labels
	if parsed.isRelationship {
		entityType = storage.ConstraintEntityRelationship
		targets = parsed.relationshipTypes
	}
	return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
		schema := e.storage.GetSchema()
		if schema == nil {
			return nil, localizedError(localization.CypherSchemaManagerUnavailable(), nil)
		}
		create, err := e.admitIndexCreationForTargets(cypher, parsed.indexName, "FULLTEXT", targets, parsed.properties, entityType)
		if err != nil {
			return nil, err
		}
		if !create {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}

		if parsed.isRelationship {
			if err := schema.AddFulltextIndex(parsed.indexName, parsed.relationshipTypes, parsed.properties, storage.ConstraintEntityRelationship); err != nil {
				return nil, localizedError(localization.CypherSchemaAddFulltextRelationshipIndexFailed(err), err)
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}

		if err := schema.AddFulltextIndex(parsed.indexName, parsed.labels, parsed.properties, storage.ConstraintEntityNode); err != nil {
			return nil, localizedError(localization.CypherSchemaAddFulltextIndexFailed(err), err)
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}}, nil
}

func (e *StorageExecutor) prepareCreateIndex(ctx context.Context, cypher string) (*preparedSchemaMutation, error) {
	prefix := "CREATE INDEX"
	query := strings.TrimSpace(cypher)
	vector := false
	if _, ok := keywordSpanAt(query, 0, "CREATE VECTOR INDEX"); ok {
		prefix, vector = "CREATE VECTOR INDEX", true
	} else if _, ok := keywordSpanAt(query, 0, "CREATE RANGE INDEX"); ok {
		prefix = "CREATE RANGE INDEX"
	}
	parsed, err := e.parseCreateIndexDDL(cypher, prefix)
	if err != nil {
		if !vector {
			// This is the supported Neo4j ON :Label(properties) grammar,
			// retaining its property-index path and existing error contract.
			if legacy, legacyErr := e.parseCreateIndexLegacyDDL(cypher, prefix); legacyErr == nil {
				name := legacy.indexName
				if name == "" {
					name = fmt.Sprintf("index_%s_%s", legacy.label, strings.Join(legacy.properties, "_"))
				}
				return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
					if err := e.addPropertyIndex(ctx, name, legacy.label, legacy.properties); err != nil {
						return nil, err
					}
					return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
				}}, nil
			}
			prefix = "CREATE INDEX"
		}
		return nil, localizedError(localization.CypherSchemaInvalidSyntax(prefix), nil)
	}
	if vector && (len(parsed.properties) != 1 || parsed.indexName == "" ||
		(!parsed.isRelationship && parsed.label == "") ||
		(parsed.isRelationship && parsed.relationshipType == "")) {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax(prefix), nil)
	}

	entityType := storage.ConstraintEntityNode
	label := parsed.label
	if parsed.isRelationship {
		entityType, label = storage.ConstraintEntityRelationship, parsed.relationshipType
	}
	name := parsed.indexName
	if name == "" {
		name = fmt.Sprintf("index_%s_%s", label, strings.Join(parsed.properties, "_"))
	}
	kind := "RANGE"
	dimensions, similarity := 0, ""
	if vector {
		kind = "VECTOR"
		dimensions, similarity = parseVectorOptions(parsed.optionsClause, e.GetDefaultEmbeddingDimensions(), "cosine")
	}
	return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
		create, err := e.admitIndexCreation(cypher, name, kind, label, parsed.properties, entityType)
		if err != nil {
			return nil, err
		}
		if create {
			if vector {
				property := parsed.properties[0]
				// Persist operator intent even with vector search disabled. Only
				// node indexes register an in-memory vector space, after commit.
				if err := e.storage.GetSchema().AddVectorIndex(name, label, property, dimensions, similarity, entityType); err != nil {
					return nil, err
				}
				if svc := e.search(); !parsed.isRelationship && (svc == nil || svc.VectorEnabled()) {
					if err := e.afterCommit(ctx, func() error {
						e.registerVectorSpace(name, label, property, dimensions, similarity)
						return nil
					}); err != nil {
						return nil, err
					}
				}
			} else if parsed.isRelationship {
				if err := e.storage.GetSchema().AddRangeIndexForEntity(name, label, parsed.properties, entityType); err != nil {
					return nil, err
				}
			} else if err := e.addPropertyIndex(ctx, name, label, parsed.properties); err != nil {
				return nil, err
			}
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}}, nil
}

func (e *StorageExecutor) prepareCreateTypedPropertyIndex(ctx context.Context, cypher string) (*preparedSchemaMutation, error) {
	kind := storage.IndexKindText
	if startsWithKeywords(cypher, "CREATE", "POINT INDEX") {
		kind = storage.IndexKindPoint
	}
	parsed, err := e.parseCreateIndexDDL(cypher, "CREATE "+string(kind)+" INDEX")
	if err != nil {
		return nil, err
	}
	if len(parsed.properties) != 1 {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax("CREATE "+string(kind)+" INDEX"), nil)
	}
	entityType := storage.ConstraintEntityNode
	label := parsed.label
	if parsed.isRelationship {
		entityType, label = storage.ConstraintEntityRelationship, parsed.relationshipType
	}
	name := parsed.indexName
	if name == "" {
		name = fmt.Sprintf("index_%s_%s_%s", label, parsed.properties[0], lowerASCII(string(kind)))
	}
	return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
		create, err := e.admitIndexCreation(cypher, name, string(kind), label, parsed.properties, entityType)
		if err != nil {
			return nil, err
		}
		if !create {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}
		if err := e.storage.GetSchema().AddTypedIndexForEntity(kind, name, label, parsed.properties, entityType); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}}, nil
}

func (e *StorageExecutor) prepareCreateLookupIndex(ctx context.Context, cypher string) (*preparedSchemaMutation, error) {
	name, ifNotExists, entityType, ok := parseCreateLookupIndex(cypher)
	if !ok {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherSchemaLookupIndexInvalid())
	}
	return &preparedSchemaMutation{run: func(ctx context.Context) (*ExecuteResult, error) {
		if err := e.storage.GetSchema().AddLookupIndex(name, entityType); err != nil {
			var localized *localization.LocalizedError
			if errors.As(err, &localized) {
				switch localized.Message.ID {
				case localization.MessageStorageSchemaConstraintAlreadyExists:
					if ifNotExists {
						return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
					}
					return nil, localizedStatusError("Neo.ClientError.Schema.ConstraintWithNameAlreadyExists", "ConstraintWithNameAlreadyExists", localized.Message)
				case localization.MessageStorageSchemaLookupIndexAlreadyExists:
					if ifNotExists {
						return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
					}
					return nil, localizedStatusError("Neo.ClientError.Schema.IndexAlreadyExists", "IndexAlreadyExists", localized.Message)
				case localization.MessageStorageSchemaIndexNameAlreadyExists:
					if ifNotExists {
						return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
					}
					return nil, localizedStatusError("Neo.ClientError.Schema.IndexWithNameAlreadyExists", "IndexWithNameAlreadyExists", localized.Message)
				}
			}
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}}, nil
}
