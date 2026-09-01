package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherSchemaCompositeDDLNotAllowed             MessageID = "cypherschema.composite_ddl_not_allowed"
	MessageCypherSchemaUnknownCommand                     MessageID = "cypherschema.unknown_command"
	MessageCypherSchemaFlushPendingWritesFailed           MessageID = "cypherschema.flush_pending_writes_failed"
	MessageCypherSchemaConstraintPropertiesRequired       MessageID = "cypherschema.constraint_properties_required"
	MessageCypherSchemaTemporalRelationshipArityRequired  MessageID = "cypherschema.temporal_relationship_arity_required"
	MessageCypherSchemaTemporalNodeArityRequired          MessageID = "cypherschema.temporal_node_arity_required"
	MessageCypherSchemaDomainValueListInvalid             MessageID = "cypherschema.domain_value_list_invalid"
	MessageCypherSchemaDomainAllowedValueRequired         MessageID = "cypherschema.domain_allowed_value_required"
	MessageCypherSchemaInvalidSyntax                      MessageID = "cypherschema.invalid_syntax"
	MessageCypherSchemaNameRequired                       MessageID = "cypherschema.name_required"
	MessageCypherSchemaRangeIndexSinglePropertyRequired   MessageID = "cypherschema.range_index_single_property_required"
	MessageCypherSchemaCreateRangeIndexFailed             MessageID = "cypherschema.create_range_index_failed"
	MessageCypherSchemaInvalidIdentifierSegment           MessageID = "cypherschema.invalid_identifier_segment"
	MessageCypherSchemaInvalidIfNotExistsClause           MessageID = "cypherschema.invalid_if_not_exists_clause"
	MessageCypherSchemaEmptyClause                        MessageID = "cypherschema.empty_clause"
	MessageCypherSchemaInvalidClause                      MessageID = "cypherschema.invalid_clause"
	MessageCypherSchemaMissingKeyword                     MessageID = "cypherschema.missing_keyword"
	MessageCypherSchemaInvalidKeyword                     MessageID = "cypherschema.invalid_keyword"
	MessageCypherSchemaInvalidTrailingSyntax              MessageID = "cypherschema.invalid_trailing_syntax"
	MessageCypherSchemaIndexPropertiesRequired            MessageID = "cypherschema.index_properties_required"
	MessageCypherSchemaIndexNameRequired                  MessageID = "cypherschema.index_name_required"
	MessageCypherSchemaFulltextPropertiesRequired         MessageID = "cypherschema.fulltext_properties_required"
	MessageCypherSchemaEmptyStatement                     MessageID = "cypherschema.empty_statement"
	MessageCypherSchemaInvalidPattern                     MessageID = "cypherschema.invalid_pattern"
	MessageCypherSchemaUnsupportedPredicate               MessageID = "cypherschema.unsupported_predicate"
	MessageCypherSchemaUnsupportedCreateConstraintShape   MessageID = "cypherschema.unsupported_create_constraint_shape"
	MessageCypherSchemaPositiveMaxCountRequired           MessageID = "cypherschema.positive_max_count_required"
	MessageCypherSchemaBackfillIndexFailed                MessageID = "cypherschema.backfill_index_failed"
	MessageCypherSchemaBackfillPropertyIndexFailed        MessageID = "cypherschema.backfill_property_index_failed"
	MessageCypherSchemaUnsupportedPropertyType            MessageID = "cypherschema.unsupported_property_type"
	MessageCypherSchemaManagerUnavailable                 MessageID = "cypherschema.manager_unavailable"
	MessageCypherSchemaAddFulltextRelationshipIndexFailed MessageID = "cypherschema.add_fulltext_relationship_index_failed"
	MessageCypherSchemaAddFulltextIndexFailed             MessageID = "cypherschema.add_fulltext_index_failed"
	MessageCypherSchemaUnterminatedString                 MessageID = "cypherschema.unterminated_string"
	MessageCypherSchemaContractEntryRequired              MessageID = "cypherschema.contract_entry_required"
	MessageCypherSchemaMalformedRequireBlock              MessageID = "cypherschema.malformed_require_block"
	MessageCypherSchemaNestedContractEntryUnsupported     MessageID = "cypherschema.nested_contract_entry_unsupported"
)

func cypherSchemaMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func CypherSchemaCompositeDDLNotAllowed() Message {
	const code = "Neo.ClientError.Statement.NotAllowed"
	return cypherSchemaMessage(MessageCypherSchemaCompositeDDLNotAllowed, code+": Schema DDL on composite databases requires a constituent target. Use USE <composite>.<alias> to target a specific constituent", map[string]any{"Code": code})
}

func CypherSchemaUnknownCommand(statement string) Message {
	return cypherSchemaMessage(MessageCypherSchemaUnknownCommand, "unknown schema command: "+statement, map[string]any{"Statement": statement})
}

func CypherSchemaFlushPendingWritesFailed(cause error) Message {
	return cypherSchemaMessage(MessageCypherSchemaFlushPendingWritesFailed, "flush pending async writes before schema DDL: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherSchemaConstraintPropertiesRequired(kind string) Message {
	return cypherSchemaMessage(MessageCypherSchemaConstraintPropertiesRequired, kind+" constraint requires properties", map[string]any{"Kind": kind})
}

func CypherSchemaTemporalRelationshipArityRequired() Message {
	return cypherSchemaMessage(MessageCypherSchemaTemporalRelationshipArityRequired, "TEMPORAL constraint requires at least 3 properties (key..., valid_from, valid_to)", nil)
}

func CypherSchemaTemporalNodeArityRequired() Message {
	return cypherSchemaMessage(MessageCypherSchemaTemporalNodeArityRequired, "TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)", nil)
}

func CypherSchemaDomainValueListInvalid(cause error) Message {
	return cypherSchemaMessage(MessageCypherSchemaDomainValueListInvalid, "invalid domain value list: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherSchemaDomainAllowedValueRequired() Message {
	return cypherSchemaMessage(MessageCypherSchemaDomainAllowedValueRequired, "DOMAIN constraint requires at least one allowed value", nil)
}

func CypherSchemaInvalidSyntax(subject string) Message {
	return cypherSchemaMessage(MessageCypherSchemaInvalidSyntax, "invalid "+subject+" syntax", map[string]any{"Subject": subject})
}

func CypherSchemaNameRequired(command, entity string) Message {
	return cypherSchemaMessage(MessageCypherSchemaNameRequired, "invalid "+command+" syntax: "+entity+" name required", map[string]any{"Command": command, "Entity": entity})
}

func CypherSchemaRangeIndexSinglePropertyRequired(count int) Message {
	return cypherSchemaMessage(MessageCypherSchemaRangeIndexSinglePropertyRequired, fmt.Sprintf("RANGE INDEX only supports single property, got %d", count), map[string]any{"Count": count})
}

func CypherSchemaCreateRangeIndexFailed(cause error) Message {
	return cypherSchemaMessage(MessageCypherSchemaCreateRangeIndexFailed, "failed to create range index: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherSchemaInvalidIdentifierSegment() Message {
	return cypherSchemaMessage(MessageCypherSchemaInvalidIdentifierSegment, "invalid identifier segment", nil)
}

func CypherSchemaInvalidIfNotExistsClause() Message {
	return cypherSchemaMessage(MessageCypherSchemaInvalidIfNotExistsClause, "invalid IF NOT EXISTS clause", nil)
}

func CypherSchemaEmptyClause(clause string) Message {
	return cypherSchemaMessage(MessageCypherSchemaEmptyClause, "empty "+clause+" clause", map[string]any{"Clause": clause})
}

func CypherSchemaInvalidClause(clause string) Message {
	return cypherSchemaMessage(MessageCypherSchemaInvalidClause, "invalid "+clause+" clause", map[string]any{"Clause": clause})
}

func CypherSchemaMissingKeyword(keyword string) Message {
	return cypherSchemaMessage(MessageCypherSchemaMissingKeyword, "missing "+keyword, map[string]any{"Keyword": keyword})
}

func CypherSchemaInvalidKeyword(keyword string) Message {
	return cypherSchemaMessage(MessageCypherSchemaInvalidKeyword, "invalid "+keyword, map[string]any{"Keyword": keyword})
}

func CypherSchemaInvalidTrailingSyntax() Message {
	return cypherSchemaMessage(MessageCypherSchemaInvalidTrailingSyntax, "invalid trailing syntax", nil)
}

func CypherSchemaIndexPropertiesRequired() Message {
	return cypherSchemaMessage(MessageCypherSchemaIndexPropertiesRequired, "no properties specified for index", nil)
}

func CypherSchemaIndexNameRequired() Message {
	return cypherSchemaMessage(MessageCypherSchemaIndexNameRequired, "missing index name", nil)
}

func CypherSchemaFulltextPropertiesRequired() Message {
	return cypherSchemaMessage(MessageCypherSchemaFulltextPropertiesRequired, "no properties found in fulltext index definition", nil)
}

func CypherSchemaEmptyStatement() Message {
	return cypherSchemaMessage(MessageCypherSchemaEmptyStatement, "empty statement", nil)
}

func CypherSchemaInvalidPattern(pattern string) Message {
	return cypherSchemaMessage(MessageCypherSchemaInvalidPattern, "invalid "+pattern+" pattern", map[string]any{"Pattern": pattern})
}

func CypherSchemaUnsupportedPredicate(predicate string) Message {
	return cypherSchemaMessage(MessageCypherSchemaUnsupportedPredicate, "unsupported "+predicate+" predicate", map[string]any{"Predicate": predicate})
}

func CypherSchemaUnsupportedCreateConstraintShape() Message {
	return cypherSchemaMessage(MessageCypherSchemaUnsupportedCreateConstraintShape, "unsupported CREATE CONSTRAINT shape", nil)
}

func CypherSchemaPositiveMaxCountRequired(raw string) Message {
	quoted := strconv.Quote(raw)
	return cypherSchemaMessage(MessageCypherSchemaPositiveMaxCountRequired, "MAX COUNT must be a positive integer, got "+quoted, map[string]any{"Value": quoted})
}

func CypherSchemaBackfillIndexFailed(label string, cause error) Message {
	return cypherSchemaMessage(MessageCypherSchemaBackfillIndexFailed, "failed to backfill index for label "+label+": "+cause.Error(), map[string]any{"Label": label, "Cause": cause.Error()})
}

func CypherSchemaBackfillPropertyIndexFailed(label, property string, cause error) Message {
	return cypherSchemaMessage(MessageCypherSchemaBackfillPropertyIndexFailed, "failed to backfill property index "+label+"("+property+"): "+cause.Error(), map[string]any{"Label": label, "Property": property, "Cause": cause.Error()})
}

func CypherSchemaUnsupportedPropertyType(propertyType string) Message {
	return cypherSchemaMessage(MessageCypherSchemaUnsupportedPropertyType, "unsupported property type: "+propertyType, map[string]any{"Type": propertyType})
}

func CypherSchemaManagerUnavailable() Message {
	return cypherSchemaMessage(MessageCypherSchemaManagerUnavailable, "schema manager not available", nil)
}

func CypherSchemaAddFulltextRelationshipIndexFailed(cause error) Message {
	return cypherSchemaMessage(MessageCypherSchemaAddFulltextRelationshipIndexFailed, "failed to add fulltext relationship index: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherSchemaAddFulltextIndexFailed(cause error) Message {
	return cypherSchemaMessage(MessageCypherSchemaAddFulltextIndexFailed, "failed to add fulltext index: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherSchemaUnterminatedString(raw string) Message {
	return cypherSchemaMessage(MessageCypherSchemaUnterminatedString, "unterminated string: "+raw, map[string]any{"Raw": raw})
}

func CypherSchemaContractEntryRequired() Message {
	return cypherSchemaMessage(MessageCypherSchemaContractEntryRequired, "constraint contract requires at least one block entry", nil)
}

func CypherSchemaMalformedRequireBlock() Message {
	return cypherSchemaMessage(MessageCypherSchemaMalformedRequireBlock, "malformed REQUIRE block", nil)
}

func CypherSchemaNestedContractEntryUnsupported(entry string) Message {
	return cypherSchemaMessage(MessageCypherSchemaNestedContractEntryUnsupported, "nested FOR ... REQUIRE entries are not supported inside REQUIRE blocks; create a separate targeted block constraint such as "+entry, map[string]any{"Entry": entry})
}
