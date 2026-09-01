package localization

import "fmt"

const (
	MessageCypherProceduresVectorQueryParseFailed                MessageID = "cypherprocedures.vector_query_parse_failed"
	MessageCypherProceduresStringQueryEmbedderRequired           MessageID = "cypherprocedures.string_query_embedder_required"
	MessageCypherProceduresEmbedQueryFailed                      MessageID = "cypherprocedures.embed_query_failed"
	MessageCypherProceduresParameterNotProvided                  MessageID = "cypherprocedures.parameter_not_provided"
	MessageCypherProceduresParameterNonNumeric                   MessageID = "cypherprocedures.parameter_non_numeric"
	MessageCypherProceduresParameterStringEmbedderRequired       MessageID = "cypherprocedures.parameter_string_embedder_required"
	MessageCypherProceduresEmbedParameterFailed                  MessageID = "cypherprocedures.embed_parameter_failed"
	MessageCypherProceduresParameterUnsupportedType              MessageID = "cypherprocedures.parameter_unsupported_type"
	MessageCypherProceduresUnsupportedParameters                 MessageID = "cypherprocedures.unsupported_parameters"
	MessageCypherProceduresQueryInputPossiblyUnsupported         MessageID = "cypherprocedures.query_input_possibly_unsupported"
	MessageCypherProceduresQueryInputRequired                    MessageID = "cypherprocedures.query_input_required"
	MessageCypherProceduresVectorCreateNodeInvalidSyntax         MessageID = "cypherprocedures.vector_create_node_invalid_syntax"
	MessageCypherProceduresVectorCreateNodeArgumentsRequired     MessageID = "cypherprocedures.vector_create_node_arguments_required"
	MessageCypherProceduresCreateVectorIndexFailed               MessageID = "cypherprocedures.create_vector_index_failed"
	MessageCypherProceduresVectorCreateRelationshipInvalidSyntax MessageID = "cypherprocedures.vector_create_relationship_invalid_syntax"
	MessageCypherProceduresVectorCreateRelationshipArguments     MessageID = "cypherprocedures.vector_create_relationship_arguments_required"
	MessageCypherProceduresInvalidDimension                      MessageID = "cypherprocedures.invalid_dimension"
	MessageCypherProceduresCreateRelationshipVectorIndexFailed   MessageID = "cypherprocedures.create_relationship_vector_index_failed"
	MessageCypherProceduresFulltextCreateNodeInvalidSyntax       MessageID = "cypherprocedures.fulltext_create_node_invalid_syntax"
	MessageCypherProceduresFulltextCreateNodeArgumentsRequired   MessageID = "cypherprocedures.fulltext_create_node_arguments_required"
	MessageCypherProceduresCreateFulltextIndexFailed             MessageID = "cypherprocedures.create_fulltext_index_failed"
	MessageCypherProceduresFulltextCreateRelationshipInvalid     MessageID = "cypherprocedures.fulltext_create_relationship_invalid_syntax"
	MessageCypherProceduresFulltextCreateRelationshipArguments   MessageID = "cypherprocedures.fulltext_create_relationship_arguments_required"
	MessageCypherProceduresCreateRelationshipFulltextIndexFailed MessageID = "cypherprocedures.create_relationship_fulltext_index_failed"
	MessageCypherProceduresFulltextDropInvalidSyntax             MessageID = "cypherprocedures.fulltext_drop_invalid_syntax"
	MessageCypherProceduresVectorDropInvalidSyntax               MessageID = "cypherprocedures.vector_drop_invalid_syntax"
	MessageCypherProceduresSetNodeVectorInvalidSyntax            MessageID = "cypherprocedures.set_node_vector_invalid_syntax"
	MessageCypherProceduresSetNodeVectorParenthesesRequired      MessageID = "cypherprocedures.set_node_vector_parentheses_required"
	MessageCypherProceduresSetNodeVectorArgumentsRequired        MessageID = "cypherprocedures.set_node_vector_arguments_required"
	MessageCypherProceduresSetNodeVectorArgumentRequired         MessageID = "cypherprocedures.set_node_vector_argument_required"
	MessageCypherProceduresNodeNotFound                          MessageID = "cypherprocedures.node_not_found"
	MessageCypherProceduresUpdateNodeFailed                      MessageID = "cypherprocedures.update_node_failed"
	MessageCypherProceduresSetRelationshipVectorInvalidSyntax    MessageID = "cypherprocedures.set_relationship_vector_invalid_syntax"
	MessageCypherProceduresSetRelationshipVectorParentheses      MessageID = "cypherprocedures.set_relationship_vector_parentheses_required"
	MessageCypherProceduresSetRelationshipVectorArguments        MessageID = "cypherprocedures.set_relationship_vector_arguments_required"
	MessageCypherProceduresSetRelationshipVectorArgument         MessageID = "cypherprocedures.set_relationship_vector_argument_required"
	MessageCypherProceduresRelationshipNotFound                  MessageID = "cypherprocedures.relationship_not_found"
	MessageCypherProceduresUpdateRelationshipFailed              MessageID = "cypherprocedures.update_relationship_failed"
	MessageCypherProceduresMetadataActiveTransactionRequired     MessageID = "cypherprocedures.metadata_active_transaction_required"
	MessageCypherProceduresMetadataInvalidSyntax                 MessageID = "cypherprocedures.metadata_invalid_syntax"
	MessageCypherProceduresMetadataObjectRequired                MessageID = "cypherprocedures.metadata_object_required"
	MessageCypherProceduresMetadataEntryRequired                 MessageID = "cypherprocedures.metadata_entry_required"
	MessageCypherProceduresMetadataTransactionUnsupported        MessageID = "cypherprocedures.metadata_transaction_unsupported"
	MessageCypherProceduresSetMetadataFailed                     MessageID = "cypherprocedures.set_metadata_failed"
	MessageCypherProceduresCreateInTransaction                   MessageID = "cypherprocedures.create_in_transaction"
	MessageCypherProceduresCreateInvalidSyntax                   MessageID = "cypherprocedures.create_invalid_syntax"
	MessageCypherProceduresBodyRequired                          MessageID = "cypherprocedures.body_required"
	MessageCypherProceduresAlreadyExists                         MessageID = "cypherprocedures.already_exists"
	MessageCypherProceduresEncodeRecordFailed                    MessageID = "cypherprocedures.encode_record_failed"
	MessageCypherProceduresUpdateCatalogFailed                   MessageID = "cypherprocedures.update_catalog_failed"
	MessageCypherProceduresPersistCatalogFailed                  MessageID = "cypherprocedures.persist_catalog_failed"
	MessageCypherProceduresDropInTransaction                     MessageID = "cypherprocedures.drop_in_transaction"
	MessageCypherProceduresDropInvalidSyntax                     MessageID = "cypherprocedures.drop_invalid_syntax"
	MessageCypherProceduresDropFailed                            MessageID = "cypherprocedures.drop_failed"
	MessageCypherProceduresRegistryReloadFailed                  MessageID = "cypherprocedures.registry_reload_failed"
	MessageCypherProceduresCatalogReadFailed                     MessageID = "cypherprocedures.catalog_read_failed"
	MessageCypherProceduresCatalogRecordDecodeFailed             MessageID = "cypherprocedures.catalog_record_decode_failed"
	MessageCypherProceduresCatalogRecordInvalid                  MessageID = "cypherprocedures.catalog_record_invalid"
	MessageCypherProceduresInvalidArgumentName                   MessageID = "cypherprocedures.invalid_argument_name"
	MessageCypherProceduresDuplicateArgument                     MessageID = "cypherprocedures.duplicate_argument"
	MessageCypherProceduresInvalidMode                           MessageID = "cypherprocedures.invalid_mode"
	MessageCypherProceduresReadContainsWrite                     MessageID = "cypherprocedures.read_contains_write"
	MessageCypherProceduresArgumentCount                         MessageID = "cypherprocedures.argument_count"
)

func cypherProceduresMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func cypherProceduresCause(id MessageID, prefix string, cause error) Message {
	return cypherProceduresMessage(id, prefix+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherProceduresVectorQueryParseFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresVectorQueryParseFailed, "vector query parse error: ", cause)
}
func CypherProceduresStringQueryEmbedderRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresStringQueryEmbedderRequired, "string query provided but no embedder configured; use vector array or configure embedding service", nil)
}
func CypherProceduresEmbedQueryFailed(query string, cause error) Message {
	return cypherProceduresMessage(MessageCypherProceduresEmbedQueryFailed, fmt.Sprintf("failed to embed query '%s': %s", query, cause), map[string]any{"Query": query, "Cause": cause.Error()})
}
func CypherProceduresParameterNotProvided(parameter string) Message {
	return cypherProceduresMessage(MessageCypherProceduresParameterNotProvided, "parameter $"+parameter+" not provided", map[string]any{"Parameter": parameter})
}
func CypherProceduresParameterNonNumeric(parameter, valueType string) Message {
	return cypherProceduresMessage(MessageCypherProceduresParameterNonNumeric, "parameter $"+parameter+" contains non-numeric value: "+valueType, map[string]any{"Parameter": parameter, "ValueType": valueType})
}
func CypherProceduresParameterStringEmbedderRequired(parameter string) Message {
	return cypherProceduresMessage(MessageCypherProceduresParameterStringEmbedderRequired, "parameter $"+parameter+" is a string but no embedder configured; provide vector array or configure embedding service", map[string]any{"Parameter": parameter})
}
func CypherProceduresEmbedParameterFailed(parameter, value string, cause error) Message {
	return cypherProceduresMessage(MessageCypherProceduresEmbedParameterFailed, fmt.Sprintf("failed to embed parameter $%s value '%s': %s", parameter, value, cause), map[string]any{"Parameter": parameter, "Value": value, "Cause": cause.Error()})
}
func CypherProceduresParameterUnsupportedType(parameter, valueType string) Message {
	return cypherProceduresMessage(MessageCypherProceduresParameterUnsupportedType, "parameter $"+parameter+" has unsupported type for vector query: "+valueType+" (expected []float32, []float64, []interface{}, or string)", map[string]any{"Parameter": parameter, "ValueType": valueType})
}
func CypherProceduresUnsupportedParameters(parameters string) Message {
	return cypherProceduresMessage(MessageCypherProceduresUnsupportedParameters, "no query vector or search text provided - parameter(s) "+parameters+" have unsupported type (expected []float32, []float64, []interface{}, or string)", map[string]any{"Parameters": parameters})
}
func CypherProceduresQueryInputPossiblyUnsupported() Message {
	return cypherProceduresMessage(MessageCypherProceduresQueryInputPossiblyUnsupported, "no query vector or search text provided (parameter may have unsupported type - expected []float32, []float64, []interface{}, or string)", nil)
}
func CypherProceduresQueryInputRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresQueryInputRequired, "no query vector or search text provided", nil)
}
func CypherProceduresVectorCreateNodeInvalidSyntax(parentheses bool) Message {
	fallback := "invalid db.index.vector.createNodeIndex syntax"
	if parentheses {
		fallback = "invalid syntax: missing parentheses"
	}
	return cypherProceduresMessage(MessageCypherProceduresVectorCreateNodeInvalidSyntax, fallback, map[string]any{"Parentheses": parentheses})
}
func CypherProceduresVectorCreateNodeArgumentsRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresVectorCreateNodeArgumentsRequired, "db.index.vector.createNodeIndex requires at least 4 arguments: indexName, label, property, dimension", nil)
}
func CypherProceduresCreateVectorIndexFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresCreateVectorIndexFailed, "failed to create vector index: ", cause)
}
func CypherProceduresVectorCreateRelationshipInvalidSyntax(parentheses bool) Message {
	fallback := "invalid db.index.vector.createRelationshipIndex syntax"
	if parentheses {
		fallback += ": missing parentheses"
	}
	return cypherProceduresMessage(MessageCypherProceduresVectorCreateRelationshipInvalidSyntax, fallback, map[string]any{"Parentheses": parentheses})
}
func CypherProceduresVectorCreateRelationshipArguments() Message {
	return cypherProceduresMessage(MessageCypherProceduresVectorCreateRelationshipArguments, "db.index.vector.createRelationshipIndex requires at least 4 arguments: indexName, relationshipType, property, dimension", nil)
}
func CypherProceduresInvalidDimension(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresInvalidDimension, "invalid dimension: ", cause)
}
func CypherProceduresCreateRelationshipVectorIndexFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresCreateRelationshipVectorIndexFailed, "failed to create relationship vector index: ", cause)
}
func CypherProceduresFulltextCreateNodeInvalidSyntax(parentheses bool) Message {
	fallback := "invalid db.index.fulltext.createNodeIndex syntax"
	if parentheses {
		fallback += ": missing parentheses"
	}
	return cypherProceduresMessage(MessageCypherProceduresFulltextCreateNodeInvalidSyntax, fallback, map[string]any{"Parentheses": parentheses})
}
func CypherProceduresFulltextCreateNodeArgumentsRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresFulltextCreateNodeArgumentsRequired, "db.index.fulltext.createNodeIndex requires at least 3 arguments: indexName, labels, properties", nil)
}
func CypherProceduresCreateFulltextIndexFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresCreateFulltextIndexFailed, "failed to create fulltext index: ", cause)
}
func CypherProceduresFulltextCreateRelationshipInvalid(parentheses bool) Message {
	fallback := "invalid db.index.fulltext.createRelationshipIndex syntax"
	if parentheses {
		fallback += ": missing parentheses"
	}
	return cypherProceduresMessage(MessageCypherProceduresFulltextCreateRelationshipInvalid, fallback, map[string]any{"Parentheses": parentheses})
}
func CypherProceduresFulltextCreateRelationshipArguments() Message {
	return cypherProceduresMessage(MessageCypherProceduresFulltextCreateRelationshipArguments, "db.index.fulltext.createRelationshipIndex requires at least 3 arguments: indexName, relationshipTypes, properties", nil)
}
func CypherProceduresCreateRelationshipFulltextIndexFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresCreateRelationshipFulltextIndexFailed, "failed to create relationship fulltext index: ", cause)
}
func CypherProceduresFulltextDropInvalidSyntax(parentheses bool) Message {
	fallback := "invalid db.index.fulltext.drop syntax"
	if parentheses {
		fallback += ": missing parentheses"
	}
	return cypherProceduresMessage(MessageCypherProceduresFulltextDropInvalidSyntax, fallback, map[string]any{"Parentheses": parentheses})
}
func CypherProceduresVectorDropInvalidSyntax(parentheses bool) Message {
	fallback := "invalid db.index.vector.drop syntax"
	if parentheses {
		fallback += ": missing parentheses"
	}
	return cypherProceduresMessage(MessageCypherProceduresVectorDropInvalidSyntax, fallback, map[string]any{"Parentheses": parentheses})
}
func CypherProceduresSetNodeVectorInvalidSyntax() Message {
	return cypherProceduresMessage(MessageCypherProceduresSetNodeVectorInvalidSyntax, "invalid db.create.setNodeVectorProperty syntax", nil)
}
func CypherProceduresSetNodeVectorParenthesesRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresSetNodeVectorParenthesesRequired, "db.create.setNodeVectorProperty: missing parentheses (expected db.create.setNodeVectorProperty(nodeId, 'key', [vector]))", nil)
}
func CypherProceduresSetNodeVectorArgumentsRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresSetNodeVectorArgumentsRequired, "db.create.setNodeVectorProperty: requires 3 arguments (nodeId, propertyKey, vector)", nil)
}
func CypherProceduresSetNodeVectorArgumentRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresSetNodeVectorArgumentRequired, "db.create.setNodeVectorProperty: missing vector argument (expected nodeId, propertyKey, [vector])", nil)
}
func CypherProceduresNodeNotFound(node string) Message {
	return cypherProceduresMessage(MessageCypherProceduresNodeNotFound, "node not found: "+node, map[string]any{"Node": node})
}
func CypherProceduresUpdateNodeFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresUpdateNodeFailed, "failed to update node: ", cause)
}
func CypherProceduresSetRelationshipVectorInvalidSyntax() Message {
	return cypherProceduresMessage(MessageCypherProceduresSetRelationshipVectorInvalidSyntax, "invalid db.create.setRelationshipVectorProperty syntax", nil)
}
func CypherProceduresSetRelationshipVectorParentheses() Message {
	return cypherProceduresMessage(MessageCypherProceduresSetRelationshipVectorParentheses, "db.create.setRelationshipVectorProperty: missing parentheses (expected db.create.setRelationshipVectorProperty(relId, 'key', [vector]))", nil)
}
func CypherProceduresSetRelationshipVectorArguments() Message {
	return cypherProceduresMessage(MessageCypherProceduresSetRelationshipVectorArguments, "db.create.setRelationshipVectorProperty: requires 3 arguments (relId, propertyKey, vector)", nil)
}
func CypherProceduresSetRelationshipVectorArgument() Message {
	return cypherProceduresMessage(MessageCypherProceduresSetRelationshipVectorArgument, "db.create.setRelationshipVectorProperty: missing vector argument (expected relId, propertyKey, [vector])", nil)
}
func CypherProceduresRelationshipNotFound(relationship string) Message {
	return cypherProceduresMessage(MessageCypherProceduresRelationshipNotFound, "relationship not found: "+relationship, map[string]any{"Relationship": relationship})
}
func CypherProceduresUpdateRelationshipFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresUpdateRelationshipFailed, "failed to update relationship: ", cause)
}
func CypherProceduresMetadataActiveTransactionRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresMetadataActiveTransactionRequired, "tx.setMetaData() requires an active transaction. Use BEGIN TRANSACTION first", nil)
}
func CypherProceduresMetadataInvalidSyntax(parentheses bool) Message {
	fallback := "invalid tx.setMetaData syntax"
	if parentheses {
		fallback += ": missing parentheses"
	}
	return cypherProceduresMessage(MessageCypherProceduresMetadataInvalidSyntax, fallback, map[string]any{"Parentheses": parentheses})
}
func CypherProceduresMetadataObjectRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresMetadataObjectRequired, "tx.setMetaData requires a metadata object: {key: value}", nil)
}
func CypherProceduresMetadataEntryRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresMetadataEntryRequired, "tx.setMetaData requires at least one key-value pair", nil)
}
func CypherProceduresMetadataTransactionUnsupported() Message {
	return cypherProceduresMessage(MessageCypherProceduresMetadataTransactionUnsupported, "transaction type not supported for metadata", nil)
}
func CypherProceduresSetMetadataFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresSetMetadataFailed, "failed to set transaction metadata: ", cause)
}
func CypherProceduresCreateInTransaction() Message {
	return cypherProceduresMessage(MessageCypherProceduresCreateInTransaction, "CREATE PROCEDURE is not allowed inside an active transaction", nil)
}
func CypherProceduresCreateInvalidSyntax() Message {
	return cypherProceduresMessage(MessageCypherProceduresCreateInvalidSyntax, "invalid CREATE PROCEDURE syntax", nil)
}
func CypherProceduresBodyRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresBodyRequired, "procedure body cannot be empty", nil)
}
func CypherProceduresAlreadyExists(procedure string) Message {
	return cypherProceduresMessage(MessageCypherProceduresAlreadyExists, "procedure "+procedure+" already exists", map[string]any{"Procedure": procedure})
}
func CypherProceduresEncodeRecordFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresEncodeRecordFailed, "failed to encode procedure record: ", cause)
}
func CypherProceduresUpdateCatalogFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresUpdateCatalogFailed, "failed to update procedure catalog: ", cause)
}
func CypherProceduresPersistCatalogFailed(cause error) Message {
	return cypherProceduresCause(MessageCypherProceduresPersistCatalogFailed, "failed to persist procedure catalog: ", cause)
}
func CypherProceduresDropInTransaction() Message {
	return cypherProceduresMessage(MessageCypherProceduresDropInTransaction, "DROP PROCEDURE is not allowed inside an active transaction", nil)
}
func CypherProceduresDropInvalidSyntax() Message {
	return cypherProceduresMessage(MessageCypherProceduresDropInvalidSyntax, "invalid DROP PROCEDURE syntax", nil)
}
func CypherProceduresDropFailed(procedure string, cause error) Message {
	return cypherProceduresMessage(MessageCypherProceduresDropFailed, "failed to drop procedure "+procedure+": "+cause.Error(), map[string]any{"Procedure": procedure, "Cause": cause.Error()})
}
func CypherProceduresRegistryReloadFailed(cause error) Message {
	return cypherProceduresMessage(MessageCypherProceduresRegistryReloadFailed, "cypher: procedure registry reload failed: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}
func CypherProceduresCatalogReadFailed(cause error) Message {
	return cypherProceduresMessage(MessageCypherProceduresCatalogReadFailed, "cypher: procedure catalog read failed: "+cause.Error(), map[string]any{"Cause": cause.Error()})
}
func CypherProceduresCatalogRecordDecodeFailed(node string) Message {
	return cypherProceduresMessage(MessageCypherProceduresCatalogRecordDecodeFailed, "cypher: procedure catalog record decode failed: node="+node, map[string]any{"Node": node})
}
func CypherProceduresCatalogRecordInvalid(node string) Message {
	return cypherProceduresMessage(MessageCypherProceduresCatalogRecordInvalid, "cypher: procedure catalog record invalid: node="+node, map[string]any{"Node": node})
}
func CypherProceduresInvalidArgumentName(argument string) Message {
	return cypherProceduresMessage(MessageCypherProceduresInvalidArgumentName, "invalid procedure argument name: "+argument, map[string]any{"Argument": argument})
}
func CypherProceduresDuplicateArgument(argument string) Message {
	return cypherProceduresMessage(MessageCypherProceduresDuplicateArgument, "duplicate procedure argument: "+argument, map[string]any{"Argument": argument})
}
func CypherProceduresInvalidMode(mode string) Message {
	return cypherProceduresMessage(MessageCypherProceduresInvalidMode, "invalid procedure mode: "+mode, map[string]any{"Mode": mode})
}
func CypherProceduresReadContainsWrite() Message {
	return cypherProceduresMessage(MessageCypherProceduresReadContainsWrite, "READ procedure body contains write operations", nil)
}
func CypherProceduresArgumentCount(procedure string, expected, actual int) Message {
	return cypherProceduresMessage(MessageCypherProceduresArgumentCount, fmt.Sprintf("procedure %s requires %d arguments, got %d", procedure, expected, actual), map[string]any{"Procedure": procedure, "Expected": expected, "Actual": actual})
}
