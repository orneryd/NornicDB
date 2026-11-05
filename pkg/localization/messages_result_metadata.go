package localization

// MessageCypherResultMetadataUnresolved identifies an execution plan whose
// complete scope and ordered result schema could not be established.
const MessageCypherResultMetadataUnresolved MessageID = "cyphercore.result_metadata_unresolved"

// CypherResultMetadataUnresolved is a terminal owner error, never an empty
// successful header or permission to execute with incomplete scope facts.
func CypherResultMetadataUnresolved() Message {
	return Message{ID: MessageCypherResultMetadataUnresolved, Fallback: "Cannot establish the statement scope and result columns"}
}

// MessageCypherResultMetadataMismatch identifies execution fields that disagree
// with the complete statement schema.
const MessageCypherResultMetadataMismatch MessageID = "cyphercore.result_metadata_mismatch"

// CypherResultMetadataMismatch reports an internal execution contract failure.
func CypherResultMetadataMismatch() Message {
	return Message{ID: MessageCypherResultMetadataMismatch, Fallback: "Execution result columns do not match the statement schema"}
}

// MessageCypherResultMetadataImportVariablesRequired identifies an invalid CALL
// import item. The CALL keyword is Cypher syntax and remains untranslated.
const MessageCypherResultMetadataImportVariablesRequired MessageID = "cyphercore.call_import_variables_required"

// CypherResultMetadataImportVariablesRequired describes the import grammar.
func CypherResultMetadataImportVariablesRequired() Message {
	return Message{ID: MessageCypherResultMetadataImportVariablesRequired, Fallback: "CALL imports must contain only variable names"}
}
