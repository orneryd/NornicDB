package localization

import "strconv"

const (
	MessageAdminImportOpenCSVFailed            MessageID = "adminimport.open_csv_failed"
	MessageAdminImportDatabaseNameRequired     MessageID = "adminimport.database_name_required"
	MessageAdminImportNodeSourceRequired       MessageID = "adminimport.node_source_required"
	MessageAdminImportStorageEngineRequired    MessageID = "adminimport.storage_engine_required"
	MessageAdminImportActualIDUnsupported      MessageID = "adminimport.actual_id_unsupported"
	MessageAdminImportRelationshipIDsRequired  MessageID = "adminimport.relationship_ids_required"
	MessageAdminImportRelationshipTypeRequired MessageID = "adminimport.relationship_type_required"
	MessageAdminImportInvalidVectorDimensions  MessageID = "adminimport.invalid_vector_dimensions"
	MessageAdminImportUnsupportedHeaderToken   MessageID = "adminimport.unsupported_header_token"
	MessageAdminImportUnsupportedPropertyType  MessageID = "adminimport.unsupported_property_type"
	MessageAdminImportEmptySource              MessageID = "adminimport.empty_source"
	MessageAdminImportRelationshipPrefixLimit  MessageID = "adminimport.relationship_prefix_limit"
	MessageAdminImportCustomQuoteUnsupported   MessageID = "adminimport.custom_quote_unsupported"
	MessageAdminImportZipSingleFileRequired    MessageID = "adminimport.zip_single_file_required"
	MessageAdminImportTargetNotEmpty           MessageID = "adminimport.target_not_empty"
	MessageAdminImportNoCSVFiles               MessageID = "adminimport.no_csv_files"
	MessageAdminImportNoNodeCSVFiles           MessageID = "adminimport.no_node_csv_files"
	MessageAdminExportOutputDirectoryRequired  MessageID = "adminimport.export_output_directory_required"
	MessageAdminExportCustomQuoteUnsupported   MessageID = "adminimport.export_custom_quote_unsupported"
	MessageAdminExportLabelUnsupported         MessageID = "adminimport.export_label_unsupported"
	MessageAdminExportArrayElementUnsupported  MessageID = "adminimport.export_array_element_unsupported"
	MessageAdminImportUnsupportedCSVHeader     MessageID = "adminimport.unsupported_csv_header"
	MessageAdminImportSchemaApplicationFailed  MessageID = "adminimport.schema_application_failed"
	MessageAdminImportDuplicateNodeID          MessageID = "adminimport.duplicate_node_id"
	MessageAdminImportDuplicateRelationshipID  MessageID = "adminimport.duplicate_relationship_id"
	MessageAdminImportDuplicateRelIDAtRow      MessageID = "adminimport.duplicate_relationship_id_at_row"
	MessageAdminImportBadStartRelationship     MessageID = "adminimport.bad_start_relationship"
	MessageAdminImportBadEndRelationship       MessageID = "adminimport.bad_end_relationship"
	MessageAdminImportBadStartTolerance        MessageID = "adminimport.bad_start_tolerance_exceeded"
	MessageAdminImportBadEndTolerance          MessageID = "adminimport.bad_end_tolerance_exceeded"
	MessageAdminImportOpenGzipCSVFailed        MessageID = "adminimport.open_gzip_csv_failed"
	MessageAdminImportOpenZipCSVFailed         MessageID = "adminimport.open_zip_csv_failed"
	MessageAdminImportOpenZipMemberFailed      MessageID = "adminimport.open_zip_member_failed"
	MessageAdminImportCSVParseError            MessageID = "adminimport.csv_parse_error"
	MessageAdminImportScanDirectoryFailed      MessageID = "adminimport.scan_directory_failed"
	MessageAdminImportReadHeaderFailed         MessageID = "adminimport.read_header_failed"
)

// AdminImportOpenCSVFailed identifies a CSV source open failure.
func AdminImportOpenCSVFailed() Message {
	return Message{ID: MessageAdminImportOpenCSVFailed, Fallback: "failed to open CSV file"}
}

func AdminImportDatabaseNameRequired() Message {
	return Message{ID: MessageAdminImportDatabaseNameRequired, Fallback: "database name is required"}
}
func AdminImportNodeSourceRequired() Message {
	return Message{ID: MessageAdminImportNodeSourceRequired, Fallback: "at least one --nodes source is required"}
}
func AdminImportStorageEngineRequired() Message {
	return Message{ID: MessageAdminImportStorageEngineRequired, Fallback: "storage engine is required"}
}
func AdminImportActualIDUnsupported() Message {
	return Message{ID: MessageAdminImportActualIDUnsupported, Fallback: "--id-type=actual is not supported"}
}
func AdminImportRelationshipIDsRequired() Message {
	return Message{ID: MessageAdminImportRelationshipIDsRequired, Fallback: "relationship source requires :START_ID and :END_ID columns"}
}
func AdminImportRelationshipTypeRequired() Message {
	return Message{ID: MessageAdminImportRelationshipTypeRequired, Fallback: "relationship source requires :TYPE column or --relationships=TYPE= prefix"}
}
func AdminImportInvalidVectorDimensions(header string) Message {
	return Message{ID: MessageAdminImportInvalidVectorDimensions, Fallback: "invalid vector dimensions in header: " + header, Data: map[string]any{"Header": header}}
}
func AdminImportUnsupportedHeaderToken(token string) Message {
	return Message{ID: MessageAdminImportUnsupportedHeaderToken, Fallback: "unsupported header token: " + token, Data: map[string]any{"Token": token}}
}
func AdminImportUnsupportedPropertyType(propertyType string) Message {
	return Message{ID: MessageAdminImportUnsupportedPropertyType, Fallback: "unsupported property type: " + propertyType, Data: map[string]any{"Type": propertyType}}
}
func AdminImportEmptySource() Message {
	return Message{ID: MessageAdminImportEmptySource, Fallback: "empty import source"}
}
func AdminImportRelationshipPrefixLimit() Message {
	return Message{ID: MessageAdminImportRelationshipPrefixLimit, Fallback: "relationship source accepts at most one type prefix"}
}
func AdminImportCustomQuoteUnsupported() Message {
	return Message{ID: MessageAdminImportCustomQuoteUnsupported, Fallback: "custom --quote is not supported by the Go CSV reader yet"}
}
func AdminImportZipSingleFileRequired() Message {
	return Message{ID: MessageAdminImportZipSingleFileRequired, Fallback: "zip CSV sources must contain exactly one file"}
}
func AdminImportTargetNotEmpty() Message {
	return Message{ID: MessageAdminImportTargetNotEmpty, Fallback: "database import full requires an empty target database"}
}
func AdminImportNoCSVFiles() Message {
	return Message{ID: MessageAdminImportNoCSVFiles, Fallback: "no Neo4j-compatible CSV files found in source directory"}
}
func AdminImportNoNodeCSVFiles() Message {
	return Message{ID: MessageAdminImportNoNodeCSVFiles, Fallback: "source directory does not contain any node CSV files"}
}
func AdminExportOutputDirectoryRequired() Message {
	return Message{ID: MessageAdminExportOutputDirectoryRequired, Fallback: "output directory is required"}
}
func AdminExportCustomQuoteUnsupported() Message {
	return Message{ID: MessageAdminExportCustomQuoteUnsupported, Fallback: "custom quote characters are not supported for Neo4j CSV export"}
}

// AdminExportLabelUnsupported identifies a label the Neo4j CSV label field
// cannot preserve. Quoting keeps control characters explicit in diagnostics.
// One representability contract covers the label array field: labels must be
// nonempty, have no leading or trailing whitespace, and contain no array
// delimiter. For example, label "A;B" with delimiter ';' needs another array
// delimiter, and label " Archive " needs a nonempty label without surrounding
// whitespace.
func AdminExportLabelUnsupported(label string, delimiter rune) Message {
	quotedLabel, quotedDelimiter := strconv.Quote(label), strconv.QuoteRune(delimiter)
	return Message{ID: MessageAdminExportLabelUnsupported, Fallback: "label " + quotedLabel + " cannot be written to Neo4j CSV with array delimiter " + quotedDelimiter + ": labels must be nonempty, must not have leading or trailing whitespace, must not contain the array delimiter, and must not contain CRLF line breaks; choose a suitable --array-delimiter for a delimiter conflict, or use a nonempty label without surrounding whitespace or CRLF line breaks, or another export format, for a normalization conflict", Data: map[string]any{"Label": quotedLabel, "Delimiter": quotedDelimiter}}
}

// AdminExportArrayElementUnsupported identifies one list element the Neo4j CSV
// array field cannot preserve. The array field joins elements with the array
// delimiter and quotes no element, while the reader splits on that delimiter,
// trims each element and drops empty elements, so an element that contains the
// delimiter, is empty, has surrounding whitespace or contains a CRLF line break
// cannot be read back as the same element. Property, Element and Delimiter are
// quoted literal values.
func AdminExportArrayElementUnsupported(property, element string, delimiter rune) Message {
	quotedProperty, quotedElement, quotedDelimiter := strconv.Quote(property), strconv.Quote(element), strconv.QuoteRune(delimiter)
	return Message{ID: MessageAdminExportArrayElementUnsupported, Fallback: "property " + quotedProperty + " list element " + quotedElement + " cannot be written to Neo4j CSV with array delimiter " + quotedDelimiter + ": list elements must be nonempty, must not have leading or trailing whitespace, must not contain the array delimiter, and must not contain CRLF line breaks; choose a suitable --array-delimiter for a delimiter conflict, or use elements without surrounding whitespace or CRLF line breaks, or another export format, for a normalization conflict", Data: map[string]any{"Property": quotedProperty, "Element": quotedElement, "Delimiter": quotedDelimiter}}
}

func AdminImportUnsupportedCSVHeader(path string) Message {
	return Message{ID: MessageAdminImportUnsupportedCSVHeader, Fallback: "unsupported CSV header in Neo4j source directory: " + path, Data: map[string]any{"Path": path}}
}
func AdminImportSchemaApplicationFailed() Message {
	return Message{ID: MessageAdminImportSchemaApplicationFailed, Fallback: "schema application failed"}
}
func AdminImportDuplicateNodeID(row int) Message {
	return Message{ID: MessageAdminImportDuplicateNodeID, Fallback: "duplicate node ID at row " + strconv.Itoa(row), Data: map[string]any{"Row": row}}
}
func AdminImportDuplicateRelationshipID() Message {
	return Message{ID: MessageAdminImportDuplicateRelationshipID, Fallback: "duplicate relationship ID"}
}
func AdminImportDuplicateRelationshipIDAtRow(row int) Message {
	return Message{ID: MessageAdminImportDuplicateRelIDAtRow, Fallback: "duplicate relationship ID at row " + strconv.Itoa(row), Data: map[string]any{"Row": row}}
}
func AdminImportBadStartRelationship(row int) Message {
	return Message{ID: MessageAdminImportBadStartRelationship, Fallback: "bad relationship at row " + strconv.Itoa(row) + ": missing start node", Data: map[string]any{"Row": row}}
}
func AdminImportBadEndRelationship(row int) Message {
	return Message{ID: MessageAdminImportBadEndRelationship, Fallback: "bad relationship at row " + strconv.Itoa(row) + ": missing end node", Data: map[string]any{"Row": row}}
}
func AdminImportBadStartToleranceExceeded(row int, badCount int64, tolerance int) Message {
	data := map[string]any{"Row": row, "BadCount": badCount, "Tolerance": tolerance}
	prefix := "bad relationship tolerance exceeded at row " + strconv.Itoa(row) + " (" + strconv.FormatInt(badCount, 10) + " > " + strconv.Itoa(tolerance) + "): "
	return Message{ID: MessageAdminImportBadStartTolerance, Fallback: prefix + "missing start node", Data: data}
}
func AdminImportBadEndToleranceExceeded(row int, badCount int64, tolerance int) Message {
	data := map[string]any{"Row": row, "BadCount": badCount, "Tolerance": tolerance}
	prefix := "bad relationship tolerance exceeded at row " + strconv.Itoa(row) + " (" + strconv.FormatInt(badCount, 10) + " > " + strconv.Itoa(tolerance) + "): "
	return Message{ID: MessageAdminImportBadEndTolerance, Fallback: prefix + "missing end node", Data: data}
}
func AdminImportOpenGzipCSVFailed() Message {
	return Message{ID: MessageAdminImportOpenGzipCSVFailed, Fallback: "failed to open gzip CSV file"}
}
func AdminImportOpenZipCSVFailed() Message {
	return Message{ID: MessageAdminImportOpenZipCSVFailed, Fallback: "failed to open zip CSV file"}
}
func AdminImportOpenZipMemberFailed() Message {
	return Message{ID: MessageAdminImportOpenZipMemberFailed, Fallback: "failed to open zipped CSV member"}
}
func AdminImportCSVParseError(where string) Message {
	return Message{ID: MessageAdminImportCSVParseError, Fallback: "CSV parse error at " + where, Data: map[string]any{"Where": where}}
}
func AdminImportScanDirectoryFailed() Message {
	return Message{ID: MessageAdminImportScanDirectoryFailed, Fallback: "failed to scan Neo4j CSV directory"}
}
func AdminImportReadHeaderFailed() Message {
	return Message{ID: MessageAdminImportReadHeaderFailed, Fallback: "failed to read CSV header"}
}
