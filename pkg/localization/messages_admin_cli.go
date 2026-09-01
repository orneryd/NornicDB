package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageAdminCLIErrorPrefix                 MessageID = "admincli.error_prefix"
	MessageAdminCLIRootShort                   MessageID = "admincli.root_short"
	MessageAdminCLIDatabaseShort               MessageID = "admincli.database_short"
	MessageAdminCLIImportShort                 MessageID = "admincli.import_short"
	MessageAdminCLIFullImportShort             MessageID = "admincli.full_import_short"
	MessageAdminCLIIncrementalImportShort      MessageID = "admincli.incremental_import_short"
	MessageAdminCLIExportShort                 MessageID = "admincli.export_short"
	MessageAdminCLINeo4jCSVExportShort         MessageID = "admincli.neo4j_csv_export_short"
	MessageAdminCLIServerShort                 MessageID = "admincli.server_short"
	MessageAdminCLIServerStatusShort           MessageID = "admincli.server_status_short"
	MessageAdminCLIVersionShort                MessageID = "admincli.version_short"
	MessageAdminCLIDataDirectoryFlag           MessageID = "admincli.flag.data_directory"
	MessageAdminCLINodesFlag                   MessageID = "admincli.flag.nodes"
	MessageAdminCLIRelationshipsFlag           MessageID = "admincli.flag.relationships"
	MessageAdminCLIFromPathFlag                MessageID = "admincli.flag.from_path"
	MessageAdminCLISchemaFlag                  MessageID = "admincli.flag.schema"
	MessageAdminCLIBuildIndexesFlag            MessageID = "admincli.flag.build_indexes"
	MessageAdminCLISkipBadRelationshipsFlag    MessageID = "admincli.flag.skip_bad_relationships"
	MessageAdminCLISkipDuplicateNodesFlag      MessageID = "admincli.flag.skip_duplicate_nodes"
	MessageAdminCLINormalizeTypesFlag          MessageID = "admincli.flag.normalize_types"
	MessageAdminCLIIgnoreExtraColumnsFlag      MessageID = "admincli.flag.ignore_extra_columns"
	MessageAdminCLIIgnoreEmptyStringsFlag      MessageID = "admincli.flag.ignore_empty_strings"
	MessageAdminCLIReportFileFlag              MessageID = "admincli.flag.report_file"
	MessageAdminCLIIDTypeFlag                  MessageID = "admincli.flag.id_type"
	MessageAdminCLIBadToleranceFlag            MessageID = "admincli.flag.bad_tolerance"
	MessageAdminCLIChunkSizeFlag               MessageID = "admincli.flag.chunk_size"
	MessageAdminCLIDelimiterFlag               MessageID = "admincli.flag.delimiter"
	MessageAdminCLIArrayDelimiterFlag          MessageID = "admincli.flag.array_delimiter"
	MessageAdminCLIVectorDelimiterFlag         MessageID = "admincli.flag.vector_delimiter"
	MessageAdminCLIQuoteFlag                   MessageID = "admincli.flag.quote"
	MessageAdminCLIConstraintsFileFlag         MessageID = "admincli.flag.constraints_file"
	MessageAdminCLIVerboseFlag                 MessageID = "admincli.flag.verbose"
	MessageAdminCLIToPathFlag                  MessageID = "admincli.flag.to_path"
	MessageAdminCLIIncrementalNotImplemented   MessageID = "admincli.incremental_not_implemented"
	MessageAdminCLIDatabaseInfoNotImplemented  MessageID = "admincli.database_info_not_implemented"
	MessageAdminCLIServerStatusNotImplemented  MessageID = "admincli.server_status_not_implemented"
	MessageAdminCLIToPathRequired              MessageID = "admincli.to_path_required"
	MessageAdminCLIExactArgs                   MessageID = "admincli.exact_args"
	MessageAdminCLIUnknownCommand              MessageID = "admincli.unknown_command"
	MessageAdminCLIUsageHeading                MessageID = "admincli.help.usage_heading"
	MessageAdminCLIAliasesHeading              MessageID = "admincli.help.aliases_heading"
	MessageAdminCLIExamplesHeading             MessageID = "admincli.help.examples_heading"
	MessageAdminCLIAvailableCommandsHeading    MessageID = "admincli.help.available_commands_heading"
	MessageAdminCLIAdditionalCommandsHeading   MessageID = "admincli.help.additional_commands_heading"
	MessageAdminCLIFlagsHeading                MessageID = "admincli.help.flags_heading"
	MessageAdminCLIGlobalFlagsHeading          MessageID = "admincli.help.global_flags_heading"
	MessageAdminCLIAdditionalHelpTopicsHeading MessageID = "admincli.help.additional_topics_heading"
	MessageAdminCLIHelpMoreInformation         MessageID = "admincli.help.more_information"
	MessageAdminCLIHelpCommandShort            MessageID = "admincli.help.command_short"
	MessageAdminCLIHelpCommandLong             MessageID = "admincli.help.command_long"
	MessageAdminCLIHelpFlag                    MessageID = "admincli.help.flag"
	MessageAdminCLICompletionCommandShort      MessageID = "admincli.completion.command_short"
	MessageAdminCLICompletionCommandLong       MessageID = "admincli.completion.command_long"
)

func AdminCLIErrorPrefix() Message {
	return Message{ID: MessageAdminCLIErrorPrefix, Fallback: "Error:"}
}
func AdminCLIRootShort() Message {
	return Message{ID: MessageAdminCLIRootShort, Fallback: "Administrative tools for NornicDB"}
}
func AdminCLIDatabaseShort() Message {
	return Message{ID: MessageAdminCLIDatabaseShort, Fallback: "Database administration commands"}
}
func AdminCLIImportShort() Message {
	return Message{ID: MessageAdminCLIImportShort, Fallback: "Import CSV data into an offline database"}
}
func AdminCLIFullImportShort() Message {
	return Message{ID: MessageAdminCLIFullImportShort, Fallback: "Run a full offline import"}
}
func AdminCLIIncrementalImportShort() Message {
	return Message{ID: MessageAdminCLIIncrementalImportShort, Fallback: "Reserved incremental import command"}
}
func AdminCLIExportShort() Message {
	return Message{ID: MessageAdminCLIExportShort, Fallback: "Export database data for offline migration"}
}
func AdminCLINeo4jCSVExportShort() Message {
	return Message{ID: MessageAdminCLINeo4jCSVExportShort, Fallback: "Export a database as Neo4j-compatible CSV files"}
}
func AdminCLIServerShort() Message {
	return Message{ID: MessageAdminCLIServerShort, Fallback: "Server commands"}
}
func AdminCLIServerStatusShort() Message {
	return Message{ID: MessageAdminCLIServerStatusShort, Fallback: "Show server status"}
}
func AdminCLIVersionShort() Message {
	return Message{ID: MessageAdminCLIVersionShort, Fallback: "Print version"}
}
func AdminCLIDataDirectoryFlag() Message {
	return Message{ID: MessageAdminCLIDataDirectoryFlag, Fallback: "Target data directory"}
}
func AdminCLINodesFlag() Message {
	return Message{ID: MessageAdminCLINodesFlag, Fallback: "Node CSV source (repeatable)"}
}
func AdminCLIRelationshipsFlag() Message {
	return Message{ID: MessageAdminCLIRelationshipsFlag, Fallback: "Relationship CSV source (repeatable)"}
}
func AdminCLIFromPathFlag() Message {
	return Message{ID: MessageAdminCLIFromPathFlag, Fallback: "Directory containing Neo4j-compatible CSV files"}
}
func AdminCLISchemaFlag() Message {
	return Message{ID: MessageAdminCLISchemaFlag, Fallback: "Cypher schema file to apply after load"}
}
func AdminCLIBuildIndexesFlag() Message {
	return Message{ID: MessageAdminCLIBuildIndexesFlag, Fallback: "Build search indexes after import"}
}
func AdminCLISkipBadRelationshipsFlag() Message {
	return Message{ID: MessageAdminCLISkipBadRelationshipsFlag, Fallback: "Skip relationships that reference missing nodes"}
}
func AdminCLISkipDuplicateNodesFlag() Message {
	return Message{ID: MessageAdminCLISkipDuplicateNodesFlag, Fallback: "Skip duplicate node IDs"}
}
func AdminCLINormalizeTypesFlag() Message {
	return Message{ID: MessageAdminCLINormalizeTypesFlag, Fallback: "Normalize imported property values"}
}
func AdminCLIIgnoreExtraColumnsFlag() Message {
	return Message{ID: MessageAdminCLIIgnoreExtraColumnsFlag, Fallback: "Ignore extra CSV columns"}
}
func AdminCLIIgnoreEmptyStringsFlag() Message {
	return Message{ID: MessageAdminCLIIgnoreEmptyStringsFlag, Fallback: "Treat empty strings as null"}
}
func AdminCLIReportFileFlag() Message {
	return Message{ID: MessageAdminCLIReportFileFlag, Fallback: "Write a JSON report"}
}
func AdminCLIIDTypeFlag() Message {
	return Message{ID: MessageAdminCLIIDTypeFlag, Fallback: "ID type: string or integer"}
}
func AdminCLIBadToleranceFlag() Message {
	return Message{ID: MessageAdminCLIBadToleranceFlag, Fallback: "Number of bad rows tolerated before abort"}
}
func AdminCLIChunkSizeFlag() Message {
	return Message{ID: MessageAdminCLIChunkSizeFlag, Fallback: "Rows per bulk write chunk"}
}
func AdminCLIDelimiterFlag() Message {
	return Message{ID: MessageAdminCLIDelimiterFlag, Fallback: "Field delimiter"}
}
func AdminCLIArrayDelimiterFlag() Message {
	return Message{ID: MessageAdminCLIArrayDelimiterFlag, Fallback: "Array delimiter"}
}
func AdminCLIVectorDelimiterFlag() Message {
	return Message{ID: MessageAdminCLIVectorDelimiterFlag, Fallback: "Vector delimiter"}
}
func AdminCLIQuoteFlag() Message {
	return Message{ID: MessageAdminCLIQuoteFlag, Fallback: "Quote character"}
}
func AdminCLIConstraintsFileFlag() Message {
	return Message{ID: MessageAdminCLIConstraintsFileFlag, Fallback: "Deprecated alias for --schema"}
}
func AdminCLIVerboseFlag() Message {
	return Message{ID: MessageAdminCLIVerboseFlag, Fallback: "Verbose logging"}
}
func AdminCLIToPathFlag() Message {
	return Message{ID: MessageAdminCLIToPathFlag, Fallback: "Output directory for Neo4j-compatible CSV files"}
}
func AdminCLIIncrementalNotImplemented() Message {
	return Message{ID: MessageAdminCLIIncrementalNotImplemented, Fallback: "database import incremental is not implemented yet"}
}
func AdminCLIDatabaseInfoNotImplemented() Message {
	return Message{ID: MessageAdminCLIDatabaseInfoNotImplemented, Fallback: "database info is not implemented yet"}
}
func AdminCLIServerStatusNotImplemented() Message {
	return Message{ID: MessageAdminCLIServerStatusNotImplemented, Fallback: "server status is not implemented yet"}
}
func AdminCLIToPathRequired() Message {
	return Message{ID: MessageAdminCLIToPathRequired, Fallback: "--to-path is required"}
}
func AdminCLIExactArgs(expected, received int) Message {
	return Message{ID: MessageAdminCLIExactArgs, Fallback: fmt.Sprintf("accepts %d arg(s), received %d", expected, received), Data: map[string]any{"Expected": expected, "Received": received}}
}
func AdminCLIUnknownCommand(argument, command string) Message {
	return Message{ID: MessageAdminCLIUnknownCommand, Fallback: fmt.Sprintf("unknown command %q for %q", argument, command), Data: map[string]any{"Argument": strconv.Quote(argument), "Command": strconv.Quote(command)}}
}
func AdminCLIUsageHeading() Message {
	return Message{ID: MessageAdminCLIUsageHeading, Fallback: "Usage:"}
}
func AdminCLIAliasesHeading() Message {
	return Message{ID: MessageAdminCLIAliasesHeading, Fallback: "Aliases:"}
}
func AdminCLIExamplesHeading() Message {
	return Message{ID: MessageAdminCLIExamplesHeading, Fallback: "Examples:"}
}
func AdminCLIAvailableCommandsHeading() Message {
	return Message{ID: MessageAdminCLIAvailableCommandsHeading, Fallback: "Available Commands:"}
}
func AdminCLIAdditionalCommandsHeading() Message {
	return Message{ID: MessageAdminCLIAdditionalCommandsHeading, Fallback: "Additional Commands:"}
}
func AdminCLIFlagsHeading() Message {
	return Message{ID: MessageAdminCLIFlagsHeading, Fallback: "Flags:"}
}
func AdminCLIGlobalFlagsHeading() Message {
	return Message{ID: MessageAdminCLIGlobalFlagsHeading, Fallback: "Global Flags:"}
}
func AdminCLIAdditionalHelpTopicsHeading() Message {
	return Message{ID: MessageAdminCLIAdditionalHelpTopicsHeading, Fallback: "Additional help topics:"}
}
func AdminCLIHelpMoreInformation(commandPath string) Message {
	return Message{ID: MessageAdminCLIHelpMoreInformation, Fallback: "Use \"" + commandPath + " [command] --help\" for more information about a command.", Data: map[string]any{"CommandPath": commandPath}}
}
func AdminCLIHelpCommandShort() Message {
	return Message{ID: MessageAdminCLIHelpCommandShort, Fallback: "Help about any command"}
}
func AdminCLIHelpCommandLong(command string) Message {
	return Message{ID: MessageAdminCLIHelpCommandLong, Fallback: "Help provides help for any command in the application.\nSimply type " + command + " help [path to command] for full details.", Data: map[string]any{"Command": command}}
}
func AdminCLIHelpFlag(command string) Message {
	return Message{ID: MessageAdminCLIHelpFlag, Fallback: "help for " + command, Data: map[string]any{"Command": command}}
}
func AdminCLICompletionCommandShort() Message {
	return Message{ID: MessageAdminCLICompletionCommandShort, Fallback: "Generate the autocompletion script for the specified shell"}
}
func AdminCLICompletionCommandLong(command string) Message {
	return Message{ID: MessageAdminCLICompletionCommandLong, Fallback: "Generate the autocompletion script for " + command + " for the specified shell.\nSee each sub-command's help for details on how to use the generated script.\n", Data: map[string]any{"Command": command}}
}
