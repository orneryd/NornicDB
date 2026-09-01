// Package main provides the NornicDB admin CLI entrypoint.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func main() {
	localizer, err := newCommandLocalizer()
	if err != nil {
		fmt.Fprintln(os.Stderr, localization.AdminCLIErrorPrefix().Fallback, err)
		os.Exit(1)
	}
	rootCmd := newRootCmdWithManager(localizer)
	rootCmd.SilenceErrors = true
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(rootCmd.ErrOrStderr(), renderCommandMessage(localizer, localization.AdminCLIErrorPrefix()), renderCommandError(localizer, err))
		os.Exit(exitCodeForError(err))
	}
}

func newCommandLocalizer() (*localization.Manager, error) {
	preferences, err := localization.ResolveProcessPreferences(localization.AutoLanguage)
	if err != nil {
		return nil, err
	}
	return localization.NewManager(preferences.Preferences, nil)
}

func renderCommandError(manager *localization.Manager, err error) string {
	var commandErr *localizedCommandError
	if errors.As(err, &commandErr) {
		return renderCommandMessage(manager, commandErr.message)
	}
	var importErr *adminimport.Error
	if !errors.As(err, &importErr) || importErr.LocalizedMessage.ID == "" {
		return err.Error()
	}
	text := renderCommandMessage(manager, importErr.LocalizedMessage)
	if importErr.Err != nil {
		text += ": " + importErr.Err.Error()
	}
	return text
}

func renderCommandMessage(manager *localization.Manager, message localization.Message) string {
	if manager != nil {
		if rendered, _, err := manager.Render(context.Background(), message); err == nil {
			return rendered
		}
	}
	return message.Fallback
}

type localizedCommandError struct {
	message localization.Message
}

func (e *localizedCommandError) Error() string {
	return e.message.Fallback
}

func newLocalizedCommandError(message localization.Message) error {
	return &localizedCommandError{message: message}
}

func newRootCmd() *cobra.Command {
	return newRootCmdWithManager(nil)
}

func newRootCmdWithManager(manager *localization.Manager) *cobra.Command {
	text := func(message localization.Message) string {
		return renderCommandMessage(manager, message)
	}
	rootCmd := &cobra.Command{
		Use:   "nornicdb-admin",
		Short: text(localization.AdminCLIRootShort()),
	}

	dataDir := rootCmd.PersistentFlags().String("data-dir", "./data", text(localization.AdminCLIDataDirectoryFlag()))

	databaseCmd := &cobra.Command{Use: "database", Short: text(localization.AdminCLIDatabaseShort())}
	importCmd := &cobra.Command{Use: "import", Short: text(localization.AdminCLIImportShort())}

	fullCmd := &cobra.Command{
		Use:   "full <db-name>",
		Short: text(localization.AdminCLIFullImportShort()),
		Args:  localizedExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runImportFull(args[0], *dataDir, cmd)
		},
	}
	fullCmd.Flags().StringSlice("nodes", nil, text(localization.AdminCLINodesFlag()))
	fullCmd.Flags().StringSlice("relationships", nil, text(localization.AdminCLIRelationshipsFlag()))
	fullCmd.Flags().String("from-path", "", text(localization.AdminCLIFromPathFlag()))
	fullCmd.Flags().String("schema", "", text(localization.AdminCLISchemaFlag()))
	fullCmd.Flags().Bool("build-indexes", true, text(localization.AdminCLIBuildIndexesFlag()))
	fullCmd.Flags().Bool("skip-bad-relationships", false, text(localization.AdminCLISkipBadRelationshipsFlag()))
	fullCmd.Flags().Bool("skip-duplicate-nodes", false, text(localization.AdminCLISkipDuplicateNodesFlag()))
	fullCmd.Flags().Bool("normalize-types", true, text(localization.AdminCLINormalizeTypesFlag()))
	fullCmd.Flags().Bool("ignore-extra-columns", false, text(localization.AdminCLIIgnoreExtraColumnsFlag()))
	fullCmd.Flags().Bool("ignore-empty-strings", false, text(localization.AdminCLIIgnoreEmptyStringsFlag()))
	fullCmd.Flags().String("report-file", "", text(localization.AdminCLIReportFileFlag()))
	fullCmd.Flags().String("id-type", "string", text(localization.AdminCLIIDTypeFlag()))
	fullCmd.Flags().Int("bad-tolerance", 0, text(localization.AdminCLIBadToleranceFlag()))
	fullCmd.Flags().Int("chunk-size", 1000, text(localization.AdminCLIChunkSizeFlag()))
	fullCmd.Flags().String("delimiter", ",", text(localization.AdminCLIDelimiterFlag()))
	fullCmd.Flags().String("array-delimiter", ";", text(localization.AdminCLIArrayDelimiterFlag()))
	fullCmd.Flags().String("vector-delimiter", ";", text(localization.AdminCLIVectorDelimiterFlag()))
	fullCmd.Flags().String("quote", "\"", text(localization.AdminCLIQuoteFlag()))
	fullCmd.Flags().String("constraints-file", "", text(localization.AdminCLIConstraintsFileFlag()))
	fullCmd.Flags().Bool("verbose", false, text(localization.AdminCLIVerboseFlag()))

	incrementalCmd := &cobra.Command{
		Use:   "incremental <db-name>",
		Short: text(localization.AdminCLIIncrementalImportShort()),
		Args:  localizedExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return newLocalizedCommandError(localization.AdminCLIIncrementalNotImplemented())
		},
	}

	importCmd.AddCommand(fullCmd, incrementalCmd)
	exportCmd := &cobra.Command{Use: "export", Short: text(localization.AdminCLIExportShort())}
	exportNeo4jCSVCmd := &cobra.Command{
		Use:   "neo4j-csv <db-name>",
		Short: text(localization.AdminCLINeo4jCSVExportShort()),
		Args:  localizedExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExportNeo4jCSV(args[0], *dataDir, cmd)
		},
	}
	exportNeo4jCSVCmd.Flags().String("to-path", "", text(localization.AdminCLIToPathFlag()))
	exportNeo4jCSVCmd.Flags().String("delimiter", ",", text(localization.AdminCLIDelimiterFlag()))
	exportNeo4jCSVCmd.Flags().String("array-delimiter", ";", text(localization.AdminCLIArrayDelimiterFlag()))
	exportNeo4jCSVCmd.Flags().String("vector-delimiter", ";", text(localization.AdminCLIVectorDelimiterFlag()))
	exportNeo4jCSVCmd.Flags().String("quote", "\"", text(localization.AdminCLIQuoteFlag()))
	exportCmd.AddCommand(exportNeo4jCSVCmd)
	databaseCmd.AddCommand(importCmd)
	databaseCmd.AddCommand(exportCmd)
	databaseCmd.AddCommand(&cobra.Command{Use: "info <db-name>", Args: localizedExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return newLocalizedCommandError(localization.AdminCLIDatabaseInfoNotImplemented())
	}})
	rootCmd.AddCommand(databaseCmd)
	serverCmd := &cobra.Command{Use: "server", Short: text(localization.AdminCLIServerShort())}
	serverCmd.AddCommand(&cobra.Command{Use: "status", Short: text(localization.AdminCLIServerStatusShort()), Args: localizedNoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		return newLocalizedCommandError(localization.AdminCLIServerStatusNotImplemented())
	}})
	rootCmd.AddCommand(serverCmd)
	rootCmd.AddCommand(&cobra.Command{Use: "version", Short: text(localization.AdminCLIVersionShort()), Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(buildinfo.DisplayVersion())
	}})
	localizeCobraHelp(rootCmd, manager)

	return rootCmd
}

func localizedExactArgs(expected int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != expected {
			return newLocalizedCommandError(localization.AdminCLIExactArgs(expected, len(args)))
		}
		return nil
	}
}

func localizedNoArgs(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return newLocalizedCommandError(localization.AdminCLIUnknownCommand(args[0], cmd.CommandPath()))
	}
	return nil
}

func localizeCobraHelp(root *cobra.Command, manager *localization.Manager) {
	text := func(message localization.Message) string {
		return renderCommandMessage(manager, message)
	}
	root.SetUsageTemplate(localizedUsageTemplate(manager))
	root.InitDefaultHelpCmd()
	root.InitDefaultCompletionCmd()
	for _, command := range root.Commands() {
		switch command.Name() {
		case "help":
			command.Short = text(localization.AdminCLIHelpCommandShort())
			command.Long = text(localization.AdminCLIHelpCommandLong(root.DisplayName()))
		case "completion":
			command.Short = text(localization.AdminCLICompletionCommandShort())
			command.Long = text(localization.AdminCLICompletionCommandLong(root.DisplayName()))
			command.Args = localizedNoArgs
		}
	}
	localizeHelpFlags(root, manager)
}

func localizeHelpFlags(command *cobra.Command, manager *localization.Manager) {
	command.InitDefaultHelpFlag()
	if helpFlag := command.Flags().Lookup("help"); helpFlag != nil {
		helpFlag.Usage = renderCommandMessage(manager, localization.AdminCLIHelpFlag(command.DisplayName()))
	}
	for _, child := range command.Commands() {
		localizeHelpFlags(child, manager)
	}
}

func localizedUsageTemplate(manager *localization.Manager) string {
	text := func(message localization.Message) string {
		return renderCommandMessage(manager, message)
	}
	return text(localization.AdminCLIUsageHeading()) + `{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

` + text(localization.AdminCLIAliasesHeading()) + `
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

` + text(localization.AdminCLIExamplesHeading()) + `
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}{{$cmds := .Commands}}{{if eq (len .Groups) 0}}

` + text(localization.AdminCLIAvailableCommandsHeading()) + `{{range $cmds}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{else}}{{range $group := .Groups}}

{{.Title}}{{range $cmds}}{{if (and (eq .GroupID $group.ID) (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if not .AllChildCommandsHaveGroup}}

` + text(localization.AdminCLIAdditionalCommandsHeading()) + `{{range $cmds}}{{if (and (eq .GroupID "") (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

` + text(localization.AdminCLIFlagsHeading()) + `
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

` + text(localization.AdminCLIGlobalFlagsHeading()) + `
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

` + text(localization.AdminCLIAdditionalHelpTopicsHeading()) + `{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

` + text(localization.AdminCLIHelpMoreInformation("{{.CommandPath}}")) + `{{end}}
`
}

func runImportFull(dbName string, dataDir string, cmd *cobra.Command) error {
	nodes, _ := cmd.Flags().GetStringSlice("nodes")
	rels, _ := cmd.Flags().GetStringSlice("relationships")
	fromPath, _ := cmd.Flags().GetString("from-path")
	reportFile, _ := cmd.Flags().GetString("report-file")
	schemaFile, _ := cmd.Flags().GetString("schema")
	idType, _ := cmd.Flags().GetString("id-type")
	delimiter, _ := cmd.Flags().GetString("delimiter")
	arrayDelimiter, _ := cmd.Flags().GetString("array-delimiter")
	vectorDelimiter, _ := cmd.Flags().GetString("vector-delimiter")
	quote, _ := cmd.Flags().GetString("quote")
	chunkSize, _ := cmd.Flags().GetInt("chunk-size")
	normalize, _ := cmd.Flags().GetBool("normalize-types")
	ignoreExtra, _ := cmd.Flags().GetBool("ignore-extra-columns")
	ignoreEmpty, _ := cmd.Flags().GetBool("ignore-empty-strings")
	buildIndexes, _ := cmd.Flags().GetBool("build-indexes")
	skipBad, _ := cmd.Flags().GetBool("skip-bad-relationships")
	skipDup, _ := cmd.Flags().GetBool("skip-duplicate-nodes")
	verbose, _ := cmd.Flags().GetBool("verbose")
	badTol, _ := cmd.Flags().GetInt("bad-tolerance")
	constraintsFile, _ := cmd.Flags().GetString("constraints-file")
	if schemaFile == "" {
		schemaFile = constraintsFile
	}
	if fromPath != "" && schemaFile == "" {
		candidate := adminimport.DefaultNeo4jCSVNornicSchemaPath(fromPath)
		if _, err := os.Stat(candidate); err == nil {
			schemaFile = candidate
		} else {
			candidate = adminimport.DefaultNeo4jCSVSchemaPath(fromPath)
			if _, err := os.Stat(candidate); err == nil {
				schemaFile = candidate
			}
		}
	}
	if fromPath != "" {
		discoveredNodes, discoveredRels, err := adminimport.DiscoverNeo4jCSVSources(fromPath, adminimport.Options{
			Delimiter:       firstRune(delimiter, ','),
			ArrayDelimiter:  firstRune(arrayDelimiter, ';'),
			VectorDelimiter: firstRune(vectorDelimiter, ';'),
			Quote:           firstRune(quote, '"'),
		})
		if err != nil {
			return err
		}
		nodes = append(nodes, discoveredNodes...)
		rels = append(rels, discoveredRels...)
	}

	engine, err := storage.NewBadgerEngine(dataDir)
	if err != nil {
		return err
	}
	defer engine.Close()

	report, err := adminimport.ImportFull(context.Background(), engine, adminimport.Options{
		DatabaseName:         dbName,
		NodeSources:          nodes,
		RelSources:           rels,
		DataDir:              dataDir,
		Delimiter:            firstRune(delimiter, ','),
		ArrayDelimiter:       firstRune(arrayDelimiter, ';'),
		VectorDelimiter:      firstRune(vectorDelimiter, ';'),
		Quote:                firstRune(quote, '"'),
		IDType:               idType,
		NormalizeTypes:       normalize,
		IgnoreExtraColumns:   ignoreExtra,
		IgnoreEmptyStrings:   ignoreEmpty,
		BadTolerance:         badTol,
		SkipBadRelationships: skipBad,
		SkipDuplicateNodes:   skipDup,
		ReportFile:           reportFile,
		SchemaFile:           schemaFile,
		BuildIndexes:         buildIndexes,
		ChunkSize:            chunkSize,
		Verbose:              verbose,
	})
	if err != nil {
		return err
	}
	_ = report
	return nil
}

func runExportNeo4jCSV(dbName string, dataDir string, cmd *cobra.Command) error {
	toPath, _ := cmd.Flags().GetString("to-path")
	delimiter, _ := cmd.Flags().GetString("delimiter")
	arrayDelimiter, _ := cmd.Flags().GetString("array-delimiter")
	vectorDelimiter, _ := cmd.Flags().GetString("vector-delimiter")
	quote, _ := cmd.Flags().GetString("quote")
	if strings.TrimSpace(toPath) == "" {
		return newLocalizedCommandError(localization.AdminCLIToPathRequired())
	}

	engine, err := storage.NewBadgerEngine(dataDir)
	if err != nil {
		return err
	}
	defer engine.Close()

	namespaced := storage.NewNamespacedEngine(engine, dbName)
	return adminimport.ExportNeo4jCSV(namespaced, adminimport.Neo4jCSVExportOptions{
		OutputDir:       toPath,
		Delimiter:       firstRune(delimiter, ','),
		ArrayDelimiter:  firstRune(arrayDelimiter, ';'),
		VectorDelimiter: firstRune(vectorDelimiter, ';'),
		Quote:           firstRune(quote, '"'),
	})
}

func firstRune(value string, fallback rune) rune {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return []rune(value)[0]
}

func exitCodeForError(err error) int {
	if err == nil {
		return adminimport.ExitOK
	}
	var importErr *adminimport.Error
	if errors.As(err, &importErr) {
		if importErr.ExitCode > 0 {
			return importErr.ExitCode
		}
	}
	return 1
}
