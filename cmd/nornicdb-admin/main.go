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
	"github.com/orneryd/nornicdb/pkg/storage"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(exitCodeForError(err))
	}
}

func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "nornicdb-admin",
		Short: "Administrative tools for NornicDB",
	}

	dataDir := rootCmd.PersistentFlags().String("data-dir", "./data", "Target data directory")

	databaseCmd := &cobra.Command{Use: "database", Short: "Database administration commands"}
	importCmd := &cobra.Command{Use: "import", Short: "Import CSV data into an offline database"}

	fullCmd := &cobra.Command{
		Use:   "full <db-name>",
		Short: "Run a full offline import",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runImportFull(args[0], *dataDir, cmd)
		},
	}
	fullCmd.Flags().StringSlice("nodes", nil, "Node CSV source (repeatable)")
	fullCmd.Flags().StringSlice("relationships", nil, "Relationship CSV source (repeatable)")
	fullCmd.Flags().String("from-path", "", "Directory containing Neo4j-compatible CSV files")
	fullCmd.Flags().String("schema", "", "Cypher schema file to apply after load")
	fullCmd.Flags().Bool("build-indexes", true, "Build search indexes after import")
	fullCmd.Flags().Bool("skip-bad-relationships", false, "Skip relationships that reference missing nodes")
	fullCmd.Flags().Bool("skip-duplicate-nodes", false, "Skip duplicate node IDs")
	fullCmd.Flags().Bool("normalize-types", true, "Normalize imported property values")
	fullCmd.Flags().Bool("ignore-extra-columns", false, "Ignore extra CSV columns")
	fullCmd.Flags().Bool("ignore-empty-strings", false, "Treat empty strings as null")
	fullCmd.Flags().String("report-file", "", "Write a JSON report")
	fullCmd.Flags().String("id-type", "string", "ID type: string or integer")
	fullCmd.Flags().Int("bad-tolerance", 0, "Number of bad rows tolerated before abort")
	fullCmd.Flags().Int("chunk-size", 1000, "Rows per bulk write chunk")
	fullCmd.Flags().String("delimiter", ",", "Field delimiter")
	fullCmd.Flags().String("array-delimiter", ";", "Array delimiter")
	fullCmd.Flags().String("vector-delimiter", ";", "Vector delimiter")
	fullCmd.Flags().String("quote", "\"", "Quote character")
	fullCmd.Flags().String("constraints-file", "", "Deprecated alias for --schema")
	fullCmd.Flags().Bool("verbose", false, "Verbose logging")

	incrementalCmd := &cobra.Command{
		Use:   "incremental <db-name>",
		Short: "Reserved incremental import command",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("database import incremental is not implemented yet")
		},
	}

	importCmd.AddCommand(fullCmd, incrementalCmd)
	exportCmd := &cobra.Command{Use: "export", Short: "Export database data for offline migration"}
	exportNeo4jCSVCmd := &cobra.Command{
		Use:   "neo4j-csv <db-name>",
		Short: "Export a database as Neo4j-compatible CSV files",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExportNeo4jCSV(args[0], *dataDir, cmd)
		},
	}
	exportNeo4jCSVCmd.Flags().String("to-path", "", "Output directory for Neo4j-compatible CSV files")
	exportNeo4jCSVCmd.Flags().String("delimiter", ",", "Field delimiter")
	exportNeo4jCSVCmd.Flags().String("array-delimiter", ";", "Array delimiter")
	exportNeo4jCSVCmd.Flags().String("vector-delimiter", ";", "Vector delimiter")
	exportNeo4jCSVCmd.Flags().String("quote", "\"", "Quote character")
	exportCmd.AddCommand(exportNeo4jCSVCmd)
	databaseCmd.AddCommand(importCmd)
	databaseCmd.AddCommand(exportCmd)
	databaseCmd.AddCommand(&cobra.Command{Use: "info <db-name>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("database info is not implemented yet")
	}})
	rootCmd.AddCommand(databaseCmd)
	serverCmd := &cobra.Command{Use: "server", Short: "Server commands"}
	serverCmd.AddCommand(&cobra.Command{Use: "status", Short: "Show server status", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("server status is not implemented yet")
	}})
	rootCmd.AddCommand(serverCmd)
	rootCmd.AddCommand(&cobra.Command{Use: "version", Short: "Print version", Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(buildinfo.DisplayVersion())
	}})

	return rootCmd
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
		return fmt.Errorf("--to-path is required")
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
