package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestExitCodeForError_UsesImportExitCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "nil", err: nil, want: adminimport.ExitOK},
		{name: "plain error", err: errors.New("boom"), want: 1},
		{name: "csv", err: &adminimport.Error{ExitCode: adminimport.ExitCSV, Message: "csv"}, want: adminimport.ExitCSV},
		{name: "wrapped", err: errors.New((&adminimport.Error{ExitCode: adminimport.ExitDuplicateID, Message: "dup"}).Error()), want: 1},
		{name: "wrapped as", err: wrapErr(&adminimport.Error{ExitCode: adminimport.ExitBadRelationship, Message: "bad"}), want: adminimport.ExitBadRelationship},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exitCodeForError(tt.err); got != tt.want {
				t.Fatalf("exitCodeForError() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestRenderCommandError_LocalizesImportMessageAndPreservesExitCode(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)
	cause := errors.New("permission denied")
	importErr := adminimport.NewLocalizedError(adminimport.ExitCSV, localization.AdminImportOpenCSVFailed(), cause)

	require.Equal(t, "no se pudo abrir el archivo CSV: permission denied", renderCommandError(manager, importErr))
	require.Equal(t, adminimport.ExitCSV, exitCodeForError(importErr))
}

func TestRootHelp_LocalizesCommandAndFlagDescriptions(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)

	command := newRootCmdWithManager(manager)
	output := new(bytes.Buffer)
	command.SetOut(output)
	command.SetArgs([]string{"--help"})

	require.NoError(t, command.Execute())
	require.Contains(t, output.String(), "Herramientas administrativas para NornicDB")
	require.Contains(t, output.String(), "Comandos disponibles:")
	require.Contains(t, output.String(), "completion  Generar el script de autocompletado para el shell especificado")
	require.Contains(t, output.String(), "help        Ayuda sobre cualquier comando")
	require.Contains(t, output.String(), "Directorio de datos de destino")
	require.Contains(t, output.String(), "ayuda para nornicdb-admin")
	require.Contains(t, output.String(), `Use "nornicdb-admin [command] --help" para obtener más información sobre un comando.`)
}

func TestRootHelp_PreservesExactEnglish(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)

	command := newRootCmdWithManager(manager)
	output := new(bytes.Buffer)
	command.SetOut(output)
	command.SetArgs([]string{"--help"})

	require.NoError(t, command.Execute())
	require.Equal(t, `Administrative tools for NornicDB

Usage:
  nornicdb-admin [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  database    Database administration commands
  help        Help about any command
  server      Server commands
  version     Print version

Flags:
      --data-dir string   Target data directory (default "./data")
  -h, --help              help for nornicdb-admin

Use "nornicdb-admin [command] --help" for more information about a command.
`, output.String())
}

func TestCommandHelp_PreservesMultilineAndFlagAlignment(t *testing.T) {
	pseudoTag := language.MustParse("en-XA")
	manager, err := localization.NewManager([]language.Tag{pseudoTag}, nil)
	require.NoError(t, err)

	command := newRootCmdWithManager(manager)
	output := new(bytes.Buffer)
	command.SetOut(output)
	command.SetArgs([]string{"database", "import", "full", "--help"})
	require.NoError(t, command.Execute())

	help := output.String()
	require.Contains(t, help, "[!! Run a full offline import !!]")
	require.Contains(t, help, "nornicdb-admin database import full <db-name> [flags]")
	require.Contains(t, help, "[!! Flags: !!]")
	require.Contains(t, help, "[!! Global Flags: !!]")
	require.Contains(t, help, "[!! Skip relationships that reference missing nodes !!]")

	nodesLine := lineContaining(t, help, "--nodes")
	relationshipsLine := lineContaining(t, help, "--relationships")
	require.Equal(t, strings.Index(nodesLine, "[!!"), strings.Index(relationshipsLine, "[!!"), "translated flag descriptions must remain column-aligned")

	command = newRootCmdWithManager(manager)
	output.Reset()
	command.SetOut(output)
	command.SetArgs([]string{"help", "help"})
	require.NoError(t, command.Execute())
	require.Contains(t, output.String(), "[!! Help provides help for any command in the application.\nSimply type nornicdb-admin help [path to command] for full details. !!]")
}

func TestCommandErrors_LocalizeWithoutChangingExitCodes(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "incremental not implemented", args: []string{"database", "import", "incremental", "mydb"}, want: "la importación incremental de bases de datos aún no está implementada"},
		{name: "database info not implemented", args: []string{"database", "info", "mydb"}, want: "la información de la base de datos aún no está implementada"},
		{name: "server status not implemented", args: []string{"server", "status"}, want: "el estado del servidor aún no está implementado"},
		{name: "required export path", args: []string{"database", "export", "neo4j-csv", "mydb"}, want: "se requiere --to-path"},
		{name: "exact arguments", args: []string{"database", "import", "full"}, want: "acepta 1 argumento(s), se recibieron 0"},
		{name: "no arguments", args: []string{"server", "status", "extra"}, want: `comando desconocido "extra" para "nornicdb-admin server status"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := newRootCmdWithManager(manager)
			command.SilenceErrors = true
			command.SilenceUsage = true
			command.SetArgs(test.args)

			executeErr := command.Execute()
			require.Error(t, executeErr)
			require.Equal(t, test.want, renderCommandError(manager, executeErr))
			require.Equal(t, 1, exitCodeForError(executeErr))
		})
	}
}

func TestCommandErrors_PreserveExactEnglish(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{args: []string{"database", "import", "incremental", "mydb"}, want: "database import incremental is not implemented yet"},
		{args: []string{"database", "import", "full"}, want: "accepts 1 arg(s), received 0"},
		{args: []string{"database", "export", "neo4j-csv", "mydb"}, want: "--to-path is required"},
		{args: []string{"server", "status", "extra"}, want: `unknown command "extra" for "nornicdb-admin server status"`},
	}
	for _, test := range tests {
		command := newRootCmd()
		command.SilenceErrors = true
		command.SilenceUsage = true
		command.SetArgs(test.args)

		executeErr := command.Execute()
		require.Error(t, executeErr)
		require.Equal(t, test.want, renderCommandError(nil, executeErr))
		require.Equal(t, 1, exitCodeForError(executeErr))
	}
}

func TestRootCommand_PreservesUseSyntaxAndFlagNames(t *testing.T) {
	command := newRootCmd()
	require.Equal(t, "nornicdb-admin", command.Use)

	full, _, err := command.Find([]string{"database", "import", "full"})
	require.NoError(t, err)
	require.Equal(t, "full <db-name>", full.Use)
	for _, name := range []string{"nodes", "relationships", "from-path", "report-file", "id-type", "delimiter"} {
		require.NotNil(t, full.Flags().Lookup(name), "flag %s must remain available", name)
	}

	export, _, err := command.Find([]string{"database", "export", "neo4j-csv"})
	require.NoError(t, err)
	require.Equal(t, "neo4j-csv <db-name>", export.Use)
	require.NotNil(t, export.Flags().Lookup("to-path"))
}

func lineContaining(t *testing.T, text, fragment string) string {
	t.Helper()
	for line := range strings.SplitSeq(text, "\n") {
		if strings.Contains(line, fragment) {
			return line
		}
	}
	t.Fatalf("output does not contain line with %q", fragment)
	return ""
}

func wrapErr(err error) error {
	return &wrapped{err: err}
}

type wrapped struct {
	err error
}

func (w *wrapped) Error() string { return "wrapped: " + w.err.Error() }
func (w *wrapped) Unwrap() error { return w.err }
