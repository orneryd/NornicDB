package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestRootCommandHelpUsesResolvedLanguage(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)

	command := newRootCommand(manager)
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"--help"})
	require.NoError(t, command.Execute())

	help := output.String()
	require.Equal(t, "NornicDB - Base de datos de grafos de alto rendimiento para agentes LLM", command.Short)
	require.Contains(t, help, "base de datos de grafos especializada")
	require.Contains(t, help, "Inicia el servidor NornicDB")
	require.Contains(t, help, "Ruta al archivo de configuración YAML")
}

func TestRootCommandEnglishHelpRemainsExact(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)

	command := newRootCommand(manager)
	require.Equal(t, "NornicDB - High-Performance Graph Database for LLM Agents", command.Short)
	require.Equal(t, `NornicDB is a purpose-built graph database written in Go,
designed for AI agent memory with Neo4j Bolt/Cypher compatibility.

Features:
  • Neo4j Bolt protocol compatibility
  • Cypher query language support
  • Knowledge-layer scoring with declarative decay profiles
  • Automatic relationship inference
  • Built-in vector search with RRF hybrid ranking
  • Server-side embedding generation`, command.Long)

	serve, _, err := command.Find([]string{"serve"})
	require.NoError(t, err)
	require.Equal(t, "Start NornicDB server", serve.Short)
	require.Equal(t, "Start NornicDB server with Bolt protocol and HTTP API endpoints", serve.Long)
	require.Equal(t, "Bolt protocol port (Neo4j compatible)", serve.Flag("bolt-port").Usage)
	require.Equal(t, "Path to YAML config file (overrides auto-discovery)", command.Flag("config").Usage)
}

func TestPseudoLocalizedHelpKeepsMultilineLayout(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.MustParse("en-XA")}, nil)
	require.NoError(t, err)

	command := newRootCommand(manager)
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"serve", "--help"})
	require.NoError(t, command.Execute())

	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	require.Greater(t, len(lines), 20)
	for _, line := range lines {
		require.NotContains(t, line, "\t", "Cobra help must use stable space-based columns")
		require.LessOrEqual(t, len([]rune(line)), 160, "localized help line should remain independently readable")
	}
	require.Contains(t, output.String(), "[!!")
	require.Contains(t, output.String(), "--embedding-provider")
}

func TestLocalizedValidationErrorPreservesCause(t *testing.T) {
	english, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	spanish, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)
	cause := errors.New("sentinel validation cause")
	command := newRootCommand(english)

	err = newCommandError(command, localization.NornicDBCLIInvalidMemoryLimit("bad", cause), cause)
	require.ErrorIs(t, err, cause)
	require.Equal(t, `invalid --memory-limit value "bad": sentinel validation cause`, err.Error())

	setCommandLocalizer(command, spanish)
	require.Equal(t, `valor no válido de --memory-limit "bad": sentinel validation cause`, err.Error())

	var localized *commandError
	require.ErrorAs(t, err, &localized)
	require.Equal(t, localization.MessageNornicDBCLIInvalidMemoryLimit, localized.message.ID)
}

func TestConfigSelectedLocaleRendersLaterCommandError(t *testing.T) {
	english, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	configPath := filepath.Join(t.TempDir(), "nornicdb.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("localization:\n  language: es-ES\n"), 0o600))

	command := newRootCommand(english)
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"serve", "--config", configPath, "--no-auth", "--memory-limit", "bad"})
	err = command.Execute()

	require.Error(t, err)
	require.Contains(t, err.Error(), `valor no válido de --memory-limit "bad"`)
	require.Contains(t, output.String(), "Configuración cargada desde:")
	var localized *commandError
	require.ErrorAs(t, err, &localized)
	require.Equal(t, localization.MessageNornicDBCLIInvalidMemoryLimit, localized.message.ID)
	require.ErrorIs(t, err, errors.Unwrap(err))
}

func TestDirectCLIErrorMessagesPreserveIdentityCauseAndLocales(t *testing.T) {
	cause := errors.New("disk unavailable")
	tests := []struct {
		name    string
		message localization.Message
		id      localization.MessageID
		english string
		spanish string
	}{
		{"explicit config", localization.NornicDBCLIConfigLoadFailed("/tmp/nornicdb.yaml", cause), localization.MessageNornicDBCLIConfigLoadFailed, "failed to load config from /tmp/nornicdb.yaml: disk unavailable", "no se pudo cargar la configuración desde /tmp/nornicdb.yaml: disk unavailable"},
		{"create data directory", localization.NornicDBCLICreateDataDirectoryFailed(cause), localization.MessageNornicDBCLICreateDataDirectoryFailed, "creating data directory: disk unavailable", "no se pudo crear el directorio de datos: disk unavailable"},
		{"open database", localization.NornicDBCLIOpenDatabaseFailed(cause), localization.MessageNornicDBCLIOpenDatabaseFailed, "opening database: disk unavailable", "no se pudo abrir la base de datos: disk unavailable"},
		{"start server", localization.NornicDBCLIStartServerFailed(cause), localization.MessageNornicDBCLIStartServerFailed, "starting server: disk unavailable", "no se pudo iniciar el servidor: disk unavailable"},
		{"initialize localization", localization.NornicDBCLILocalizationInitFailed(cause), localization.MessageNornicDBCLILocalizationInitFailed, "initialize localization: disk unavailable", "no se pudo inicializar la localización: disk unavailable"},
		{"bolt adapter", localization.NornicDBCLIBoltAdapterFailed(cause), localization.MessageNornicDBCLIBoltAdapterFailed, "bolt: disk unavailable", "Bolt: disk unavailable"},
		{"transaction missing", localization.NornicDBCLITransactionNotFound(), localization.MessageNornicDBCLITransactionNotFound, "transaction not found", "no se encontró la transacción"},
		{"write config", localization.NornicDBCLIWriteConfigFailed(cause), localization.MessageNornicDBCLIWriteConfigFailed, "writing config: disk unavailable", "no se pudo escribir la configuración: disk unavailable"},
		{"read shell input", localization.NornicDBCLIReadInputFailed(cause), localization.MessageNornicDBCLIReadInputFailed, "reading input: disk unavailable", "no se pudo leer la entrada: disk unavailable"},
		{"load decay nodes", localization.NornicDBCLILoadNodesFailed(cause), localization.MessageNornicDBCLILoadNodesFailed, "loading nodes: disk unavailable", "no se pudieron cargar los nodos: disk unavailable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.id, test.message.ID)
			require.Equal(t, test.english, test.message.Fallback)

			for _, locale := range []struct {
				tag  language.Tag
				want string
			}{
				{language.AmericanEnglish, test.english},
				{language.EuropeanSpanish, test.spanish},
			} {
				manager, err := localization.NewManager([]language.Tag{locale.tag}, nil)
				require.NoError(t, err)
				require.Equal(t, locale.want, commandText(manager, test.message))
			}

			pseudo, err := localization.NewManager([]language.Tag{language.MustParse("en-XA")}, nil)
			require.NoError(t, err)
			require.Contains(t, commandText(pseudo, test.message), "[!!")
			if strings.Contains(test.english, cause.Error()) {
				require.Contains(t, commandText(pseudo, test.message), cause.Error(), "diagnostic causes remain untranslated")
			}
		})
	}
}

func TestTransactionAdapterErrorsPreserveIdentityCauseAndLocale(t *testing.T) {
	spanish, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)

	executor := &DBQueryExecutor{
		localizer: spanish,
		txID:      "missing",
		txMgr: txsession.NewManager(time.Minute, func(string) (*cypher.StorageExecutor, error) {
			t.Fatal("missing transaction must fail before creating an executor")
			return nil, nil
		}),
	}
	_, err = executor.Execute(context.Background(), "RETURN 1", nil)
	require.EqualError(t, err, "no se encontró la transacción")
	require.ErrorIs(t, err, errors.Unwrap(err))
	var localized *commandError
	require.ErrorAs(t, err, &localized)
	require.Equal(t, localization.MessageNornicDBCLITransactionNotFound, localized.message.ID)

	executor.txID = "active"
	err = executor.BeginTransaction(context.Background(), nil)
	require.EqualError(t, err, "ya hay una transacción activa")
	require.ErrorIs(t, err, errors.Unwrap(err))

	_, err = newTxScopedExecutor(nil, "nornic")
	require.EqualError(t, err, "database is not initialized")
	require.ErrorIs(t, err, errors.Unwrap(err))
}

func TestEnglishEndpointSummaryRemainsExact(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)

	message := localization.NornicDBCLIEndpoints(
		"localhost",
		7474,
		7687,
		"http://localhost:9464/metrics",
		"http://localhost:6060/debug/pprof/",
		"http://localhost:7474/mcp",
		true,
	)
	require.Equal(t, `Endpoints:
  • HTTP API:     http://localhost:7474
  • Bolt:         bolt://localhost:7687
  • Health:       http://localhost:7474/health
  • Search:       POST http://localhost:7474/nornicdb/search
  • Cypher:       POST http://localhost:7474/db/nornicdb/tx/commit
  • MCP:          http://localhost:7474/mcp
  • Telemetry:    http://localhost:9464/metrics
  • pprof:        http://localhost:6060/debug/pprof/`, commandText(manager, message))
}
