package cypher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

func TestAdminCredentialErrorOutput(t *testing.T) {
	const password = "synthetic admin output 812469"
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	if err != nil {
		t.Fatal("manager setup failed")
	}
	defer manager.Close()
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	commands := []struct {
		name   string
		prefix string
		tail   string
		id     localization.MessageID
		run    func(context.Context, string) (*ExecuteResult, error)
	}{
		{"create_alias", "CREATE ALIAS rejected FOR DATABASE nornic", "", localization.MessageCypherAdminInvalidDatabaseName, executor.executeCreateAlias},
		{"create_database", "CREATE DATABASE rejected", "", localization.MessageCypherAdminInvalidDatabaseName, func(ctx context.Context, query string) (*ExecuteResult, error) {
			return executor.executeDatabaseCreate(ctx, query, createNewDatabase)
		}},
		{"replace_database", "CREATE OR REPLACE DATABASE `broken`name", "`", localization.MessageCypherAdminInvalidIdentifier, func(ctx context.Context, query string) (*ExecuteResult, error) {
			return executor.executeDatabaseCreate(ctx, query, replaceExistingDatabase)
		}},
		{"drop_database", "DROP DATABASE rejected", "", localization.MessageCypherAdminInvalidDatabaseName, func(ctx context.Context, query string) (*ExecuteResult, error) {
			return executor.executeDatabaseDrop(ctx, query, dropStandardDatabase)
		}},
		{"drop_alias", "DROP ALIAS rejected", "", localization.MessageCypherAdminInvalidAliasName, executor.executeDropAlias},
		{"alter_database", "ALTER DATABASE rejected", " SET LIMIT max_nodes = 1", localization.MessageCypherAdminDatabaseNotFound, executor.executeAlterDatabase},
		{"show_limits", "SHOW LIMITS FOR DATABASE rejected", "", localization.MessageCypherAdminDatabaseNotFound, executor.executeShowLimits},
	}
	for _, command := range commands {
		for _, quote := range []string{"'", "\""} {
			t.Run(command.name+"/"+quote, func(t *testing.T) {
				query := command.prefix + " AT 'http://127.0.0.1:1' USER admin PASSWORD " + quote + password + quote + command.tail
				_, err := command.run(context.Background(), query)
				var localized *localization.LocalizedError
				if err == nil || !errors.As(err, &localized) || localized.Message.ID != command.id {
					t.Fatal("admin error identity changed; details withheld")
				}
				for _, output := range []string{err.Error(), localized.Message.Fallback, fmt.Sprint(localized.Message.Data)} {
					for _, secret := range []string{password, strings.ReplaceAll(password, " ", "")} {
						if strings.Contains(output, secret) {
							t.Error("admin error disclosed a synthetic password; value withheld")
						}
					}
				}
			})
		}
	}
	// Alias commands reject whitespace in the appended target/alias name.
	// A valid quoted name with URI credentials reaches the manager cause boundary.
	// The display wrapper must retain the original cause and localized identity.
	cause := errors.New("manager rejected synthetic-cause-748321")
	executor.SetDatabaseManager(newCypherAdminErrorManager(t, testCatalogCreateAlias, cause))
	_, err = executor.executeCreateAlias(context.Background(), "CREATE ALIAS rejected FOR DATABASE `http://admin:synthetic-cause-748321@example.invalid`")
	var localizedCause *localization.LocalizedError
	if !errors.As(err, &localizedCause) || localizedCause.Message.ID != localization.MessageCypherAdminCreateAliasFailed {
		t.Error("manager cause lost localized identity")
	}
	if !errors.Is(err, cause) || strings.Contains(err.Error(), "synthetic-cause-748321") {
		t.Error("admin error lost cause classification or disclosed a password; details withheld")
	}
}
