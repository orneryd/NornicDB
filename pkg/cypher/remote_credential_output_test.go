package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type remoteOutputConstituents struct {
	*testDatabaseManagerAdapter
	refs []interface{}
}

func TestCompositeRemoteCredentialErrorOutput(t *testing.T) {
	const password = "synthetic-output-error-748296"
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	if err != nil {
		t.Fatal("manager setup failed")
	}
	fixtureowner.Own(t, manager)
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	for _, scheme := range []string{"http", "bolt"} {
		endpoint := scheme + "://admin:" + password + "@127.0.0.1:1"
		for _, operation := range []string{"CREATE", "ALTER"} {
			t.Run(scheme+"/"+operation, func(t *testing.T) {
				query := "CREATE COMPOSITE DATABASE output_error ALIAS r FOR DATABASE nornic '" + endpoint + "'"
				var err error
				if operation == "CREATE" {
					_, err = executor.executeCompositeCommand(context.Background(), query, compositeCreateCommand)
				} else {
					query = "ALTER COMPOSITE DATABASE output_error ADD ALIAS r FOR DATABASE nornic '" + endpoint + "'"
					_, err = executor.executeCompositeCommand(context.Background(), query, compositeAlterCommand)
				}
				var localized *localization.LocalizedError
				if err == nil || !errors.As(err, &localized) || localized.Message.ID != localization.MessageCypherCompositeConstituentUnexpectedToken {
					t.Fatal("unexpected error identity; details withheld")
				}
				for _, output := range []string{err.Error(), localized.Message.Fallback, fmt.Sprint(localized.Message.Data)} {
					if strings.Contains(output, password) {
						t.Error("composite error disclosed URI password; value withheld")
					}
				}
				// ANTLR can reject before the native command handler is entered.
				// Its validation diagnostic is a separate public output boundary.
				_, err = executor.Execute(context.Background(), query, nil)
				if err == nil || strings.Contains(err.Error(), password) {
					t.Error("public validation error missing or disclosed URI password; details withheld")
				}
			})
		}
	}
}

func (a *remoteOutputConstituents) GetCompositeConstituents(string) ([]interface{}, error) {
	return a.refs, nil
}

// Cover the command projections directly under both parser configurations.
// The public wire test separately pins the ANTLR remote-DDL grammar rejection.
func TestCompositeRemoteCredentialOutput(t *testing.T) {
	const uriPassword = "synthetic-output-uri-482617"
	const explicitPassword = "synthetic-output-explicit-936251"
	for _, scheme := range []string{"http", "bolt"} {
		for _, form := range []string{"explicit", "uri", "both"} {
			t.Run(scheme+"/"+form, func(t *testing.T) {
				cfg := multidb.DefaultConfig()
				cfg.RemoteCredentialEncryptionKey = "synthetic-output-key"
				manager, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), cfg)
				if err != nil {
					t.Fatal("manager setup failed; details withheld")
				}
				fixtureowner.Own(t, manager)
				engine, err := manager.GetStorage("nornic")
				if err != nil {
					t.Fatal("storage setup failed")
				}
				executor := NewStorageExecutor(engine)
				adapter := &testDatabaseManagerAdapter{manager: manager}
				executor.SetDatabaseManager(adapter)
				var logs bytes.Buffer
				executor.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
				executor.SetSlowQueryThreshold(time.Nanosecond)
				check := func(label string, value interface{}) {
					t.Helper()
					data, marshalErr := json.Marshal(value)
					if marshalErr != nil {
						t.Fatal("could not inspect output")
					}
					for _, secret := range []string{uriPassword, explicitPassword} {
						if bytes.Contains(data, []byte(secret)) {
							t.Error(label + " disclosed a synthetic password; value withheld")
						}
					}
				}
				ctx := context.Background()
				endpoint := scheme + "://127.0.0.1:1/db?region=west"
				if form != "explicit" {
					endpoint = scheme + "://admin:" + uriPassword + "@127.0.0.1:1/db?region=west"
				}
				suffix := ""
				if form != "uri" {
					suffix = " USER admin PASSWORD '" + explicitPassword + "'"
				}
				declaration := " FOR DATABASE nornic AT '" + endpoint + "'" + suffix + " TYPE remote ACCESS read"
				create := "CREATE COMPOSITE DATABASE output_test ALIAS seed" + declaration
				result, err := executor.executeCompositeCommand(ctx, create, compositeCreateCommand)
				check("CREATE result", result)
				if err != nil || result == nil || len(result.Rows) != 1 {
					t.Fatal("CREATE failed; details withheld")
				}
				alter := "ALTER COMPOSITE DATABASE output_test ADD ALIAS r" + declaration
				result, err = executor.executeCompositeCommand(ctx, alter, compositeAlterCommand)
				check("ALTER ADD result", result)
				if err != nil || result == nil || len(result.Rows) != 1 {
					t.Fatal("ALTER ADD failed; details withheld")
				}
				if result.Rows[0][5] != observability.RedactURL(endpoint) {
					t.Error("ALTER URI differs from SHOW's canonical display; value withheld")
				}
				// Loaded legacy records and map adapters may still contain userinfo.
				// Each SHOW representation must redact without changing its input.
				for _, representation := range []string{"typed", "map"} {
					var ref interface{} = multidb.ConstituentRef{Alias: "legacy", DatabaseName: "nornic", Type: "remote", URI: endpoint}
					if representation == "map" {
						ref = map[string]interface{}{"alias": "legacy", "database_name": "nornic", "type": "remote", "uri": endpoint}
					}
					executor.SetDatabaseManager(&remoteOutputConstituents{testDatabaseManagerAdapter: adapter, refs: []interface{}{ref}})
					result, err = executor.executeShowConstituents(ctx, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE output_test")
					if err != nil || result == nil || len(result.Rows) != 1 {
						t.Fatal("SHOW failed; details withheld")
					}
					check("SHOW "+representation, result)
					if result.Rows[0][4] != observability.RedactURL(endpoint) {
						t.Error("SHOW URI changed its display contract; value withheld")
					}
					if strings.Contains(fmt.Sprint(ref), "[REDACTED]") {
						t.Error("SHOW mutated stored metadata")
					}
				}
				executor.SetDatabaseManager(adapter)
				result, err = executor.executeShowCompositeDatabases(ctx, "SHOW COMPOSITE DATABASES")
				if err != nil {
					t.Fatal("SHOW databases failed")
				}
				check("SHOW databases", result)
				// SET is not an implemented constituent update path. Rejected DDL
				// and its real slow-query log must still keep URI credentials out.
				_, err = executor.Execute(ctx, "ALTER COMPOSITE DATABASE output_test SET ALIAS r AT '"+endpoint+"'", nil)
				if err == nil {
					t.Fatal("unsupported SET unexpectedly succeeded")
				}
				check("SET error", err.Error())
				check("slow-query log", logs.String())
				result, err = executor.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE output_test ADD ALIAS local FOR DATABASE nornic", compositeAlterCommand)
				if err != nil || result.Rows[0][5] != nil {
					t.Error("local ADD changed its nil URI output")
				}
			})
		}
	}
}
