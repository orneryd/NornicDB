package cypher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

func TestCompositeRemotePasswordErrorOutput(t *testing.T) {
	const password = "synthetic-composite-error-916482"
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	if err != nil {
		t.Fatal("manager setup failed")
	}
	fixtureowner.Own(t, manager)
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	for _, create := range []bool{true, false} {
		for _, quote := range []string{"'", "\""} {
			t.Run(fmt.Sprint(create)+quote, func(t *testing.T) {
				command := "CREATE COMPOSITE DATABASE malformed ALIAS r FOR DATABASE nornic"
				if !create {
					command = "ALTER COMPOSITE DATABASE malformed ADD ALIAS r FOR DATABASE nornic"
				}
				command += " AT 'http://127.0.0.1:1' USER 'admin' PASSWORD PASSWORD " + quote + password + quote
				var err error
				if create {
					_, err = executor.executeCompositeCommand(context.Background(), command, compositeCreateCommand)
				} else {
					_, err = executor.executeCompositeCommand(context.Background(), command, compositeAlterCommand)
				}
				var localized *localization.LocalizedError
				if err == nil || !errors.As(err, &localized) || localized.Message.ID != localization.MessageCypherCompositeConstituentUnexpectedToken {
					t.Fatal("malformed composite error identity changed; details withheld")
				}
				for _, value := range []string{err.Error(), localized.Message.Fallback, fmt.Sprint(localized.Message.Data)} {
					if strings.Contains(value, password) {
						t.Error("malformed composite error disclosed PASSWORD literal; value withheld")
					}
				}
			})
		}
	}
}
