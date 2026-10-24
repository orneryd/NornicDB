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

func TestCredentialRejectionPublic(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	const secret = "synthetic-bare-password-936715"
	for _, prefix := range []string{
		"CREATE USER probe SET PASSWORD ",
		"ALTER USER probe SET PASSWORD ",
		"ALTER CURRENT USER SET PASSWORD FROM ",
		"ALTER CURRENT USER SET PASSWORD FROM 'synthetic-old-password-291734' TO ",
		"CREATE ALIAS probe FOR DATABASE remote AT 'http://example.test' USER probe PASSWORD ",
		"ALTER ALIAS probe SET DATABASE PASSWORD ",
		"CREATE COMPOSITE DATABASE probe ALIAS r FOR DATABASE remote AT 'http://example.test' USER probe PASSWORD ",
		"ALTER COMPOSITE DATABASE probe ADD ALIAS r FOR DATABASE remote AT 'http://example.test' USER probe PASSWORD ",
	} {
		for _, value := range []string{secret, secret + " " + secret, "'" + secret + "' garbage", "'" + secret} {
			_, err := executor.Execute(context.Background(), prefix+value, nil)
			if err == nil {
				t.Errorf("malformed credential command accepted: %s", prefix)
				continue
			}
			outputs := []string{err.Error()}
			var localized *localization.LocalizedError
			if errors.As(err, &localized) {
				outputs = append(outputs, localized.Message.Fallback, fmt.Sprint(localized.Message.Data))
			}
			for _, output := range outputs {
				if strings.Contains(output, secret) {
					t.Errorf("credential command error disclosed synthetic value: %s", prefix)
				}
			}
		}
	}
}

func TestShowCompositeTrailingTokensPublic(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	for _, query := range []string{"SHOW COMPOSITE DATABASES garbage", "SHOW COMPOSITE DATABASES garbage YIELD name"} {
		if _, err := executor.Execute(context.Background(), query, nil); err == nil {
			t.Error("trailing tokens accepted")
		}
	}
	for _, query := range []string{"SHOW COMPOSITE DATABASES", "SHOW COMPOSITE DATABASES YIELD name RETURN name"} {
		if _, err := executor.Execute(context.Background(), query, nil); err != nil {
			t.Fatal(err)
		}
	}
}
