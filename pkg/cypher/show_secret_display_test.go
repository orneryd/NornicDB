package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
)

func TestShowSecretDisplay(t *testing.T) {
	const credential = "synthetic-show-credential-314159"
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	active := map[string]string{"db.nornic.embedding.api.key": credential, "db.nornic.embedding.api.url": "http://u:" + credential + "@example.test/api?api_key=" + credential, "db.nornic.search.vector.warming": "lazy"}
	executor.SetSettingsResolver(func() SettingsSnapshot { return SettingsSnapshot{Configured: active, Active: active} })
	for _, query := range []string{"SHOW SETTINGS YIELD *", "CALL dbms.listConfig()"} {
		result, err := executor.Execute(context.Background(), query, nil)
		if err != nil {
			t.Fatal("settings execution failed (details withheld)")
		}
		if strings.Contains(fmt.Sprint(result.Rows), credential) {
			t.Error("settings disclosed credential (rows withheld)")
		}
		if !strings.Contains(fmt.Sprint(result.Rows), "example.test/api") || !strings.Contains(fmt.Sprint(result.Rows), "lazy") {
			t.Error("ordinary settings lost")
		}
	}
	if active["db.nornic.embedding.api.key"] != credential {
		t.Error("runtime settings mutated")
	}
}

func TestShowUnsetSecretsRemainEmpty(t *testing.T) {
	const key = "db.nornic.embedding.api.key"
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, value := range []string{"", "synthetic-show-set-credential-813624"} {
		settings := map[string]string{key: value}
		executor.SetSettingsResolver(func() SettingsSnapshot { return SettingsSnapshot{Configured: settings, Active: settings} })
		result, err := executor.Execute(context.Background(), "SHOW SETTINGS YIELD *", nil)
		if err != nil {
			t.Fatal("SHOW SETTINGS failed; details withheld")
		}
		columns := make(map[string]int)
		for i, column := range result.Columns {
			columns[column] = i
		}
		found := false
		for _, row := range result.Rows {
			if row[columns["name"]] != key {
				continue
			}
			found = true
			want := ""
			if value != "" {
				want = observability.RedactedPlaceholder()
			}
			for _, column := range []string{"value", "startupValue"} {
				if row[columns[column]] != want {
					t.Error("SHOW changed unset state or disclosed a secret; row withheld")
				}
			}
			if row[columns["defaultValue"]] != "" {
				t.Error("SHOW changed the unset secret default")
			}
		}
		if !found {
			t.Error("secret setting row missing")
		}
		if settings[key] != value {
			t.Error("SHOW changed runtime settings")
		}
	}
}
