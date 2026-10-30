package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
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
