package multidb

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRuntimeQueryCredentialErrorsAreRedacted(t *testing.T) {
	const marker = "synthetic-r73-restored-query-91826"
	cfg := DefaultConfig()
	cfg.RemoteCredentialEncryptionKey = "synthetic-r73-encryption-key"
	called := false
	cfg.RemoteEngineFactory = func(ref ConstituentRef, _ string) (storage.Engine, error) {
		called = true
		parsed, err := url.Parse(ref.URI)
		if err != nil {
			t.Fatal("runtime URL invalid; value withheld")
		}
		actual := parsed.Query().Get("api_key")
		if actual != marker {
			t.Fatal("runtime query credential was not restored; values withheld")
		}
		return nil, fmt.Errorf("factory authentication: %w", errors.Join(
			errors.New("rejected credential "+actual),
			errors.New("connection failed"),
		))
	}
	manager, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), cfg)
	if err != nil {
		t.Fatal("manager setup failed")
	}
	defer manager.Close()
	err = manager.CreateCompositeDatabase(t.Context(), "r73query", []ConstituentRef{{
		Alias: "remote", DatabaseName: "nornic", Type: "remote", AccessMode: "read",
		URI: "https://example.invalid/?api_key=" + marker,
	}})
	if err != nil {
		t.Fatal("catalog setup failed; values withheld")
	}
	for _, name := range []string{"r73query.remote", "r73query"} {
		_, err = manager.GetStorage(name)
		if !called || err == nil {
			t.Fatal("expected runtime factory failure")
		}
		if strings.Contains(err.Error(), marker) {
			t.Error("manager error disclosed restored query credential; value withheld")
		}
	}
}
