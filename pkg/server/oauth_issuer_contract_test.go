package server

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

func TestR3HTTPStartupRejectsCredentialIssuer(t *testing.T) {
	const credential = "synthetic-http-issuer-846291"
	dbConfig := nornicdb.DefaultConfig()
	dbConfig.Memory.EmbeddingEnabled = false
	dbConfig.Memory.DecayEnabled = false
	dbConfig.Memory.AutoLinksEnabled = false
	db, err := nornicdb.Open(t.TempDir(), dbConfig)
	if err != nil {
		t.Fatal("test database did not open")
	}
	defer db.Close()
	t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	t.Setenv("NORNICDB_OAUTH_ISSUER", "https://user:"+credential+"@idp.example")
	config := DefaultConfig()
	config.BoltEnabled = false
	s, err := New(db, nil, config)
	if s != nil {
		stopTestServer(t, s)
	}
	if err == nil || !strings.Contains(err.Error(), "OAuth issuer") {
		t.Error("HTTP constructor did not reject issuer with Bolt disabled")
	} else if strings.Contains(err.Error(), credential) {
		t.Error("HTTP constructor disclosed issuer credential; capture withheld")
	}
}
