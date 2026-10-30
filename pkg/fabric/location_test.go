package fabric

import (
	"testing"
)

func TestLocationLocal_DatabaseName(t *testing.T) {
	loc := &LocationLocal{DBName: "mydb"}
	if loc.DatabaseName() != "mydb" {
		t.Errorf("expected mydb, got %s", loc.DatabaseName())
	}
}

func TestLocationRemote_DatabaseName(t *testing.T) {
	loc := &LocationRemote{
		DBName:   "remotedb",
		URI:      "bolt://10.0.0.1:7687",
		AuthMode: "oidc_forwarding",
	}
	if loc.DatabaseName() != "remotedb" {
		t.Errorf("expected remotedb, got %s", loc.DatabaseName())
	}
}

func TestLocationRemote_UserPasswordAuth(t *testing.T) {
	loc := &LocationRemote{
		DBName:   "shard_a",
		URI:      "bolt://10.0.0.2:7687",
		AuthMode: "user_password",
		User:     "svc-user",
		Password: "svc-pass",
	}
	if loc.AuthMode != "user_password" {
		t.Errorf("expected user_password, got %s", loc.AuthMode)
	}
	if loc.User != "svc-user" {
		t.Errorf("expected svc-user, got %s", loc.User)
	}
	if loc.Password != "svc-pass" {
		t.Errorf("expected svc-pass, got %s", loc.Password)
	}
}

// TestLocationInterface verifies both types implement Location.
func TestLocationInterface(t *testing.T) {
	var _ Location = (*LocationLocal)(nil)
	var _ Location = (*LocationRemote)(nil)
}
