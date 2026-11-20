package fgac

import (
	"context"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func sessionFixtureForTest() *Fixture {
	return &Fixture{
		Principals: map[string]Principal{
			"admin":  {Admin: true},
			"reader": {User: "fgac_reader"},
		},
	}
}

func TestSessionConfigSelectsDatabaseAndImpersonation(t *testing.T) {
	runner := &endpointRunner{
		config:  EndpointConfig{Name: "candidate", URI: "bolt://127.0.0.1:17687", Database: "nornic"},
		fixture: sessionFixtureForTest(),
	}
	tests := []struct {
		name         string
		testCase     Case
		wantDatabase string
		wantUser     string
	}{
		{
			name:         "explicit target database",
			testCase:     Case{Database: DatabaseTarget},
			wantDatabase: "nornic",
		},
		{
			name:         "explicit system database",
			testCase:     Case{Database: DatabaseSystem},
			wantDatabase: DatabaseSystem,
		},
		{
			name:         "omitted database selection",
			testCase:     Case{Database: DatabaseTarget, Session: SessionSpec{Database: SessionDatabaseOmitted}},
			wantDatabase: "",
		},
		{
			name:         "impersonated synthetic user",
			testCase:     Case{Database: DatabaseTarget, Impersonate: "reader"},
			wantDatabase: "nornic",
			wantUser:     "fgac_reader",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := runner.sessionConfig(test.testCase)
			if config.DatabaseName != test.wantDatabase {
				t.Errorf("database = %q, want %q", config.DatabaseName, test.wantDatabase)
			}
			if config.ImpersonatedUser != test.wantUser {
				t.Errorf("impersonated user = %q, want %q", config.ImpersonatedUser, test.wantUser)
			}
			if config.AccessMode != neo4j.AccessModeWrite {
				t.Errorf("access mode = %v, want %v", config.AccessMode, neo4j.AccessModeWrite)
			}
		})
	}
}

func TestRoutedSessionRequiresRoutingURI(t *testing.T) {
	runner := &endpointRunner{
		config:  EndpointConfig{Name: "candidate", URI: "bolt://127.0.0.1:17687"},
		fixture: sessionFixtureForTest(),
		drivers: make(map[driverKey]neo4j.DriverWithContext),
	}
	_, err := runner.driver(context.Background(), "admin", true)
	if err == nil || !strings.Contains(err.Error(), "routed Bolt URI") {
		t.Fatalf("driver error = %v, want a routed Bolt URI requirement", err)
	}
	if len(runner.drivers) != 0 {
		t.Fatalf("driver cache holds %d entries after the routed URI refusal", len(runner.drivers))
	}
}
