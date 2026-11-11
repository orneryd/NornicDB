package server

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUISetupStatementsUsePublicSchemaAndPolicyContracts(t *testing.T) {
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	statements := []string{
		"CREATE INDEX `by name` IF NOT EXISTS FOR (n:`Person`) ON (n.`name`)",
		"CREATE CONSTRAINT `email_unique` IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`tenant`, n.`email`) IS UNIQUE",
		"CREATE CONSTRAINT `source_required` IF NOT EXISTS FOR ()-[r:`SUPPORTS`]-() REQUIRE r.`source` IS NOT NULL",
		"CREATE DECAY PROFILE idle_strength OPTIONS { halfLifeSeconds: -604800, function: 'exponential', scope: 'NODE', scoreFrom: 'LAST_ACCESSED', visibilityThreshold: 0.1, scoreFloor: 0.1, decayEnabled: true }",
		"CREATE DECAY PROFILE episode_idle FOR (n:MemoryEpisode) APPLY { DECAY PROFILE idle_strength }",
		"SHOW INDEXES", "SHOW CONSTRAINTS", "CALL kp.profiles()", "CALL db.labels()", "CALL db.relationshipTypes()",
		"DROP CONSTRAINT `email_unique` IF EXISTS", "DROP INDEX `by name` IF EXISTS",
	}
	for _, statement := range statements {
		t.Run(statement, func(t *testing.T) { require.Empty(t, txErrorCode(t, server, "nornic", admin, statement), statement) })
	}
}
