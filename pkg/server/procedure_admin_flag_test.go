package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A procedure needs an administrator exactly when SHOW PROCEDURES lists it
// as admin (its Admin flag), as in Neo4j: db.stats.* and the other admin
// procedures are refused to a reader with Neo.ClientError.Security.Forbidden
// and run for an administrator, while the public DBMS procedures run for
// anyone.
func TestAdminProceduresFollowTheirAdminFlagOverHTTP(t *testing.T) {
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	adminOnly := []string{
		"CALL db.stats.collect('QUERIES')",
		"CALL db.stats.retrieve('QUERIES')",
		"CALL db.stats.status()",
		"CALL db.stats.stop('QUERIES')",
		"CALL db.stats.clear('QUERIES')",
		"CALL db.stats.retrieveAllAnTheStats()",
		"CALL db.clearQueryCaches()",
		"CALL dbms.listConfig()",
		"CALL db.txlog.entries()",
	}
	for _, statement := range adminOnly {
		require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "nornic", reader, statement), statement)
		require.Empty(t, txErrorCode(t, server, "nornic", admin, statement), statement)
	}
	for _, statement := range []string{
		"CALL dbms.components()",
		"CALL dbms.info()",
		"CALL dbms.listConnections()",
		"CALL dbms.procedures()",
	} {
		require.Empty(t, txErrorCode(t, server, "nornic", reader, statement), statement)
	}
}
