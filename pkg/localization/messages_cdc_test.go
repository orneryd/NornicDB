package localization

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// The Change Data Capture and database option messages keep their English
// text and data (#937).
func TestChangeDataCaptureMessages(t *testing.T) {
	for _, testCase := range []struct {
		message Message
		text    string
	}{
		{CypherSpecializedCallsCDCDisabled("db"), "Change Data Capture is not currently enabled for database 'db'."},
		{CypherSpecializedCallsCDCInvalidIdentifier("x"), "Invalid change identifier 'x'."},
		{CypherSpecializedCallsCDCExpiredIdentifier("x"), "Invalid change identifier 'x': it is older than the earliest retained change (db.cdc.earliest())."},
		{CypherSpecializedCallsCDCFutureIdentifier("x"), "Change identifier 'x' points to a future transaction that has not yet happened on this database instance."},
		{CypherSpecializedCallsCDCScanFailed(errors.New("disk")), "Unable to read the Change Data Capture data for this database: disk"},
		{CypherSpecializedCallsCDCSelectorInvalid("why"), "Invalid change selector: why"},
		{CypherAdminDatabaseOptionUnknown("o"), "Could not create database with specified options: unknown option 'o'"},
		{CypherAdminDatabaseOptionUnsupported("o"), "the database option 'o' is not supported by NornicDB"},
		{CypherAdminDatabaseOptionValue("o", "v", "A, B"), "invalid value 'v' for database option 'o': expected one of A, B"},
		{CypherAdminDatabaseOptionsUnavailable("ALTER DATABASE"), "database options are not available - ALTER DATABASE requires a database manager with options"},
		{CypherProcedureMetadataDbCdcCurrent(), "Returns the change identifier just after the last committed change"},
		{CypherProcedureMetadataDbCdcEarliest(), "Returns the change identifier just before the earliest retained change"},
		{CypherProcedureMetadataDbCdcQuery(), "Returns the committed changes after a change identifier, optionally filtered by selectors"},
	} {
		require.Equal(t, testCase.text, testCase.message.Fallback, testCase.message.ID)
		require.NotEmpty(t, testCase.message.ID)
	}
}
