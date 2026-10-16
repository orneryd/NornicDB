package cypher

import (
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func TestListSubscriptIndexErrorsUseLocalization(t *testing.T) {
	for _, tc := range []struct {
		name, messageID, class string
		index                  interface{}
	}{
		{"float", "cyphercore.list_index_non_integer", "", 1.5},
		{"string", "cyphercore.list_index_non_number", "", "first"},
		{"true", "cyphercore.list_index_boolean", "BooleanValue$1", true},
		{"false", "cyphercore.list_index_boolean", "BooleanValue$2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := listSubscriptIndexError([]interface{}{int64(1), int64(2)}, tc.index)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
			var localized *localization.LocalizedError
			require.ErrorAs(t, err, &localized)
			require.Equal(t, localization.MessageID(tc.messageID), localized.Message.ID)
			require.Equal(t, tc.messageID, localized.Code)
			require.Equal(t, neo4jValueRepr(tc.index), localized.Message.Data["Value"])
			if tc.messageID != "cyphercore.list_index_non_integer" {
				require.Equal(t, "List{Long(1), Long(2)}", localized.Message.Data["List"])
			}
			if tc.class != "" {
				require.Equal(t, tc.class, localized.Message.Data["Class"])
			}
		})
	}
}
