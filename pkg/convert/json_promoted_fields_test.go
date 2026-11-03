package convert

import (
	"github.com/stretchr/testify/require"
	"testing"
)

type promotedJSONFields struct {
	Value any `json:"value"`
}

func TestJSONNumericDomainPromotedFields(t *testing.T) {
	for _, decode := range []struct {
		name string
		read func([]byte, any) error
	}{
		{"property", PropertyJSON.Unmarshal}, {"input", CypherInputJSON.Unmarshal},
	} {
		t.Run(decode.name, func(t *testing.T) {
			var target struct{ promotedJSONFields }
			require.NoError(t, decode.read([]byte(`{"value":9007199254740993}`), &target))
			require.Equal(t, int64(9007199254740993), target.Value)
		})
	}
}
