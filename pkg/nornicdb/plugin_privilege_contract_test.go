package nornicdb

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type unclassifiedFunctionPlugin struct{}

func (unclassifiedFunctionPlugin) Functions() map[string]interface{} {
	return map[string]interface{}{"unclassified": func() int { return 1 }}
}

func TestPluginExtractionRejectsMissingPrivilegeClass(t *testing.T) {
	functions, err := extractFunctions(reflect.ValueOf(unclassifiedFunctionPlugin{}), "test")
	require.ErrorContains(t, err, "explicit privilege class required")
	require.Empty(t, functions)
}
