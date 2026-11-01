package fn

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemotePropertiesWithoutMapAreEmpty(t *testing.T) {
	value, err := evalProperties(context.Background(), Context{Eval: func(string) (interface{}, error) {
		return storage.RemoteEntityValue{"elementId": "n", "labels": []interface{}{}}, nil
	}}, []string{"n"})
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{}, value)
}

func TestRemotePathSizeUsesPathMeaning(t *testing.T) {
	for _, path := range []storage.RemotePathValue{nil, {storage.RemoteEntityValue{"elementId": "n"}}} {
		value, err := evalSize(context.Background(), Context{Eval: func(string) (interface{}, error) { return path, nil }}, []string{"p"})
		if path == nil {
			require.NoError(t, err)
			require.Nil(t, value)
		} else {
			require.Error(t, err)
		}
	}
}
