package apoc

import (
	"reflect"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

func TestN15268RootTextCallerPolicies(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))
	for _, tc := range []struct {
		name    string
		args    []interface{}
		want    interface{}
		failure bool
	}{
		{"apoc.text.base64Decode", []interface{}{"Zg"}, "f", false},
		{"apoc.text.base64Decode", []interface{}{"Zg="}, nil, true},
		{"apoc.util.decodeBase64", []interface{}{"%%%"}, nil, true},
		{"apoc.text.urldecode", []interface{}{"%zz"}, nil, true},
		{"apoc.text.capitalize", []interface{}{""}, nil, true},
		{"apoc.text.capitalize", []interface{}{"hello"}, "Hello", false},
		{"apoc.text.decapitalize", []interface{}{"Hello"}, "hello", false},
		{"apoc.text.decapitalize", []interface{}{nil}, nil, false},
		// Utility plus handling is retained observed behavior, not an APOC oracle.
		{"apoc.util.decodeURL", []interface{}{"hello+world"}, "hello world", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Call(tc.name, tc.args)
			if tc.failure {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, args := range [][]interface{}{{}, {"a", "b"}, {42}, {nil}} {
		_, err := Call("apoc.text.base64Decode", args)
		require.Error(t, err)
		require.False(t, registry.InvocationStarted(err))
	}
	// CompareCleaned's preexisting two-string/bool shape remains outside the
	// root Call policy. Typed/Cypher callers retain their own admission policy.
	_, err := Call("apoc.text.compareCleaned", []interface{}{"É", "e"})
	require.Error(t, err)
	require.False(t, registry.InvocationStarted(err))
}

func TestN15268CheckedTextKeepsXMLCallerPolicy(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))
	for _, name := range []string{"apoc.xml.prettify", "apoc.xml.toJson", "apoc.xml.fromJson"} {
		t.Run(name, func(t *testing.T) {
			_, err := Call(name, []interface{}{"<a>v</a>"})
			require.Error(t, err)
			require.False(t, registry.InvocationStarted(err))
			info, ok := GetInfo(name)
			require.True(t, ok)
			value, err := info.Handler.Call([]interface{}{"<a>v</a>"})
			require.NoError(t, err)
			require.NotEmpty(t, value)
		})
	}
}

func TestN15268CheckedSignaturePopulation(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))
	checked := reflect.TypeOf(registry.CheckedTextFunction(nil))
	nullable := reflect.TypeOf((func(interface{}) (interface{}, error))(nil))
	var checkedNames, nullableNames []string
	for _, name := range List() {
		info, ok := GetInfo(name)
		require.True(t, ok)
		switch info.Handler.Signature() {
		case checked:
			checkedNames = append(checkedNames, name)
		case nullable:
			nullableNames = append(nullableNames, name)
		}
	}
	sort.Strings(checkedNames)
	sort.Strings(nullableNames)
	require.Equal(t, []string{"apoc.text.base64Decode", "apoc.text.capitalize", "apoc.text.urldecode", "apoc.util.decodeBase64", "apoc.util.decodeURL"}, checkedNames)
	require.Equal(t, []string{"apoc.text.decapitalize"}, nullableNames)
}
