package storage

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestInstalledConstraintDomainCopyPopulation(t *testing.T) {
	for _, example := range []any{[]byte{1}, []string{"x"}, []int{1}, []int32{1}, []int64{9007199254740993}, []float32{1}, []float64{1}, []bool{false}, []uint64{1}, [][]int64{{1}}} {
		for _, nest := range []string{"direct", "list", "map"} {
			t.Run(reflect.TypeOf(example).String()+"/"+nest, func(t *testing.T) {
				list := reflect.MakeSlice(reflect.TypeOf(example), 1, 1)
				reflect.Copy(list, reflect.ValueOf(example))
				var value any = list.Interface()
				if nest == "list" {
					value = []any{value}
				}
				if nest == "map" {
					value = map[string]any{"nested": []any{value}}
				}
				before := copyContractValue(value)
				owner := installConstraintDefinition(Constraint{AllowedValues: []any{value}})
				item := list.Index(0)
				for item.Kind() == reflect.Slice {
					item = item.Index(0)
				}
				switch item.Kind() {
				case reflect.String:
					item.SetString("changed")
				case reflect.Bool:
					item.SetBool(true)
				case reflect.Uint8, reflect.Uint64:
					item.SetUint(2)
				case reflect.Float32, reflect.Float64:
					item.SetFloat(2)
				default:
					item.SetInt(item.Int() + 1)
				}
				require.NotEqual(t, before, value, "mutation must change a real typed element")
				require.Equal(t, before, owner.snapshot().AllowedValues[0])
				output := owner.snapshot()
				prior := renderExported(output.AllowedValues[0])
				mutateExported(reflect.ValueOf(&output).Elem(), map[uintptr]bool{})
				require.NotEqual(t, prior, renderExported(output.AllowedValues[0]), "the mutable output subject must actually change")
				require.Equal(t, before, owner.snapshot().AllowedValues[0], "snapshot aliased the owner")
			})
		}
	}
	for _, value := range []any{[]int64(nil), []int64{}, []any(nil), []any{}, map[string]any(nil), map[string]any{}} {
		require.Equal(t, value, copyContractValue(value), "nil/empty representation changed")
	}
}

func TestInstalledConstraintPropertyViewSemantics(t *testing.T) {
	for _, input := range []any{nil, []string(nil), []string{}} {
		names := constraintPropertyNamesOf(input)
		require.Zero(t, names.Len())
		require.Empty(t, names.Join(","))
		_, ok := splitTemporalKeySpec(input)
		require.False(t, ok)
		if input == nil || reflect.ValueOf(input).IsNil() {
			require.Nil(t, names.snapshot())
		} else {
			require.Equal(t, []string{}, names.snapshot())
		}
	}
	owner := installConstraintDefinition(Constraint{Properties: []string{"tenant", "key", "from", "to"}})
	spec, ok := splitTemporalKeySpec(owner.Properties)
	require.True(t, ok)
	require.Equal(t, "tenant, key", spec.keyLabel())
	require.Equal(t, "tenant\x1fkey", spec.indexKeyProp())
	require.Equal(t, "key", spec.keyProps.Slice(1, 2).Join(","))
	copy := spec.keyProps.snapshot()
	copy[0] = "replacement"
	require.Equal(t, []string{"tenant", "key"}, spec.keyProps.snapshot())
	require.Equal(t, []string{"tenant", "key", "from", "to"}, owner.snapshot().Properties)
}
