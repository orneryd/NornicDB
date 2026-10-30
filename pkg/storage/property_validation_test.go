package storage

import "testing"

func TestValidatePropertiesForStorageRejectsUnsupportedType(t *testing.T) {
	type badValue struct {
		Reason string
	}

	props := map[string]interface{}{
		"bad": badValue{Reason: "nope"},
	}

	if err := validatePropertiesForStorage(props); err == nil {
		t.Fatal("expected error for unsupported property type")
	}
}

func TestValidatePropertiesForStorageAcceptsNestedValues(t *testing.T) {
	props := map[string]interface{}{
		"meta": map[string]interface{}{
			"tags": []interface{}{"a", "b", int64(3)},
			"ok":   true,
		},
	}

	if err := validatePropertiesForStorage(props); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidatePropertyValueForStorage_Branches(t *testing.T) {
	tests := []struct {
		name      string
		value     interface{}
		expectErr bool
	}{
		{name: "nil", value: nil},
		{name: "primitive", value: int64(3)},
		{name: "typed string slice", value: []string{"a", "b"}},
		{name: "typed bool slice", value: []bool{true, false}},
		{name: "typed float slice", value: []float64{1.1, 2.2}},
		{name: "unsupported nested list item", value: []interface{}{"ok", struct{ X int }{X: 1}}, expectErr: true},
		{name: "unsupported nested map item", value: map[string]interface{}{"ok": 1, "bad": struct{ Y string }{Y: "x"}}, expectErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validatePropertyValueForStorage(tc.value)
			if tc.expectErr && err == nil {
				t.Fatalf("expected error")
			}
			if !tc.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
