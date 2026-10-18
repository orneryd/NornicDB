package apocmeta

import (
	"strings"
	"testing"
)

func TestNativeFunctionMetadataOwner(t *testing.T) {
	functions := NativeFunctions()
	if len(functions) != 52 {
		t.Fatalf("native population: %d", len(functions))
	}
	seen := make(map[string]bool)
	for _, function := range functions {
		name := strings.ToLower(function.Name)
		if seen[name] || !IsNative(strings.ToUpper(name)) || function.Signature == "" {
			t.Fatalf("invalid canonical entry: %#v", function)
		}
		seen[name] = true
		if IsNativeValue(name) != function.NativeValue {
			t.Fatalf("native value policy: %s", name)
		}
	}
	original := functions[0].Name
	functions[0].Name = "changed"
	functions[0].ArgumentDescriptions[0] = "changed"
	if NativeFunctions()[0].Name != original || NativeFunctions()[0].ArgumentDescriptions[0] == "changed" {
		t.Fatal("metadata was lent to a consumer")
	}
	if len(NativeFunctionNames()) != len(seen) {
		t.Fatal("name catalog differs")
	}
}

func TestNativeLookupSkipsOtherNamespacesWithoutAllocation(t *testing.T) {
	for _, name := range []string{"toLower", "myPlugin.fn", "APO", "notApoc.fn"} {
		if IsNative(name) || IsNativeValue(name) || IsNativeValueCaseMismatch(name) {
			t.Fatalf("ordinary function %q classified as native APOC", name)
		}
		if allocations := testing.AllocsPerRun(100, func() { IsNative(name); IsNativeValue(name); IsNativeValueCaseMismatch(name) }); allocations != 0 {
			t.Errorf("lookup %q: %g allocations, expected zero", name, allocations)
		}
	}
	for _, function := range NativeFunctions() {
		if !IsNative(strings.ToUpper(function.Name)) || IsNativeValue(strings.ToUpper(function.Name)) != function.NativeValue {
			t.Errorf("case-folded lookup %q lost its metadata", function.Name)
		}
	}
}

func TestNativeValueCaseUsesCanonicalMetadata(t *testing.T) {
	for _, function := range NativeFunctions() {
		if IsNativeValueCaseMismatch(function.Name) {
			t.Errorf("canonical name %q rejected", function.Name)
		}
		for _, spelling := range []string{strings.ToLower(function.Name), strings.ToUpper(function.Name)} {
			want := function.NativeValue && spelling != function.Name
			if IsNativeValueCaseMismatch(spelling) != want {
				t.Errorf("case policy %q, canonical %q: expected mismatch %v", spelling, function.Name, want)
			}
		}
	}
	for _, name := range []string{"toLower", "myPlugin.camelCase", "apoc.unknown"} {
		if IsNativeValueCaseMismatch(name) {
			t.Errorf("unknown name %q classified as a native case mismatch", name)
		}
	}
}
