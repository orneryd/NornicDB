package fgac

import (
	"reflect"
	"strings"
	"testing"
)

func TestReferencePinsAreExactAndImmutable(t *testing.T) {
	pins, err := LoadReferencePins(referencesPath)
	if err != nil {
		t.Fatalf("load reference pins: %v", err)
	}
	want := []ReferencePin{
		{
			Version: "5.26.30",
			Edition: "enterprise",
			Image:   "neo4j:5.26.30-enterprise",
			Digest:  "sha256:8befcd4e0f48ebd34c58ae071c3e960ca0479675d1046ec35580f8a1dd19891c",
		},
		{
			Version:    "2026.10.2",
			Edition:    "enterprise",
			Image:      "neo4j:2026.10.2-enterprise",
			Digest:     "sha256:941ba98b5acb7cfd9aa72a0aeebd6ffbefd870a514455620eac9ef0bc67e25a9",
			NativeABAC: true,
		},
	}
	if !reflect.DeepEqual(pins.References, want) {
		t.Fatalf("reference pins = %#v, want %#v", pins.References, want)
	}
	if got := pins.References[0].Ref(); got != want[0].Image+"@"+want[0].Digest {
		t.Errorf("Ref() = %q", got)
	}
}

func TestReferencePinValidation(t *testing.T) {
	tests := []struct {
		name string
		pin  ReferencePin
		want string
	}{
		{name: "community", pin: ReferencePin{Version: "5.26.30", Edition: "community", Image: "neo4j:5.26.30-enterprise", Digest: "sha256:" + strings.Repeat("a", 64)}, want: "not enterprise"},
		{name: "wrong image", pin: ReferencePin{Version: "5.26.30", Edition: "enterprise", Image: "neo4j:5.26.30", Digest: "sha256:" + strings.Repeat("a", 64)}, want: "must be"},
		{name: "short digest", pin: ReferencePin{Version: "5.26.30", Edition: "enterprise", Image: "neo4j:5.26.30-enterprise", Digest: "sha256:abc"}, want: "not a sha256"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pins := ReferencePins{References: []ReferencePin{test.pin}}
			err := pins.Validate()
			if err == nil {
				t.Fatal("Validate succeeded, want an error")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestReferencePinsResolve(t *testing.T) {
	pins, err := LoadReferencePins(referencesPath)
	if err != nil {
		t.Fatalf("load reference pins: %v", err)
	}
	resolved, err := pins.Resolve("2026.10.2")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(resolved) != 1 || resolved[0].Version != "2026.10.2" {
		t.Fatalf("Resolve = %#v", resolved)
	}
	if _, err := pins.Resolve("5.26.29"); err == nil || !strings.Contains(err.Error(), "not pinned") {
		t.Fatalf("Resolve unpinned error = %v", err)
	}
	all, err := pins.Resolve("")
	if err != nil || len(all) != 2 {
		t.Fatalf("Resolve all = %#v, %v", all, err)
	}
}

func TestVerifyReferenceIdentity(t *testing.T) {
	pins, err := LoadReferencePins(referencesPath)
	if err != nil {
		t.Fatalf("load reference pins: %v", err)
	}
	pin, err := VerifyReferenceIdentity(ComponentInfo{Name: "Neo4j Kernel", Versions: []string{"5.26.30"}, Edition: "enterprise"}, pins, "")
	if err != nil {
		t.Fatalf("VerifyReferenceIdentity: %v", err)
	}
	if pin.Version != "5.26.30" {
		t.Fatalf("pin = %#v", pin)
	}
	if _, err := VerifyReferenceIdentity(ComponentInfo{Name: "Neo4j Kernel", Versions: []string{"5.26.30"}, Edition: "community"}, pins, ""); err == nil {
		t.Fatal("VerifyReferenceIdentity accepted Community")
	}
	if _, err := VerifyReferenceIdentity(ComponentInfo{Name: "Neo4j Kernel", Versions: []string{"5.26.29"}, Edition: "enterprise"}, pins, ""); err == nil {
		t.Fatal("VerifyReferenceIdentity accepted an unpinned version")
	}
	if _, err := VerifyReferenceIdentity(ComponentInfo{Name: "Neo4j Kernel", Versions: []string{"2026.10.2"}, Edition: "enterprise"}, pins, "5.26.30"); err == nil {
		t.Fatal("VerifyReferenceIdentity accepted the wrong requested version")
	}
}
