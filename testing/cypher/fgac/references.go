package fgac

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/pkg/convert"
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// ReferencePins is the canonical list of pinned Neo4j Enterprise references.
type ReferencePins struct {
	References []ReferencePin `json:"references"`
}

// ReferencePin identifies one pinned Neo4j Enterprise image.
type ReferencePin struct {
	Version    string `json:"version"`
	Edition    string `json:"edition"`
	Image      string `json:"image"`
	Digest     string `json:"digest"`
	NativeABAC bool   `json:"native_abac,omitempty"`
}

// LoadReferencePins reads and validates the pinned reference list.
func LoadReferencePins(path string) (*ReferencePins, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read FGAC reference pins %s: %w", path, err)
	}
	decoder := convert.CypherInputJSON.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	var pins ReferencePins
	if err := decoder.Decode(&pins); err != nil {
		return nil, fmt.Errorf("decode FGAC reference pins %s: %w", path, err)
	}
	if err := pins.Validate(); err != nil {
		return nil, fmt.Errorf("validate FGAC reference pins %s: %w", path, err)
	}
	return &pins, nil
}

// Validate checks that every pin is an immutable Enterprise image reference.
func (pins *ReferencePins) Validate() error {
	if len(pins.References) == 0 {
		return errors.New("no references are pinned")
	}
	seen := make(map[string]bool, len(pins.References))
	for index, pin := range pins.References {
		location := fmt.Sprintf("reference %d", index)
		if pin.Version == "" {
			return fmt.Errorf("%s: version is empty", location)
		}
		if seen[pin.Version] {
			return fmt.Errorf("%s: duplicate version %q", location, pin.Version)
		}
		seen[pin.Version] = true
		if !strings.EqualFold(pin.Edition, "enterprise") {
			return fmt.Errorf("%s: edition %q is not enterprise", location, pin.Edition)
		}
		expectedImage := "neo4j:" + pin.Version + "-enterprise"
		if pin.Image != expectedImage {
			return fmt.Errorf("%s: image %q must be %q", location, pin.Image, expectedImage)
		}
		if !digestPattern.MatchString(pin.Digest) {
			return fmt.Errorf("%s: digest %q is not a sha256 digest", location, pin.Digest)
		}
	}
	return nil
}

// Resolve returns the pin for a version, or every pin when version is empty.
func (pins *ReferencePins) Resolve(version string) ([]ReferencePin, error) {
	if version == "" {
		return pins.References, nil
	}
	for _, pin := range pins.References {
		if pin.Version == version {
			return []ReferencePin{pin}, nil
		}
	}
	versions := make([]string, 0, len(pins.References))
	for _, pin := range pins.References {
		versions = append(versions, pin.Version)
	}
	return nil, fmt.Errorf("reference version %q is not pinned; pinned versions: %s", version, strings.Join(versions, ", "))
}

// Ref is the immutable image reference passed to Docker.
func (pin ReferencePin) Ref() string {
	return pin.Image + "@" + pin.Digest
}
