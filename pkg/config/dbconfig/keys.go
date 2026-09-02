// Package dbconfig: allowed per-DB config keys for validation and UI.

package dbconfig

import "strings"

// KeyMeta describes one allowed per-database config key.
type KeyMeta struct {
	Key                 string       `json:"key"`
	EnvironmentVariable string       `json:"environmentVariable,omitempty"`
	Type                string       `json:"type"`     // "string", "number", "boolean", "duration"
	Category            string       `json:"category"` // "Embeddings", "Search", "HNSW", etc.
	Scope               SettingScope `json:"scope,omitempty"`
	Dynamic             bool         `json:"dynamic"`
	RestartLevel        RestartLevel `json:"restartLevel,omitempty"`
	DefaultValue        string       `json:"defaultValue,omitempty"`
	ValidValues         []string     `json:"validValues,omitempty"`
	// Description is optional. Surfaced as field-level help in the per-DB
	// config UI when present. Looked up via KeyDescription(key).
	Description string `json:"description,omitempty"`
}

// KeyDescription returns the operator-facing help text for a key, or the
// empty string when none is registered. Stable for callers (UI/help) to
// consume; returning "" lets callers skip rendering the description line.
func KeyDescription(key string) string {
	definition, ok := LookupSetting(key)
	if !ok {
		return ""
	}
	return definition.Description
}

// AllowedKeys returns the list of allowed per-DB config keys and their metadata,
// with Description populated from the keyDescriptions map for keys that have one.
// Used by API validation and by GET /admin/databases/config/keys.
func AllowedKeys() []KeyMeta {
	definitions := Settings()
	keys := make([]KeyMeta, 0, len(definitions))
	for _, definition := range definitions {
		if definition.Scope == ScopePhysicalEngine || KeysExcludedFromPerDB[definition.Name] {
			continue
		}
		keys = append(keys, KeyMeta{
			Key:                 definition.Name,
			EnvironmentVariable: definition.EnvironmentVariable,
			Type:                definition.Type,
			Category:            definition.Category,
			Scope:               definition.Scope,
			Dynamic:             definition.Dynamic,
			RestartLevel:        definition.RestartLevel,
			DefaultValue:        definition.DefaultValue,
			ValidValues:         append([]string(nil), definition.ValidValues...),
			Description:         definition.Description,
		})
	}
	return keys
}

// AllowedKeysSet returns a set of allowed key names for validation.
func AllowedKeysSet() map[string]KeyMeta {
	set := make(map[string]KeyMeta)
	for _, m := range AllowedKeys() {
		set[m.Key] = m
	}
	return set
}

// KeysExcludedFromPerDB are not allowed as per-DB overrides (reserved for future use).
var KeysExcludedFromPerDB = map[string]bool{}

// IsAllowedKey returns true if the key can be set as a per-DB override.
func IsAllowedKey(key string) bool {
	canonical := CanonicalSettingName(key)
	if KeysExcludedFromPerDB[key] || KeysExcludedFromPerDB[canonical] {
		return false
	}
	definition, ok := LookupSetting(canonical)
	return ok && definition.Scope != ScopePhysicalEngine
}

// EnumValues returns the permitted values for a key whose Type is "enum:v1,v2,...",
// or nil for non-enum keys (or unknown keys).
func EnumValues(key string) []string {
	definition, ok := LookupSetting(key)
	if !ok {
		return nil
	}
	if definition.Type != "enum" {
		return nil
	}
	out := make([]string, 0, len(definition.ValidValues))
	for _, p := range definition.ValidValues {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// IsValidEnumValue returns (true, "") if value matches one of the enum's
// permitted values (case-insensitive). For non-enum keys it returns (true, "")
// — only enums get value-level validation here. Returns (false, "<csv>") for
// unknown enum values so callers can echo the permitted list back to operators.
func IsValidEnumValue(key, value string) (bool, string) {
	values := EnumValues(key)
	if values == nil {
		return true, ""
	}
	v := strings.ToLower(strings.TrimSpace(value))
	for _, allowed := range values {
		if strings.ToLower(allowed) == v {
			return true, ""
		}
	}
	return false, strings.Join(values, ",")
}
