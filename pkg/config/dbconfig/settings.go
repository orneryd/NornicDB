package dbconfig

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type SettingScope string

const (
	ScopeDatabase       SettingScope = "database"
	ScopeDatabaseIndex  SettingScope = "database-index"
	ScopePhysicalEngine SettingScope = "physical-engine"
)

type RestartLevel string

const (
	RestartNone     RestartLevel = "none"
	RestartDatabase RestartLevel = "database"
	RestartProcess  RestartLevel = "process"
)

// SettingDefinition is the source of truth for setting validation and metadata.
type SettingDefinition struct {
	Name          string
	LegacyKey     string
	Type          string
	Category      string
	Description   string
	DefaultValue  string
	Scope         SettingScope
	Dynamic       bool
	RestartLevel  RestartLevel
	ZeroSemantics string
	Deprecated    bool
	Redacted      bool
	LegacySource  string
	ValidValues   []string
}

var canonicalSettings = []SettingDefinition{
	{Name: "server.memory.query_cache.per_db_cache_num_entries", Type: "number", Category: "Query cache", DefaultValue: "1000", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, ZeroSemantics: "disabled", LegacySource: "NORNICDB_QUERY_CACHE_SIZE"},
	{Name: "db.nornic.query_cache.ttl", Type: "duration", Category: "Query cache", DefaultValue: "5m", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, LegacySource: "NORNICDB_QUERY_CACHE_TTL"},
	{Name: "db.nornic.query_plan_cache.max_entries", Type: "number", Category: "Query cache", DefaultValue: "500", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, ZeroSemantics: "disabled"},
	{Name: "db.nornic.fabric_plan_cache.max_entries", Type: "number", Category: "Query cache", DefaultValue: "500", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, ZeroSemantics: "disabled"},
	{Name: "db.nornic.query_analysis_cache.max_entries", Type: "number", Category: "Query cache", DefaultValue: "1000", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, ZeroSemantics: "disabled"},
	{Name: "db.nornic.search_result_cache.max_entries", Type: "number", Category: "Search", DefaultValue: "1000", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, ZeroSemantics: "disabled"},
	{Name: "db.nornic.query_lookup_metadata.max_entries", Type: "number", Category: "Query cache", DefaultValue: "0", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, ZeroSemantics: "existing per-cache bounds"},
	{Name: "db.memory.transaction.total.max", Type: "bytes", Category: "Transactions", DefaultValue: "0", Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone, ZeroSemantics: "unlimited"},
	{Name: "db.nornic.memory.storage.mode", Type: "enum", Category: "Storage", DefaultValue: "default", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess, ValidValues: []string{"default", "low"}},
	{Name: "db.nornic.memory.storage.node_cache.max_entries", Type: "number", Category: "Storage", DefaultValue: "10000", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess, LegacySource: "NORNICDB_BADGER_NODE_CACHE_MAX_ENTRIES"},
	{Name: "db.nornic.memory.storage.edge_type_cache.max_entries", Type: "number", Category: "Storage", DefaultValue: "50", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess, LegacySource: "NORNICDB_BADGER_EDGE_TYPE_CACHE_MAX_TYPES"},
	{Name: "db.nornic.memory.index.bm25.max", Type: "bytes", Category: "Search", DefaultValue: "0", Scope: ScopeDatabaseIndex, RestartLevel: RestartDatabase, ZeroSemantics: "unlimited"},
	{Name: "db.nornic.memory.index.vector.max", Type: "bytes", Category: "Search", DefaultValue: "0", Scope: ScopeDatabaseIndex, RestartLevel: RestartDatabase, ZeroSemantics: "unlimited"},
	{Name: "db.nornic.memory.index.metadata.max", Type: "bytes", Category: "Search", DefaultValue: "0", Scope: ScopeDatabaseIndex, RestartLevel: RestartDatabase, ZeroSemantics: "unlimited"},
	{Name: "db.nornic.index.bm25.storage", Type: "enum", Category: "Search", DefaultValue: "memory", Scope: ScopeDatabaseIndex, RestartLevel: RestartDatabase, ValidValues: []string{"memory", "disk"}},
	{Name: "db.nornic.index.vector.storage", Type: "enum", Category: "Search", DefaultValue: "automatic", Scope: ScopeDatabaseIndex, RestartLevel: RestartDatabase, ValidValues: []string{"automatic", "memory", "disk"}},
	{Name: "db.nornic.recovery.batch.max_bytes", Type: "bytes", Category: "Recovery", DefaultValue: "0", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess, ZeroSemantics: "unlimited"},
	{Name: "db.nornic.recovery.memory.max", Type: "bytes", Category: "Recovery", DefaultValue: "0", Scope: ScopePhysicalEngine, RestartLevel: RestartProcess, ZeroSemantics: "unlimited"},
}

// Settings returns defensive copies of all legacy and canonical definitions.
func Settings() []SettingDefinition {
	raw := allowedKeysRaw()
	settings := make([]SettingDefinition, 0, len(raw)+len(canonicalSettings))
	for _, key := range raw {
		definition := SettingDefinition{
			Name: key.key, LegacyKey: key.key, Type: key.typ, Category: key.category,
			Description: keyDescriptions[key.key], Scope: ScopeDatabase, Dynamic: true, RestartLevel: RestartNone,
		}
		if strings.HasPrefix(key.typ, "enum:") {
			definition.Type = "enum"
			definition.ValidValues = strings.Split(strings.TrimPrefix(key.typ, "enum:"), ",")
		}
		settings = append(settings, definition)
	}
	for _, definition := range canonicalSettings {
		definition.ValidValues = append([]string(nil), definition.ValidValues...)
		settings = append(settings, definition)
	}
	return settings
}

func LookupSetting(name string) (SettingDefinition, bool) {
	for _, definition := range Settings() {
		if definition.Name == name || definition.LegacyKey != "" && definition.LegacyKey == name {
			return definition, true
		}
	}
	return SettingDefinition{}, false
}

func ParseByteSize(raw string) (int64, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return 0, fmt.Errorf("byte size is empty")
	}
	multiplier := int64(1)
	if suffix := value[len(value)-1]; suffix < '0' || suffix > '9' {
		switch suffix {
		case 'k':
			multiplier = 1 << 10
		case 'm':
			multiplier = 1 << 20
		case 'g':
			multiplier = 1 << 30
		case 't':
			multiplier = 1 << 40
		default:
			return 0, fmt.Errorf("invalid byte-size suffix in %q", raw)
		}
		value = strings.TrimSpace(value[:len(value)-1])
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid byte size %q", raw)
	}
	if parsed > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("byte size overflows int64: %q", raw)
	}
	return parsed * multiplier, nil
}

func NormalizeSettingValue(name, raw string) (string, error) {
	definition, ok := LookupSetting(name)
	if !ok {
		return "", fmt.Errorf("unknown setting %s", name)
	}
	value := strings.TrimSpace(raw)
	switch definition.Type {
	case "bytes":
		parsed, err := ParseByteSize(value)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(parsed, 10), nil
	case "boolean":
		parsed, err := strconv.ParseBool(value)
		if value == "1" {
			parsed, err = true, nil
		} else if value == "0" {
			parsed, err = false, nil
		}
		if err != nil {
			return "", fmt.Errorf("invalid boolean %q", raw)
		}
		return strconv.FormatBool(parsed), nil
	case "number":
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || parsed < 0 {
			return "", fmt.Errorf("invalid nonnegative number %q", raw)
		}
		return value, nil
	case "duration":
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 0 {
			return "", fmt.Errorf("invalid duration %q", raw)
		}
		return parsed.String(), nil
	case "enum":
		for _, allowed := range definition.ValidValues {
			if strings.EqualFold(value, allowed) {
				return allowed, nil
			}
		}
		return "", fmt.Errorf("invalid value for %s: got %s (allowed: %s)", name, raw, strings.Join(definition.ValidValues, ","))
	default:
		return value, nil
	}
}
