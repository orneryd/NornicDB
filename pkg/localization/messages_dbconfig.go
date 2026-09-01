package localization

const (
	MessageSystemDBOverridesUnsupported MessageID = "dbconfig.system_database_overrides_unsupported"
	MessageDBConfigUnavailable          MessageID = "dbconfig.store_unavailable"
	MessageDBManagerUnavailable         MessageID = "dbconfig.manager_unavailable"
	MessageMVCCCompositeUnsupported     MessageID = "dbconfig.mvcc_composite_unsupported"
	MessageMVCCScheduleUnsupported      MessageID = "dbconfig.mvcc_schedule_unsupported"
	MessageInvalidInterval              MessageID = "dbconfig.invalid_interval"
	MessageMVCCDebtUnsupported          MessageID = "dbconfig.mvcc_debt_unsupported"
	MessageInvalidLimit                 MessageID = "dbconfig.invalid_limit"
	MessageDisallowedConfigKey          MessageID = "dbconfig.disallowed_key"
)

// SystemDatabaseOverridesUnsupported identifies an override attempt on the system database.
func SystemDatabaseOverridesUnsupported() Message {
	return Message{ID: MessageSystemDBOverridesUnsupported, Fallback: "system database cannot have config overrides"}
}

// DatabaseConfigUnavailable identifies an unavailable per-database configuration store.
func DatabaseConfigUnavailable() Message {
	return Message{ID: MessageDBConfigUnavailable, Fallback: "per-database config not available (system DB unavailable)"}
}

// DatabaseManagerUnavailable identifies an unavailable database manager.
func DatabaseManagerUnavailable() Message {
	return Message{ID: MessageDBManagerUnavailable, Fallback: "database manager unavailable"}
}

// MVCCCompositeUnsupported identifies MVCC lifecycle controls on a composite database.
func MVCCCompositeUnsupported() Message {
	return Message{ID: MessageMVCCCompositeUnsupported, Fallback: "mvcc lifecycle controls are not supported for composite databases"}
}

// MVCCScheduleUnsupported identifies unavailable lifecycle schedule control.
func MVCCScheduleUnsupported() Message {
	return Message{ID: MessageMVCCScheduleUnsupported, Fallback: "mvcc lifecycle schedule control is not supported for this database"}
}

// InvalidInterval identifies a malformed lifecycle interval.
func InvalidInterval() Message {
	return Message{ID: MessageInvalidInterval, Fallback: "invalid interval"}
}

// MVCCDebtUnsupported identifies unavailable lifecycle debt inspection.
func MVCCDebtUnsupported() Message {
	return Message{ID: MessageMVCCDebtUnsupported, Fallback: "mvcc lifecycle debt inspection is not supported for this database"}
}

// InvalidLimit identifies a malformed result limit.
func InvalidLimit() Message {
	return Message{ID: MessageInvalidLimit, Fallback: "invalid limit"}
}

// DisallowedOrUnknownConfigKey identifies a configuration key outside the allowlist.
func DisallowedOrUnknownConfigKey(key string) Message {
	return Message{
		ID:       MessageDisallowedConfigKey,
		Fallback: "disallowed or unknown key: " + key,
		Data:     map[string]any{"Key": key},
	}
}
