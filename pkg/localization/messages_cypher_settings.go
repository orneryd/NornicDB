package localization

const (
	MessageCypherSettingsSetConfigValueArguments MessageID = "cyphersettings.set_config_value_arguments"
	MessageCypherSettingsUnknownSetting          MessageID = "cyphersettings.unknown_setting"
	MessageCypherSettingsSettingNotDynamic       MessageID = "cyphersettings.setting_not_dynamic"
	MessageCypherSettingsInvalidSettingValue     MessageID = "cyphersettings.invalid_setting_value"
	MessageCypherSettingsDatabaseSetting         MessageID = "cyphersettings.database_setting"
)

// CypherSettingsSetConfigValueArguments is dbms.setConfigValue called
// without its two STRING arguments.
func CypherSettingsSetConfigValueArguments() Message {
	return Message{ID: MessageCypherSettingsSetConfigValueArguments, Fallback: "dbms.setConfigValue requires two STRING arguments: the setting's name and its value"}
}

// CypherSettingsUnknownSetting is a setting name no setting has.
func CypherSettingsUnknownSetting(setting string) Message {
	return Message{ID: MessageCypherSettingsUnknownSetting, Fallback: "Unknown setting: " + setting, Data: map[string]any{"Setting": setting}}
}

// CypherSettingsSettingNotDynamic is a setting dbms.setConfigValue can't
// change while the process runs.
func CypherSettingsSettingNotDynamic(setting string) Message {
	return Message{ID: MessageCypherSettingsSettingNotDynamic, Fallback: "Setting '" + setting + "' is not dynamic and can not be changed at runtime", Data: map[string]any{"Setting": setting}}
}

// CypherSettingsInvalidSettingValue is a value a setting doesn't take.
func CypherSettingsInvalidSettingValue(setting, value string, cause error) Message {
	return Message{ID: MessageCypherSettingsInvalidSettingValue, Fallback: "'" + value + "' is not a valid value for setting '" + setting + "': " + cause.Error(), Data: map[string]any{"Setting": setting, "Value": value, "Cause": cause.Error()}}
}

// CypherSettingsDatabaseSetting is a database's own setting named to
// dbms.setConfigValue: it changes through the per-database config API.
func CypherSettingsDatabaseSetting(setting string) Message {
	return Message{ID: MessageCypherSettingsDatabaseSetting, Fallback: "Setting '" + setting + "' is a database setting: change it for a database with PUT /admin/databases/{database}/config", Data: map[string]any{"Setting": setting}}
}
