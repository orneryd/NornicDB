package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Dynamic process settings (#1343): Neo4j's dbms.setConfigValue changes a
// dynamic setting of the running process; the change lasts until the
// process restarts, as in Neo4j. A database's own settings change through
// the per-database config API instead.

// processSetting is a dynamic process setting: apply gives it a new value,
// in force once apply returns; reset returns it to the value the process
// was configured with (dbms.setConfigValue with an empty value, as in
// Neo4j); live is the value in force once dbms.setConfigValue set it, read
// from where it is enforced, so SHOW SETTINGS and dbms.listConfig report
// what is enforced, whichever change came last. Every setting has all
// three.
type processSetting struct {
	apply func(value string) error
	reset func()
	live  func() (string, bool)
}

var processSettings = map[string]processSetting{
	globalTransactionMemorySetting: {
		apply: func(value string) error {
			limit, err := dbconfig.ParseByteSize(value)
			if err != nil {
				return err
			}
			SetGlobalTransactionMemoryLimit(limit)
			return nil
		},
		reset: ResetGlobalTransactionMemoryLimit,
		live: func() (string, bool) {
			limit, set := liveGlobalTransactionMemoryLimit()
			return strconv.FormatInt(limit, 10), set
		},
	},
}

// liveProcessSetting is the value of name in force once dbms.setConfigValue
// set it, if it did.
func liveProcessSetting(name string) (string, bool) {
	setting, ok := processSettings[name]
	if !ok {
		return "", false
	}
	return setting.live()
}

func dbmsSetConfigValueProcedureSpec() ProcedureSpec {
	name := "dbms.setConfigValue"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(setting :: STRING, value :: STRING)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeDBMS,
		WorksOnSystem:      true,
		Admin:              true,
		Params: []ProcedureParam{
			{Name: "setting", Type: "STRING", Description: "The name of the setting."},
			{Name: "value", Type: "STRING", Description: "The value to set."},
		},
		MinArgs: 2,
		MaxArgs: 2,
	}
}

// callDbmsSetConfigValue is dbms.setConfigValue(setting, value): a dynamic
// process setting takes value now. An unknown setting, one that isn't
// dynamic, a database setting, or a value the setting doesn't take fails
// the call.
func (e *StorageExecutor) callDbmsSetConfigValue(_ context.Context, arguments []interface{}) (*ExecuteResult, error) {
	failed := func(message localization.Message) error {
		return localizedStatusError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed", message)
	}
	if len(arguments) != 2 {
		return nil, failed(localization.CypherSettingsSetConfigValueArguments())
	}
	name, nameOK := arguments[0].(string)
	value, valueOK := arguments[1].(string)
	if !nameOK || !valueOK {
		return nil, failed(localization.CypherSettingsSetConfigValueArguments())
	}
	name = strings.TrimSpace(name)
	definition, known := dbconfig.LookupSetting(name)
	if !known {
		return nil, failed(localization.CypherSettingsUnknownSetting(name))
	}
	if definition.Scope == dbconfig.ScopeDatabase {
		return nil, failed(localization.CypherSettingsDatabaseSetting(definition.Name))
	}
	setting, applicable := processSettings[definition.Name]
	if !definition.Dynamic || !applicable {
		return nil, failed(localization.CypherSettingsSettingNotDynamic(definition.Name))
	}
	// An empty value resets the setting, before any value is parsed.
	if value = strings.TrimSpace(value); value == "" {
		setting.reset()
	} else if err := setting.apply(value); err != nil {
		return nil, failed(localization.CypherSettingsInvalidSettingValue(definition.Name, value, err))
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
