package cypher

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Dynamic process settings (#1343): Neo4j's dbms.setConfigValue changes a
// dynamic setting of the running process; the change lasts until the
// process restarts, as in Neo4j. A database's own settings change through
// the per-database config API instead.

// processSettingAppliers apply a dynamic process setting's new value.
var processSettingAppliers = map[string]func(value string) (string, error){
	globalTransactionMemorySetting: func(value string) (string, error) {
		limit, err := dbconfig.ParseByteSize(value)
		if err != nil {
			return "", err
		}
		SetGlobalTransactionMemoryLimit(limit)
		return strconv.FormatInt(limit, 10), nil
	},
}

// processSettingValues are the values dbms.setConfigValue set, which SHOW
// SETTINGS and dbms.listConfig report.
var processSettingValues sync.Map // setting name -> normalized value

// liveProcessSetting is the value dbms.setConfigValue set for name, if any.
func liveProcessSetting(name string) (string, bool) {
	value, ok := processSettingValues.Load(name)
	if !ok {
		return "", false
	}
	return value.(string), true
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
	apply, applicable := processSettingAppliers[definition.Name]
	if !definition.Dynamic || !applicable {
		return nil, failed(localization.CypherSettingsSettingNotDynamic(definition.Name))
	}
	normalized, err := apply(strings.TrimSpace(value))
	if err != nil {
		return nil, failed(localization.CypherSettingsInvalidSettingValue(definition.Name, value, err))
	}
	processSettingValues.Store(definition.Name, normalized)
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
