package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// The APOC 5 trigger procedures (TriggerNewProcedures, Trigger). install,
// drop, dropAll, stop, start and show run on the system database and name
// the database they manage; list and the deprecated add, remove,
// removeAll, pause and resume manage the session's database. All need the
// admin entitlement, as APOC marks them @Admin. Each returns APOC's
// TriggerInfo rows: name, query, selector, params, installed, paused.
//
// Two documented differences from APOC: install and add take effect at
// once (APOC refreshes every apoc.trigger.refresh ms), and install refuses
// a statement that doesn't parse or a phase that doesn't exist, which in
// APOC would fail every later write of the database.

const apocTriggerInfoSignature = " :: (name :: STRING, query :: STRING, selector :: MAP, params :: MAP, installed :: BOOLEAN, paused :: BOOLEAN)"

var apocTriggerInfoColumns = []string{"name", "query", "selector", "params", "installed", "paused"}

func registerApocTriggerProcedures() {
	system := func(name, arguments, description string, mode ProcedureMode, minArgs, maxArgs int, handler ProcedureHandler) {
		registerAdminBuiltInProcedureLiteral(name, name+"("+arguments+")"+apocTriggerInfoSignature, description, mode, minArgs, maxArgs, true, handler)
	}
	system("apoc.trigger.install", "databaseName :: STRING, name :: STRING, statement :: STRING, selector :: MAP, config :: MAP = {}",
		"Adds or replaces a trigger for the given database", ProcedureModeDBMS, 4, 5, callApocTriggerInstall)
	system("apoc.trigger.drop", "databaseName :: STRING, name :: STRING",
		"Removes the given trigger", ProcedureModeDBMS, 2, 2, callApocTriggerDrop)
	system("apoc.trigger.dropAll", "databaseName :: STRING",
		"Removes all triggers from the given database", ProcedureModeDBMS, 1, 1, callApocTriggerDropAll)
	system("apoc.trigger.stop", "databaseName :: STRING, name :: STRING",
		"Stops the given trigger", ProcedureModeDBMS, 2, 2, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocTriggerSetPaused("apoc.trigger.stop", args, true)
		})
	system("apoc.trigger.start", "databaseName :: STRING, name :: STRING",
		"Restarts the given stopped trigger", ProcedureModeDBMS, 2, 2, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocTriggerSetPaused("apoc.trigger.start", args, false)
		})
	system("apoc.trigger.show", "databaseName :: STRING",
		"Lists the triggers of the given database", ProcedureModeRead, 1, 1, callApocTriggerShow)

	session := func(name, arguments, description string, mode ProcedureMode, minArgs, maxArgs int, deprecatedBy string, handler ProcedureHandler) {
		registerProcedure(ProcedureSpec{
			Name:         name,
			Signature:    name + "(" + arguments + ")" + apocTriggerInfoSignature,
			Description:  description,
			Mode:         mode,
			Admin:        true,
			IsDeprecated: deprecatedBy != "",
			DeprecatedBy: deprecatedBy,
			MinArgs:      minArgs,
			MaxArgs:      maxArgs,
		}, handler)
	}
	session("apoc.trigger.list", "", "Lists the triggers of the session database", ProcedureModeRead, 0, 0, "",
		func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			runtime, err := e.apocTriggerRuntime()
			if err != nil {
				return nil, err
			}
			return apocTriggerRows(runtime.list(e.currentDatabaseName()), true), nil
		})
	session("apoc.trigger.add", "name :: STRING, statement :: STRING, selector :: MAP, config :: MAP = {}",
		"Adds or replaces a trigger of the session database", ProcedureModeWrite, 3, 4, "apoc.trigger.install", callApocTriggerAdd)
	session("apoc.trigger.remove", "name :: STRING",
		"Removes the given trigger of the session database", ProcedureModeWrite, 1, 1, "apoc.trigger.drop", callApocTriggerRemove)
	session("apoc.trigger.removeAll", "",
		"Removes all triggers of the session database", ProcedureModeWrite, 0, 0, "apoc.trigger.dropAll",
		func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			runtime, err := e.apocTriggerRuntime()
			if err != nil {
				return nil, err
			}
			removed, err := runtime.remove(e.currentDatabaseName(), nil)
			if err != nil {
				return nil, err
			}
			return apocTriggerRows(removed, false), nil
		})
	session("apoc.trigger.pause", "name :: STRING",
		"Pauses the given trigger of the session database", ProcedureModeWrite, 1, 1, "apoc.trigger.stop",
		func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocTriggerSessionPaused("apoc.trigger.pause", args, true)
		})
	session("apoc.trigger.resume", "name :: STRING",
		"Resumes the given paused trigger of the session database", ProcedureModeWrite, 1, 1, "apoc.trigger.start",
		func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocTriggerSessionPaused("apoc.trigger.resume", args, false)
		})
}

// apocTriggerRuntime is the runtime of the executor's engine; the
// procedures fail as APOC's do while triggers aren't enabled.
func (e *StorageExecutor) apocTriggerRuntime() (*TriggerRuntime, error) {
	runtime := triggerRuntimeFor(e.storage)
	if runtime == nil {
		if system, err := e.systemStorage(); err == nil {
			runtime = triggerRuntimeFor(system)
		}
	}
	if runtime == nil || !runtime.config.Enabled {
		return nil, apocTriggerCallError(localization.CypherProceduresTriggerNotEnabled())
	}
	return runtime, nil
}

// apocTriggerSystemRuntime is apocTriggerRuntime for a procedure that runs
// on the system database only.
func (e *StorageExecutor) apocTriggerSystemRuntime() (*TriggerRuntime, error) {
	runtime, err := e.apocTriggerRuntime()
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(e.currentDatabaseName(), runtime.config.SystemDatabase) {
		return nil, apocTriggerCallError(localization.CypherProceduresTriggerSystemDatabaseRequired())
	}
	return runtime, nil
}

func apocTriggerCallError(message localization.Message) error {
	return localizedStatusError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed", message)
}

func callApocTriggerInstall(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
	const procedure = "apoc.trigger.install"
	runtime, err := e.apocTriggerSystemRuntime()
	if err != nil {
		return nil, err
	}
	database, err := requiredProcedureString(procedure, args, 0, "databaseName")
	if err != nil {
		return nil, err
	}
	if err := runtime.checkTarget(database); err != nil {
		return nil, err
	}
	trigger, err := e.apocTriggerDefinition(procedure, database, args[1:])
	if err != nil {
		return nil, err
	}
	if err := runtime.put(trigger); err != nil {
		return nil, err
	}
	return apocTriggerRows([]*apocTrigger{trigger}, true), nil
}

// checkTarget is install's check of its database: an existing user
// database (APOC's checkTargetDatabase).
func (r *TriggerRuntime) checkTarget(database string) error {
	exists := false
	if r.config.DatabaseExists != nil {
		exists = r.config.DatabaseExists(database)
	} else if _, err := r.config.ExecutorFor(database); err == nil {
		exists = true
	}
	if strings.EqualFold(database, r.config.SystemDatabase) {
		return apocTriggerCallError(localization.CypherProceduresTriggerBadTarget())
	}
	if !exists {
		return apocTriggerCallError(localization.CypherProceduresTriggerDatabaseNotFound(database))
	}
	return nil
}

// apocTriggerDefinition reads (name, statement, selector, config) into a
// trigger of database, checking its statement and phase.
func (e *StorageExecutor) apocTriggerDefinition(procedure, database string, args []interface{}) (*apocTrigger, error) {
	name, err := requiredProcedureString(procedure, args, 0, "name")
	if err != nil {
		return nil, err
	}
	statement, err := requiredProcedureString(procedure, args, 1, "statement")
	if err != nil {
		return nil, err
	}
	selector, err := optionalProcedureMap(procedure, args, 2, "selector")
	if err != nil {
		return nil, err
	}
	if procedureArgument(args, 2) == nil {
		selector = nil // listed as null, as APOC stores a null selector
	}
	config, err := optionalProcedureMap(procedure, args, 3, "config")
	if err != nil {
		return nil, err
	}
	phase, err := apocTriggerPhaseOf(selector)
	if err != nil {
		return nil, err
	}
	if err := e.validateSyntax(strings.TrimSpace(statement), false); err != nil {
		return nil, apocTriggerCallError(localization.CypherProceduresTriggerStatementInvalid(name, err))
	}
	params, _ := config["params"].(map[string]interface{})
	if params == nil {
		params = map[string]interface{}{}
	}
	// Stored as JSON, as APOC stores them; held as they read back, so a
	// trigger's params are the same before and after a restart.
	if selector, err = apocTriggerJSONRoundTrip(selector); err != nil {
		return nil, apocTriggerCallError(localization.CypherProceduresTriggerParamsInvalid(err))
	}
	if params, err = apocTriggerJSONRoundTrip(params); err != nil {
		return nil, apocTriggerCallError(localization.CypherProceduresTriggerParamsInvalid(err))
	}
	return &apocTrigger{Database: database, Name: name, Statement: statement, Selector: selector, Params: params, phase: phase}, nil
}

func callApocTriggerDrop(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
	const procedure = "apoc.trigger.drop"
	runtime, err := e.apocTriggerSystemRuntime()
	if err != nil {
		return nil, err
	}
	database, err := requiredProcedureString(procedure, args, 0, "databaseName")
	if err != nil {
		return nil, err
	}
	name, err := requiredProcedureString(procedure, args, 1, "name")
	if err != nil {
		return nil, err
	}
	removed, err := runtime.remove(database, []string{name})
	if err != nil {
		return nil, err
	}
	return apocTriggerRows(removed, false), nil
}

func callApocTriggerDropAll(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
	runtime, err := e.apocTriggerSystemRuntime()
	if err != nil {
		return nil, err
	}
	database, err := requiredProcedureString("apoc.trigger.dropAll", args, 0, "databaseName")
	if err != nil {
		return nil, err
	}
	removed, err := runtime.remove(database, nil)
	if err != nil {
		return nil, err
	}
	return apocTriggerRows(removed, false), nil
}

func (e *StorageExecutor) callApocTriggerSetPaused(procedure string, args []interface{}, paused bool) (*ExecuteResult, error) {
	runtime, err := e.apocTriggerSystemRuntime()
	if err != nil {
		return nil, err
	}
	database, err := requiredProcedureString(procedure, args, 0, "databaseName")
	if err != nil {
		return nil, err
	}
	name, err := requiredProcedureString(procedure, args, 1, "name")
	if err != nil {
		return nil, err
	}
	trigger, err := runtime.setPaused(database, name, paused)
	if err != nil {
		return nil, err
	}
	if trigger == nil {
		return apocTriggerRows(nil, true), nil
	}
	return apocTriggerRows([]*apocTrigger{trigger}, true), nil
}

func callApocTriggerShow(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
	runtime, err := e.apocTriggerSystemRuntime()
	if err != nil {
		return nil, err
	}
	database, err := requiredProcedureString("apoc.trigger.show", args, 0, "databaseName")
	if err != nil {
		return nil, err
	}
	return apocTriggerRows(runtime.list(database), true), nil
}

// callApocTriggerAdd is the deprecated add: install for the session
// database. Replacing a trigger returns the old one (installed false), then
// the new one.
func callApocTriggerAdd(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
	runtime, err := e.apocTriggerRuntime()
	if err != nil {
		return nil, err
	}
	database := e.currentDatabaseName()
	trigger, err := e.apocTriggerDefinition("apoc.trigger.add", database, args)
	if err != nil {
		return nil, err
	}
	var previous []*apocTrigger
	for _, existing := range runtime.list(database) {
		if existing.Name == trigger.Name {
			previous = append(previous, existing)
		}
	}
	if err := runtime.put(trigger); err != nil {
		return nil, err
	}
	result := apocTriggerRows(previous, false)
	result.Rows = append(result.Rows, apocTriggerRows([]*apocTrigger{trigger}, true).Rows...)
	return result, nil
}

// callApocTriggerRemove is the deprecated remove: one row, the removed
// trigger, or the name alone when there was none.
func callApocTriggerRemove(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
	runtime, err := e.apocTriggerRuntime()
	if err != nil {
		return nil, err
	}
	name, err := requiredProcedureString("apoc.trigger.remove", args, 0, "name")
	if err != nil {
		return nil, err
	}
	removed, err := runtime.remove(e.currentDatabaseName(), []string{name})
	if err != nil {
		return nil, err
	}
	if len(removed) == 0 {
		return &ExecuteResult{Columns: apocTriggerInfoColumns, Rows: [][]interface{}{{name, nil, nil, nil, false, false}}}, nil
	}
	return apocTriggerRows(removed, false), nil
}

// callApocTriggerSessionPaused is the deprecated pause and resume; a
// trigger that isn't installed is an error, as in APOC.
func (e *StorageExecutor) callApocTriggerSessionPaused(procedure string, args []interface{}, paused bool) (*ExecuteResult, error) {
	runtime, err := e.apocTriggerRuntime()
	if err != nil {
		return nil, err
	}
	name, err := requiredProcedureString(procedure, args, 0, "name")
	if err != nil {
		return nil, err
	}
	trigger, err := runtime.setPaused(e.currentDatabaseName(), name, paused)
	if err != nil {
		return nil, err
	}
	if trigger == nil {
		return nil, apocTriggerCallError(localization.CypherProceduresTriggerNotFound(name))
	}
	return apocTriggerRows([]*apocTrigger{trigger}, true), nil
}

// apocTriggerRows are TriggerInfo rows.
func apocTriggerRows(triggers []*apocTrigger, installed bool) *ExecuteResult {
	rows := make([][]interface{}, 0, len(triggers))
	for _, trigger := range triggers {
		var selector, params interface{}
		if trigger.Selector != nil {
			selector = trigger.Selector
		}
		if trigger.Params != nil {
			params = trigger.Params
		}
		rows = append(rows, []interface{}{trigger.Name, trigger.Statement, selector, params, installed, trigger.Paused})
	}
	return &ExecuteResult{Columns: apocTriggerInfoColumns, Rows: rows}
}

// apocTriggerJSONRoundTrip is value as it reads back from its JSON: what
// the store keeps.
func apocTriggerJSONRoundTrip(value map[string]interface{}) (map[string]interface{}, error) {
	if value == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return apocTriggerDecodeJSON(encoded)
}

// apocTriggerDecodeJSON decodes a stored map; whole numbers are integers,
// as APOC's JSON reader gives them.
func apocTriggerDecodeJSON(encoded []byte) (map[string]interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded map[string]interface{}
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	converted, _ := apocTriggerJSONNumbers(decoded).(map[string]interface{})
	return converted, nil
}

func apocTriggerJSONNumbers(value interface{}) interface{} {
	switch typed := value.(type) {
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer
		}
		float, _ := typed.Float64()
		return float
	case map[string]interface{}:
		for key, element := range typed {
			typed[key] = apocTriggerJSONNumbers(element)
		}
		return typed
	case []interface{}:
		for index, element := range typed {
			typed[index] = apocTriggerJSONNumbers(element)
		}
		return typed
	}
	return value
}
