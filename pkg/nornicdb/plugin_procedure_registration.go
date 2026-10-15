package nornicdb

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"reflect"
	"strings"
)

type pluginProcedureDefinition struct {
	spec    cypher.ProcedureSpec
	handler cypher.ProcedureHandler
}

// registerPluginProcedures validates every declaration before publishing any:
// a malformed plugin must not leave some of its procedures available to CALL.
func registerPluginProcedures(procedures []PluginProcedure) error {
	definitions := make([]pluginProcedureDefinition, 0, len(procedures))
	validation := cypher.NewProcedureRegistry()
	for _, proc := range procedures {
		definition, err := pluginProcedureForCypher(proc)
		if err != nil {
			return err
		}
		if err := validation.RegisterUser(definition.spec, definition.handler); err != nil {
			return err
		}
		definitions = append(definitions, definition)
	}
	for _, definition := range definitions {
		if err := cypher.RegisterUserProcedure(definition.spec, definition.handler); err != nil {
			return err
		}
	}
	return nil
}

func registerProcedureWithCypher(proc PluginProcedure) error {
	return registerPluginProcedures([]PluginProcedure{proc})
}

func pluginProcedureForCypher(proc PluginProcedure) (pluginProcedureDefinition, error) {
	var handler cypher.ProcedureHandler
	switch fn := proc.Handler.(type) {
	case cypher.ProcedureHandler:
		handler = fn
	case func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error):
		handler = fn
	case func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error):
		if fn != nil {
			handler = func(ctx context.Context, _ *cypher.StorageExecutor, query string, args []interface{}) (*cypher.ExecuteResult, error) {
				return fn(ctx, query, args)
			}
		}
	}
	if handler == nil || reflect.ValueOf(handler).IsNil() {
		return pluginProcedureDefinition{}, fmt.Errorf("plugin procedure %q: unsupported or nil handler", proc.Name)
	}
	mode := cypher.ProcedureModeRead
	switch strings.ToUpper(proc.Mode) {
	case "", "READ":
	case "WRITE":
		mode = cypher.ProcedureModeWrite
	case "SCHEMA":
		mode = cypher.ProcedureModeSchema
	case "ADMIN":
		mode = cypher.ProcedureModeAdmin
	case "DBMS":
		mode = cypher.ProcedureModeDBMS
	default:
		return pluginProcedureDefinition{}, fmt.Errorf("plugin procedure %q: invalid mode %q", proc.Name, proc.Mode)
	}
	spec := cypher.ProcedureSpec{Name: proc.Name, Signature: proc.Signature, Description: proc.Description, Mode: mode, WorksOnSystem: proc.WorksOnSystem, MinArgs: proc.MinArgs, MaxArgs: proc.MaxArgs}
	checked := func(ctx context.Context, exec *cypher.StorageExecutor, query string, args []interface{}) (result *cypher.ExecuteResult, err error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				result = nil
				err = fmt.Errorf("plugin procedure %q panicked: %v", proc.Name, recovered)
			}
		}()
		result, err = handler(ctx, exec, query, args)
		if result == nil && err == nil {
			return nil, fmt.Errorf("plugin procedure %q returned nil result", proc.Name)
		}
		return result, err
	}
	return pluginProcedureDefinition{spec, checked}, nil
}
