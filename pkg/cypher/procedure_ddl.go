package cypher

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"time"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	procedureCatalogLabel  = "_ProcedureCatalog"
	procedureCatalogPrefix = "__proc__:"
)

var (
	createProcedurePattern = regexp.MustCompile(`(?is)^CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+([a-zA-Z_][a-zA-Z0-9_\.]*)\s*\((.*?)\)\s+MODE\s+(READ|WRITE|SCHEMA|ADMIN|DBMS)\s+AS\s+(.+)$`)
	dropProcedurePattern   = regexp.MustCompile(`(?is)^DROP\s+PROCEDURE\s+([a-zA-Z_][a-zA-Z0-9_\.]*)\s*$`)
)

type persistedProcedureRecord struct {
	Name        string   `msgpack:"name"`
	ArgNames    []string `msgpack:"args"`
	Mode        string   `msgpack:"mode"`
	Body        string   `msgpack:"body"`
	Signature   string   `msgpack:"sig"`
	Description string   `msgpack:"desc"`
	MinArgs     int      `msgpack:"min"`
	MaxArgs     int      `msgpack:"max"`
	UpdatedAt   int64    `msgpack:"updated_at"`
}

func isCreateProcedureCommand(cypher string) bool {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	return strings.HasPrefix(upper, "CREATE PROCEDURE") || strings.HasPrefix(upper, "CREATE OR REPLACE PROCEDURE")
}

func isDropProcedureCommand(cypher string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(cypher)), "DROP PROCEDURE")
}

func (e *StorageExecutor) executeCreateProcedure(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.txContext != nil && e.txContext.active {
		return nil, localizedError(localization.CypherProceduresCreateInTransaction(), nil)
	}
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}
	m := createProcedurePattern.FindStringSubmatch(strings.TrimSpace(cypher))
	if len(m) != 6 {
		return nil, localizedError(localization.CypherProceduresCreateInvalidSyntax(), nil)
	}

	replace := strings.TrimSpace(m[1]) != ""
	name := strings.TrimSpace(m[2])
	argNames, err := parseProcedureArgNames(m[3])
	if err != nil {
		return nil, err
	}
	mode := strings.ToUpper(strings.TrimSpace(m[4]))
	body := strings.TrimSpace(m[5])
	if body == "" {
		return nil, localizedError(localization.CypherProceduresBodyRequired(), nil)
	}

	spec, handler, record, err := e.compilePersistedProcedure(persistedProcedureRecord{
		Name:      name,
		ArgNames:  argNames,
		Mode:      mode,
		Body:      body,
		Signature: buildProcedureSignature(name, argNames),
		MinArgs:   len(argNames),
		MaxArgs:   len(argNames),
		UpdatedAt: time.Now().UTC().Unix(),
	})
	if err != nil {
		return nil, err
	}

	nodeID := procedureCatalogNodeID(name)
	store := e.getStorage(ctx)
	existing, getErr := store.GetNode(nodeID)
	if getErr == nil && existing != nil && !replace {
		return nil, localizedError(localization.CypherProceduresAlreadyExists(name), nil)
	}

	blob, err := msgpack.Marshal(record)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresEncodeRecordFailed(err), err)
	}
	props := map[string]interface{}{
		"name":      record.Name,
		"mode":      record.Mode,
		"record":    base64.StdEncoding.EncodeToString(blob),
		"updatedAt": record.UpdatedAt,
	}

	if existing != nil {
		existing.Labels = ensureLabel(existing.Labels, procedureCatalogLabel)
		existing.Properties = props
		if err := store.UpdateNode(existing); err != nil {
			return nil, localizedError(localization.CypherProceduresUpdateCatalogFailed(err), err)
		}
	} else {
		node := &storage.Node{
			ID:         nodeID,
			Labels:     []string{procedureCatalogLabel},
			Properties: props,
		}
		if _, err := store.CreateNode(node); err != nil {
			return nil, localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
		}
	}

	if err := RegisterUserProcedure(spec, handler); err != nil {
		return nil, err
	}
	return &ExecuteResult{
		Columns: []string{"name", "mode", "status"},
		Rows:    [][]interface{}{{name, mode, "created"}},
	}, nil
}

func (e *StorageExecutor) executeDropProcedure(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.txContext != nil && e.txContext.active {
		return nil, localizedError(localization.CypherProceduresDropInTransaction(), nil)
	}
	m := dropProcedurePattern.FindStringSubmatch(strings.TrimSpace(cypher))
	if len(m) != 2 {
		return nil, localizedError(localization.CypherProceduresDropInvalidSyntax(), nil)
	}
	name := strings.TrimSpace(m[1])
	store := e.getStorage(ctx)
	if err := store.DeleteNode(procedureCatalogNodeID(name)); err != nil {
		return nil, localizedError(localization.CypherProceduresDropFailed(name, err), err)
	}
	// Keep implementation simple and deterministic: refresh user registry from persisted catalog.
	ClearUserProcedures()
	if err := e.loadPersistedProcedures(); err != nil {
		return nil, localizedError(localization.CypherProceduresRegistryReloadFailed(err), nerrors.ErrProcedureRegistryReloadFailed)
	}

	return &ExecuteResult{
		Columns: []string{"name", "status"},
		Rows:    [][]interface{}{{name, "dropped"}},
	}, nil
}

func parseProcedureArgNames(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{}, nil
	}
	parts := splitProcedureTopLevelComma(raw)
	args := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, p := range parts {
		arg := strings.TrimSpace(p)
		if arg == "" {
			continue
		}
		if strings.HasPrefix(arg, "$") {
			arg = strings.TrimSpace(arg[1:])
		}
		if !isValidIdentifier(arg) {
			return nil, localizedError(localization.CypherProceduresInvalidArgumentName(arg), nil)
		}
		if _, exists := seen[arg]; exists {
			return nil, localizedError(localization.CypherProceduresDuplicateArgument(arg), nil)
		}
		seen[arg] = struct{}{}
		args = append(args, arg)
	}
	return args, nil
}

func (e *StorageExecutor) compilePersistedProcedure(record persistedProcedureRecord) (ProcedureSpec, ProcedureHandler, persistedProcedureRecord, error) {
	mode := ProcedureMode(strings.ToUpper(record.Mode))
	switch mode {
	case ProcedureModeRead, ProcedureModeWrite, ProcedureModeSchema, ProcedureModeAdmin, ProcedureModeDBMS:
	default:
		return ProcedureSpec{}, nil, record, localizedError(localization.CypherProceduresInvalidMode(record.Mode), nil)
	}

	info := e.analyzer.Analyze(record.Body)
	if mode == ProcedureModeRead && info.IsWriteQuery {
		return ProcedureSpec{}, nil, record, localizedError(localization.CypherProceduresReadContainsWrite(), nil)
	}

	argNames := append([]string{}, record.ArgNames...)
	spec := ProcedureSpec{
		Name:        record.Name,
		Signature:   record.Signature,
		Description: record.Description,
		Mode:        mode,
		MinArgs:     len(argNames),
		MaxArgs:     len(argNames),
		Params:      make([]ProcedureParam, 0, len(argNames)),
	}
	for _, arg := range argNames {
		spec.Params = append(spec.Params, ProcedureParam{Name: arg, Type: "ANY"})
	}

	handler := func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		if len(args) != len(argNames) {
			return nil, localizedError(localization.CypherProceduresArgumentCount(record.Name, len(argNames), len(args)), nil)
		}
		params := make(map[string]interface{}, len(argNames))
		for i, arg := range argNames {
			params[arg] = args[i]
		}
		return exec.Execute(ctx, record.Body, params)
	}
	return spec, handler, record, nil
}

func buildProcedureSignature(name string, args []string) string {
	if len(args) == 0 {
		return fmt.Sprintf("%s() :: (value :: ANY)", name)
	}
	argSpec := make([]string, 0, len(args))
	for _, a := range args {
		argSpec = append(argSpec, fmt.Sprintf("$%s :: ANY", a))
	}
	return fmt.Sprintf("%s(%s) :: (value :: ANY)", name, strings.Join(argSpec, ", "))
}

func procedureCatalogNodeID(name string) storage.NodeID {
	return storage.NodeID(procedureCatalogPrefix + strings.ToLower(strings.TrimSpace(name)))
}

func ensureLabel(labels []string, label string) []string {
	for _, l := range labels {
		if l == label {
			return labels
		}
	}
	return append(labels, label)
}

func (e *StorageExecutor) loadPersistedProcedures() error {
	nodes, err := e.storage.GetNodesByLabel(procedureCatalogLabel)
	if err != nil {
		return localizedError(localization.CypherProceduresCatalogReadFailed(err), nerrors.ErrProcedureCatalogReadFailed)
	}
	for _, node := range nodes {
		raw, ok := node.Properties["record"]
		if !ok {
			continue
		}
		var payload []byte
		switch v := raw.(type) {
		case []byte:
			payload = v
		case string:
			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return localizedError(localization.CypherProceduresCatalogRecordDecodeFailed(string(node.ID)), nerrors.ErrProcedureCatalogRecordDecodeFailed)
			}
			payload = decoded
		default:
			return localizedError(localization.CypherProceduresCatalogRecordDecodeFailed(string(node.ID)), nerrors.ErrProcedureCatalogRecordDecodeFailed)
		}
		var record persistedProcedureRecord
		if err := util.DecodeMsgpackBytes(payload, &record); err != nil {
			return localizedError(localization.CypherProceduresCatalogRecordDecodeFailed(string(node.ID)), nerrors.ErrProcedureCatalogRecordDecodeFailed)
		}
		spec, handler, _, err := e.compilePersistedProcedure(record)
		if err != nil {
			return localizedError(localization.CypherProceduresCatalogRecordInvalid(string(node.ID)), nerrors.ErrProcedureCatalogRecordInvalid)
		}
		if err := RegisterUserProcedure(spec, handler); err != nil {
			return localizedError(localization.CypherProceduresRegistryReloadFailed(err), nerrors.ErrProcedureRegistryReloadFailed)
		}
	}
	return nil
}
