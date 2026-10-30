package cypher

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"regexp"
	"strings"
	"time"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
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
	// ArgTypes, ArgDefaults and Outputs come from a declared signature
	// (apoc.custom): each argument's type and default literal ("" when
	// required), and the output columns. Empty for CREATE PROCEDURE.
	ArgTypes    []string          `msgpack:"arg_types,omitempty"`
	ArgDefaults []string          `msgpack:"arg_defaults,omitempty"`
	Outputs     []ProcedureColumn `msgpack:"outputs,omitempty"`
}

// isCreateProcedureCommand reports whether cypher is CREATE [OR REPLACE]
// PROCEDURE. It runs on every statement's routing, so it doesn't allocate.
func isCreateProcedureCommand(cypher string) bool {
	rest := strings.TrimSpace(cypher)
	if !startsWithKeywordFold(rest, "CREATE") {
		return false
	}
	rest = strings.TrimSpace(rest[len("CREATE"):])
	if startsWithKeywordFold(rest, "OR") {
		rest = strings.TrimSpace(rest[len("OR"):])
		if !startsWithKeywordFold(rest, "REPLACE") {
			return false
		}
		rest = strings.TrimSpace(rest[len("REPLACE"):])
	}
	return startsWithKeywordFold(rest, "PROCEDURE")
}

func isDropProcedureCommand(cypher string) bool {
	return strings.HasPrefix(upperASCII(strings.TrimSpace(cypher)), "DROP PROCEDURE")
}

func (e *StorageExecutor) executeCreateProcedure(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.txContext != nil && e.txContext.active {
		return nil, localizedError(localization.CypherProceduresCreateInTransaction(), nil)
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
	mode := upperASCII(strings.TrimSpace(m[4]))
	body := strings.TrimSpace(m[5])
	if body == "" {
		return nil, localizedError(localization.CypherProceduresBodyRequired(), nil)
	}

	if err := e.installPersistedProcedure(ctx, persistedProcedureRecord{
		Name:      name,
		ArgNames:  argNames,
		Mode:      mode,
		Body:      body,
		Signature: buildProcedureSignature(name, argNames),
		MinArgs:   len(argNames),
		MaxArgs:   len(argNames),
		UpdatedAt: time.Now().UTC().Unix(),
	}, replace); err != nil {
		return nil, err
	}
	return &ExecuteResult{
		Columns: []string{"name", "mode", "status"},
		Rows:    [][]interface{}{{name, mode, "created"}},
	}, nil
}

// installPersistedProcedure compiles record, writes it to the procedure
// catalog (over an existing one of the same name only when replace is set)
// and registers it. CREATE PROCEDURE and apoc.custom share it.
func (e *StorageExecutor) installPersistedProcedure(ctx context.Context, record persistedProcedureRecord, replace bool) error {
	name := record.Name
	spec, handler, record, err := e.compilePersistedProcedure(ctx, record)
	if err != nil {
		return err
	}

	nodeID := procedureCatalogNodeID(name)
	store, err := admittedStatementWriter(ctx, e.getStorage(ctx))
	if err != nil {
		return err
	}
	existing, _, err := e.resolveProcedureCatalogIdentity(ctx, store, name)
	if err != nil {
		return err
	}
	if existing != nil && !replace {
		return localizedError(localization.CypherProceduresAlreadyExists(name), nil)
	}

	blob, err := msgpack.Marshal(record)
	if err != nil {
		return localizedError(localization.CypherProceduresEncodeRecordFailed(err), err)
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
			return localizedError(localization.CypherProceduresUpdateCatalogFailed(err), err)
		}
	} else {
		node := &storage.Node{
			ID:         nodeID,
			Labels:     []string{procedureCatalogLabel},
			Properties: props,
		}
		if _, err := store.CreateNode(node); err != nil {
			return localizedError(localization.CypherProceduresPersistCatalogFailed(err), err)
		}
	}
	return RegisterUserProcedure(spec, handler)
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
	if err := e.dropPersistedProcedure(ctx, name); err != nil {
		return nil, err
	}
	return &ExecuteResult{
		Columns: []string{"name", "status"},
		Rows:    [][]interface{}{{name, "dropped"}},
	}, nil
}

// dropPersistedProcedure removes name from the procedure catalog and reloads
// the user procedures from it.
func (e *StorageExecutor) dropPersistedProcedure(ctx context.Context, name string) error {
	store, err := admittedStatementWriter(ctx, e.getStorage(ctx))
	if err != nil {
		return err
	}
	existing, _, err := e.resolveProcedureCatalogIdentity(ctx, store, name)
	if err != nil {
		return err
	}
	if existing == nil {
		return localizedError(localization.CypherProceduresDropFailed(name, storage.ErrNotFound), storage.ErrNotFound)
	}
	if err := store.DeleteNode(existing.ID); err != nil {
		return localizedError(localization.CypherProceduresDropFailed(name, err), err)
	}
	// Keep implementation simple and deterministic: refresh user registry from persisted catalog.
	ClearUserProcedures()
	if err := e.loadPersistedProcedures(ctx); err != nil {
		return localizedError(localization.CypherProceduresRegistryReloadFailed(err), nerrors.ErrProcedureRegistryReloadFailed)
	}
	return nil
}

func parseProcedureArgNames(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{}, nil
	}
	parts := splitTopLevelComma(raw)
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
		if !cyphertext.IsIdentifier(arg) {
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

func (e *StorageExecutor) compilePersistedProcedure(ctx context.Context, record persistedProcedureRecord) (ProcedureSpec, ProcedureHandler, persistedProcedureRecord, error) {
	mode := ProcedureMode(upperASCII(record.Mode))
	switch mode {
	case ProcedureModeRead, ProcedureModeWrite, ProcedureModeSchema, ProcedureModeAdmin, ProcedureModeDBMS:
	default:
		return ProcedureSpec{}, nil, record, localizedError(localization.CypherProceduresInvalidMode(record.Mode), nil)
	}

	contract, err := readStatementExecutionContract(ctx, record.Body)
	if err != nil {
		return ProcedureSpec{}, nil, record, err
	}
	if mode == ProcedureModeRead && contract.writes {
		return ProcedureSpec{}, nil, record, localizedError(localization.CypherProceduresReadContainsWrite(), nil)
	}

	argNames := append([]string{}, record.ArgNames...)
	minArgs := len(argNames)
	if len(record.ArgDefaults) == len(argNames) {
		minArgs = record.MinArgs
	}
	spec := ProcedureSpec{
		Name:        record.Name,
		Signature:   record.Signature,
		Description: record.Description,
		Mode:        mode,
		MinArgs:     minArgs,
		MaxArgs:     len(argNames),
		Params:      make([]ProcedureParam, 0, len(argNames)),
		Returns:     append([]ProcedureColumn(nil), record.Outputs...),
		ResultCache: ProcedureResultCache{CypherInputs: []ProcedureCypherInput{{StoredText: record.Body}}},
	}
	for i, arg := range argNames {
		param := ProcedureParam{Name: arg, Type: "ANY", Optional: i >= minArgs}
		if i < len(record.ArgTypes) {
			param.Type = record.ArgTypes[i]
		}
		if i < len(record.ArgDefaults) {
			param.Default = record.ArgDefaults[i]
		}
		spec.Params = append(spec.Params, param)
	}

	handler := func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		if len(args) < minArgs || len(args) > len(argNames) {
			return nil, localizedError(localization.CypherProceduresArgumentCount(record.Name, len(argNames), len(args)), nil)
		}
		params := make(map[string]interface{}, len(argNames))
		for i, arg := range argNames {
			if i < len(args) {
				params[arg] = args[i]
				continue
			}
			// An omitted optional argument takes its declared default.
			params[arg], _ = parseLiteralValueFromComputedRow(record.ArgDefaults[i])
		}
		// The body is a statement the procedure runs, like one
		// apoc.cypher.run runs: through the one route for those, which
		// refuses administration commands and NornicDB's internal functions
		// and keeps the body in the caller's transaction.
		return exec.executeUserStatement(ctx, record.Body, params)
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
	return storage.NodeID(procedureCatalogPrefix + strings.TrimSpace(name))
}

func ensureLabel(labels []string, label string) []string {
	for _, l := range labels {
		if l == label {
			return labels
		}
	}
	return append(labels, label)
}

// Loading is its own entry: executor construction carries no request, so a
// legacy-identity migration it performs is admitted here.
func (e *StorageExecutor) loadPersistedProcedures(ctx context.Context) error {
	ctx, _ = storage.AdmitVectorWork(ctx)
	nodes, err := e.storage.GetNodesByLabel(procedureCatalogLabel)
	if err != nil {
		return localizedError(localization.CypherProceduresCatalogReadFailed(err), nerrors.ErrProcedureCatalogReadFailed)
	}
	for _, node := range nodes {
		if _, ok := node.Properties["record"]; !ok {
			continue
		}
		record, err := decodeProcedureCatalogRecord(node)
		if err != nil {
			return err
		}
		spec, handler, _, err := e.compilePersistedProcedure(ctx, record)
		if err != nil {
			return localizedError(localization.CypherProceduresCatalogRecordInvalid(string(node.ID)), nerrors.ErrProcedureCatalogRecordInvalid)
		}
		if _, superseded, err := e.resolveProcedureCatalogIdentity(ctx, e.storage, record.Name, node); err != nil {
			return err
		} else if superseded {
			continue
		}
		if err := RegisterUserProcedure(spec, handler); err != nil {
			return localizedError(localization.CypherProceduresRegistryReloadFailed(err), nerrors.ErrProcedureRegistryReloadFailed)
		}
	}
	return nil
}
