package cypher

import (
	"context"
	"encoding/base64"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
)

// apocCustomPrefix is the namespace APOC gives a custom procedure: one
// installed as walk is called as custom.walk (#1066).
const apocCustomPrefix = "custom."

// customSignature is a parsed apoc.custom signature,
// name(argument [= default] :: TYPE, ...) :: (output :: TYPE, ...).
type customSignature struct {
	name     string
	args     []string
	types    []string
	defaults []string
	outputs  []ProcedureColumn
}

// parseCustomSignature parses an apoc.custom signature. A default may come
// before or after the type (depth = 2 :: INTEGER, depth :: INTEGER = 2); the
// outputs may be omitted or VOID.
func parseCustomSignature(signature string) (customSignature, bool) {
	signature = strings.TrimSpace(signature)
	open := strings.IndexByte(signature, '(')
	if open <= 0 {
		return customSignature{}, false
	}
	closing := findMatchingParen(signature, open)
	if closing < 0 {
		return customSignature{}, false
	}
	parsed := customSignature{name: strings.TrimPrefix(strings.TrimSpace(signature[:open]), apocCustomPrefix)}
	for _, part := range strings.Split(parsed.name, ".") {
		if !cyphertext.IsIdentifier(part) {
			return customSignature{}, false
		}
	}
	seen := map[string]bool{}
	for _, part := range splitTopLevelComma(signature[open+1 : closing]) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, typeName, defaultLiteral := part, "ANY", ""
		if separator := strings.Index(part, "::"); separator >= 0 {
			name, typeName = strings.TrimSpace(part[:separator]), strings.TrimSpace(part[separator+2:])
			if equals := strings.Index(typeName, "="); equals >= 0 {
				defaultLiteral, typeName = strings.TrimSpace(typeName[equals+1:]), strings.TrimSpace(typeName[:equals])
			}
		}
		if equals := strings.Index(name, "="); equals >= 0 {
			defaultLiteral, name = strings.TrimSpace(name[equals+1:]), strings.TrimSpace(name[:equals])
		}
		name = strings.TrimPrefix(name, "$")
		if !cyphertext.IsIdentifier(name) || seen[name] || typeName == "" {
			return customSignature{}, false
		}
		if defaultLiteral == "" && len(parsed.defaults) > 0 && parsed.defaults[len(parsed.defaults)-1] != "" {
			return customSignature{}, false // a required argument after an optional one
		}
		seen[name] = true
		parsed.args = append(parsed.args, name)
		parsed.types = append(parsed.types, upperASCII(typeName))
		parsed.defaults = append(parsed.defaults, defaultLiteral)
	}
	rest := strings.TrimSpace(signature[closing+1:])
	if rest == "" {
		return parsed, true
	}
	if !strings.HasPrefix(rest, "::") {
		return customSignature{}, false
	}
	rest = strings.TrimSpace(rest[2:])
	if strings.EqualFold(rest, "VOID") {
		return parsed, true
	}
	inner, ok := stripEnclosingRowDelimiter(rest, '(', ')')
	if !ok {
		return customSignature{}, false
	}
	for _, part := range splitTopLevelComma(inner) {
		name, typeName, found := strings.Cut(part, "::")
		name, typeName = strings.TrimSpace(name), strings.TrimSpace(typeName)
		if !found || !cyphertext.IsIdentifier(name) || typeName == "" {
			return customSignature{}, false
		}
		parsed.outputs = append(parsed.outputs, ProcedureColumn{Name: name, Type: upperASCII(typeName)})
	}
	return parsed, true
}

// text renders the signature as the registry lists it.
func (s customSignature) text() string {
	args := make([]string, len(s.args))
	for i, arg := range s.args {
		args[i] = arg + " :: " + s.types[i]
		if s.defaults[i] != "" {
			args[i] = arg + " = " + s.defaults[i] + " :: " + s.types[i]
		}
	}
	out := apocCustomPrefix + s.name + "(" + strings.Join(args, ", ") + ")"
	if len(s.outputs) == 0 {
		return out + " :: VOID"
	}
	outputs := make([]string, len(s.outputs))
	for i, column := range s.outputs {
		outputs[i] = column.Name + " :: " + column.Type
	}
	return out + " :: (" + strings.Join(outputs, ", ") + ")"
}

// customStringArgument reads args[index] as a STRING, fallback when absent
// or null.
func customStringArgument(procedure string, args []interface{}, index int, name, fallback string) (string, error) {
	if index >= len(args) || args[index] == nil {
		return fallback, nil
	}
	text, ok := args[index].(string)
	if !ok {
		return "", localizedError(localization.CypherProceduresCustomArgumentType(procedure, name, "STRING"), nil)
	}
	return text, nil
}

// callApocCustomInstall installs a custom procedure from its signature and
// statement, replacing one of the same name, as APOC does. installProcedure
// takes (signature, statement, databaseName, mode, description) and
// declareProcedure (signature, statement, mode, description); the procedure
// is installed in the current database.
func (e *StorageExecutor) callApocCustomInstall(ctx context.Context, procedure string, args []interface{}, hasDatabaseName bool) (*ExecuteResult, error) {
	if e.txContext != nil && e.txContext.active {
		return nil, localizedError(localization.CypherProceduresCreateInTransaction(), nil)
	}
	signatureText, err := customStringArgument(procedure, args, 0, "signature", "")
	if err != nil {
		return nil, err
	}
	statement, err := customStringArgument(procedure, args, 1, "statement", "")
	if err != nil {
		return nil, err
	}
	next := 2
	if hasDatabaseName {
		if _, err := customStringArgument(procedure, args, next, "databaseName", "neo4j"); err != nil {
			return nil, err
		}
		next++
	}
	mode, err := customStringArgument(procedure, args, next, "mode", "read")
	if err != nil {
		return nil, err
	}
	description, err := customStringArgument(procedure, args, next+1, "description", "")
	if err != nil {
		return nil, err
	}
	signature, ok := parseCustomSignature(signatureText)
	if !ok {
		return nil, localizedError(localization.CypherProceduresCustomSignatureInvalid(signatureText), nil)
	}
	if strings.TrimSpace(statement) == "" {
		return nil, localizedError(localization.CypherProceduresBodyRequired(), nil)
	}
	minArgs := 0
	for minArgs < len(signature.defaults) && signature.defaults[minArgs] == "" {
		minArgs++
	}
	err = e.installPersistedProcedure(ctx, persistedProcedureRecord{
		Name:        apocCustomPrefix + signature.name,
		ArgNames:    signature.args,
		Mode:        upperASCII(strings.TrimSpace(mode)),
		Body:        strings.TrimSpace(statement),
		Signature:   signature.text(),
		Description: description,
		MinArgs:     minArgs,
		MaxArgs:     len(signature.args),
		UpdatedAt:   time.Now().UTC().Unix(),
		ArgTypes:    signature.types,
		ArgDefaults: signature.defaults,
		Outputs:     signature.outputs,
	}, true)
	if err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// callApocCustomDrop drops a custom procedure installed in the current
// database: apoc.custom.dropProcedure(name, databaseName).
func (e *StorageExecutor) callApocCustomDrop(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	const procedure = "apoc.custom.dropProcedure"
	if e.txContext != nil && e.txContext.active {
		return nil, localizedError(localization.CypherProceduresDropInTransaction(), nil)
	}
	name, err := customStringArgument(procedure, args, 0, "name", "")
	if err != nil {
		return nil, err
	}
	if _, err := customStringArgument(procedure, args, 1, "databaseName", "neo4j"); err != nil {
		return nil, err
	}
	if err := e.dropPersistedProcedure(ctx, apocCustomPrefix+strings.TrimPrefix(strings.TrimSpace(name), apocCustomPrefix)); err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// callApocCustomList lists the custom procedures installed in the current
// database, in APOC's columns: apoc.custom.list() and apoc.custom.show().
func (e *StorageExecutor) callApocCustomList(ctx context.Context) (*ExecuteResult, error) {
	result := &ExecuteResult{Columns: []string{"type", "name", "description", "mode", "statement", "inputs", "outputs", "forceSingle"}, Rows: [][]interface{}{}}
	nodes, err := e.getStorage(ctx).GetNodesByLabel(procedureCatalogLabel)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresCatalogReadFailed(err), err)
	}
	for _, node := range nodes {
		encoded, _ := node.Properties["record"].(string)
		payload, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			continue
		}
		var record persistedProcedureRecord
		if util.DecodeMsgpackBytes(payload, &record) != nil || !strings.HasPrefix(record.Name, apocCustomPrefix) {
			continue
		}
		inputs := make([]interface{}, len(record.ArgNames))
		for i, arg := range record.ArgNames {
			input := []interface{}{arg, "ANY"}
			if i < len(record.ArgTypes) {
				input[1] = lowerASCII(record.ArgTypes[i])
			}
			if i < len(record.ArgDefaults) && record.ArgDefaults[i] != "" {
				input = append(input, record.ArgDefaults[i])
			}
			inputs[i] = input
		}
		outputs := make([]interface{}, len(record.Outputs))
		for i, column := range record.Outputs {
			outputs[i] = []interface{}{column.Name, lowerASCII(column.Type)}
		}
		result.Rows = append(result.Rows, []interface{}{
			"procedure", strings.TrimPrefix(record.Name, apocCustomPrefix), record.Description,
			lowerASCII(record.Mode), record.Body, inputs, outputs, false,
		})
	}
	return result, nil
}
