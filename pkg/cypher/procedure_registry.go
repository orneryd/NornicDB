package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
	"golang.org/x/text/language"
)

// ProcedureMode represents Neo4j-compatible procedure execution mode.
type ProcedureMode string

const (
	ProcedureModeRead   ProcedureMode = "READ"
	ProcedureModeWrite  ProcedureMode = "WRITE"
	ProcedureModeSchema ProcedureMode = "SCHEMA"
	ProcedureModeAdmin  ProcedureMode = "ADMIN"
	ProcedureModeDBMS   ProcedureMode = "DBMS"
)

// ProcedureParam defines one procedure argument in canonical metadata.
// Description and IsDeprecated are exposed by SHOW PROCEDURES. Default is
// its display representation, not an argument value; an empty Default omits
// the metadata entry. Optional does not change the procedure's declared arity.
//
// For example, an optional map argument can use:
//
//	ProcedureParam{Name: "options", Type: "MAP", Optional: true,
//		Default: "DefaultParameterValue{value={}, type=MAP}"}
type ProcedureParam struct {
	Name         string
	Type         string
	Optional     bool
	Description  string
	Default      string
	IsDeprecated bool
}

// ProcedureColumn defines one YIELD column in canonical metadata.
// Description and IsDeprecated describe that column in SHOW PROCEDURES.
type ProcedureColumn struct {
	Name         string
	Type         string
	Description  string
	IsDeprecated bool
}

// ProcedureSpec is the canonical contract for built-in and user-defined procedures.
// Admin, IsDeprecated and DeprecatedBy describe the procedure in SHOW PROCEDURES;
// they do not replace handler authorization or change execution behavior.
// An empty DeprecatedBy is exposed as null. For example, a deprecated procedure
// can set IsDeprecated: true and DeprecatedBy: "custom.replacement".
type ProcedureSpec struct {
	Name               string
	Signature          string
	Description        string
	DescriptionMessage localization.Message
	Mode               ProcedureMode
	WorksOnSystem      bool
	Admin              bool
	IsDeprecated       bool
	DeprecatedBy       string
	Params             []ProcedureParam
	Returns            []ProcedureColumn
	MinArgs            int
	MaxArgs            int
}

// writes reports whether a call of the procedure changes the database: its
// data (WRITE) or its schema (SCHEMA).
func (s ProcedureSpec) writes() bool {
	return s.Mode == ProcedureModeWrite || s.Mode == ProcedureModeSchema
}

// ProcedureMetadataRenderer renders localized procedure metadata for a request context.
type ProcedureMetadataRenderer interface {
	Render(context.Context, localization.Message) (string, language.Tag, error)
}

// ProcedureHandler executes a registered procedure.
type ProcedureHandler func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error)

type registeredProcedure struct {
	Spec    ProcedureSpec
	Handler ProcedureHandler
	User    bool
}

type ProcedureRegistry struct {
	mu       sync.RWMutex
	builtins map[string]registeredProcedure
	user     map[string]registeredProcedure
}

func NewProcedureRegistry() *ProcedureRegistry {
	return &ProcedureRegistry{
		builtins: make(map[string]registeredProcedure),
		user:     make(map[string]registeredProcedure),
	}
}

func (r *ProcedureRegistry) RegisterBuiltIn(spec ProcedureSpec, handler ProcedureHandler) error {
	if err := validateProcedureSpec(spec); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("procedure %q: nil handler", spec.Name)
	}
	key := lowerASCII(spec.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.builtins[key] = registeredProcedure{Spec: spec, Handler: handler}
	return nil
}

func (r *ProcedureRegistry) RegisterUser(spec ProcedureSpec, handler ProcedureHandler) error {
	if err := validateProcedureSpec(spec); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("procedure %q: nil handler", spec.Name)
	}
	key := lowerASCII(spec.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.user[key] = registeredProcedure{Spec: spec, Handler: handler, User: true}
	return nil
}

func (r *ProcedureRegistry) Get(name string) (registeredProcedure, bool) {
	key := lowerASCII(name)
	r.mu.RLock()
	defer r.mu.RUnlock()
	if p, ok := r.user[key]; ok {
		return p, true
	}
	p, ok := r.builtins[key]
	return p, ok
}

func (r *ProcedureRegistry) List() []ProcedureSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ProcedureSpec, 0, util.SafePreallocSum(len(r.builtins), len(r.user)))
	for _, p := range r.builtins {
		out = append(out, p.Spec)
	}
	for _, p := range r.user {
		out = append(out, p.Spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *ProcedureRegistry) ListBuiltIns() []ProcedureSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ProcedureSpec, 0, len(r.builtins))
	for _, p := range r.builtins {
		out = append(out, p.Spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *ProcedureRegistry) ClearUser() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.user = make(map[string]registeredProcedure)
}

func validateProcedureSpec(spec ProcedureSpec) error {
	if strings.TrimSpace(spec.Name) == "" {
		return fmt.Errorf("procedure name cannot be empty")
	}
	if spec.MinArgs < 0 {
		spec.MinArgs = 0
	}
	if spec.MaxArgs >= 0 && spec.MaxArgs < spec.MinArgs {
		return fmt.Errorf("procedure %q: MaxArgs(%d) < MinArgs(%d)", spec.Name, spec.MaxArgs, spec.MinArgs)
	}
	return nil
}

func procedureOutputTypes(clause string) map[string]string {
	procedure, found := globalProcedureRegistry.Get(extractProcedureName(clause))
	if !found {
		return nil
	}
	outputs := procedure.Spec.Returns
	if len(outputs) == 0 {
		separator := strings.LastIndex(procedure.Spec.Signature, " :: (")
		if separator < 0 || !strings.HasSuffix(procedure.Spec.Signature, ")") {
			return nil
		}
		for _, output := range splitTopLevelComma(procedure.Spec.Signature[separator+5 : len(procedure.Spec.Signature)-1]) {
			if name, typeName, ok := strings.Cut(output, "::"); ok {
				outputs = append(outputs, ProcedureColumn{Name: strings.TrimSpace(name), Type: typeName})
			}
		}
	}
	result := make(map[string]string, len(outputs))
	for _, output := range outputs {
		if typeName := procedureStaticType(output.Type); typeName != "" {
			result[output.Name] = typeName
		}
	}
	return result
}

func procedureStaticType(typeName string) string {
	typeName = strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(typeName)), "?")
	if strings.HasPrefix(typeName, "LIST<") && strings.HasSuffix(typeName, ">") {
		element := procedureStaticType(typeName[5 : len(typeName)-1])
		if element == "" {
			element = "Any"
		}
		return "List<" + element + ">"
	}
	switch typeName {
	case "STRING", "INTEGER", "FLOAT", "BOOLEAN", "NODE", "RELATIONSHIP", "PATH", "MAP":
		return typeName[:1] + strings.ToLower(typeName[1:])
	default:
		return ""
	}
}

var globalProcedureRegistry = NewProcedureRegistry()

// RegisterUserProcedure registers a user-defined procedure into the global registry.
func RegisterUserProcedure(spec ProcedureSpec, handler ProcedureHandler) error {
	return globalProcedureRegistry.RegisterUser(spec, handler)
}

// ListRegisteredProcedures returns built-in and user-registered procedures.
func ListRegisteredProcedures() []ProcedureSpec {
	return globalProcedureRegistry.List()
}

// ClearUserProcedures resets user-defined procedures (primarily for tests/reload paths).
func ClearUserProcedures() {
	globalProcedureRegistry.ClearUser()
}

func validateProcedureArgCount(spec ProcedureSpec, args []interface{}) error {
	return validateProcedureArgumentCount(spec, len(args))
}

func validateProcedureArgumentCount(spec ProcedureSpec, count int) error {
	if spec.MinArgs > 0 && count < spec.MinArgs {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidNumberOfArguments",
			fmt.Sprintf("procedure %s requires at least %d arguments, got %d", spec.Name, spec.MinArgs, count),
		)
	}
	if spec.MaxArgs >= 0 && count > spec.MaxArgs {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidNumberOfArguments",
			fmt.Sprintf("procedure %s accepts at most %d arguments, got %d", spec.Name, spec.MaxArgs, count),
		)
	}
	return nil
}
