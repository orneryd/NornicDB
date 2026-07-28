package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/util"
)

// ProcedureMode represents Neo4j-compatible procedure execution mode.
type ProcedureMode string

const (
	ProcedureModeRead  ProcedureMode = "READ"
	ProcedureModeWrite ProcedureMode = "WRITE"
	ProcedureModeDBMS  ProcedureMode = "DBMS"
)

// ProcedureParam defines one procedure argument in canonical metadata.
type ProcedureParam struct {
	Name     string
	Type     string
	Optional bool
}

// ProcedureColumn defines one YIELD column in canonical metadata.
type ProcedureColumn struct {
	Name string
	Type string
}

// ProcedureSpec is the canonical contract for built-in and user-defined procedures.
type ProcedureSpec struct {
	Name          string
	Signature     string
	Description   string
	Mode          ProcedureMode
	WorksOnSystem bool
	Params        []ProcedureParam
	Returns       []ProcedureColumn
	MinArgs       int
	MaxArgs       int
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
	key := strings.ToLower(spec.Name)
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
	key := strings.ToLower(spec.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.user[key] = registeredProcedure{Spec: spec, Handler: handler, User: true}
	return nil
}

func (r *ProcedureRegistry) Get(name string) (registeredProcedure, bool) {
	key := strings.ToLower(name)
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
	if spec.MinArgs > 0 && len(args) < spec.MinArgs {
		return fmt.Errorf("procedure %s requires at least %d arguments, got %d", spec.Name, spec.MinArgs, len(args))
	}
	if spec.MaxArgs >= 0 && len(args) > spec.MaxArgs {
		return fmt.Errorf("procedure %s accepts at most %d arguments, got %d", spec.Name, spec.MaxArgs, len(args))
	}
	return nil
}
