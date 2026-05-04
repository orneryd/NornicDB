// Package observability — Phase 5 K8s autodetect.
//
// k8sProbe inspects env + filesystem signals (KUBERNETES_SERVICE_HOST and
// /var/run/secrets/kubernetes.io/serviceaccount/token presence) to decide
// whether tenant labels should default ON. Conservative AND-signal logic
// per CONTEXT.md D-02. Pure-ish function: only stdlib os reads.
//
// Phase 5 / Plan 05-03 fills the Detect + ResolveTenantLabels bodies.
package observability

import (
	"log/slog"
	"os"
	"strings"
)

// Reason* are the closed-set source strings logged at startup (D-02b).
const (
	ReasonExplicitYAML      = "explicit_yaml"
	ReasonK8sDetected       = "k8s_detected"
	ReasonServiceHostAbsent = "not_k8s_service_host_absent"
	ReasonTokenFileAbsent   = "not_k8s_token_file_absent"
	ReasonTokenFileEmpty    = "not_k8s_token_file_empty"
	ReasonTokenStatError    = "not_k8s_token_stat_error"
)

// Well-known K8s mount points pinned for grep-discoverability.
const (
	k8sServiceHostEnv = "KUBERNETES_SERVICE_HOST"
	k8sTokenPath      = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// k8sProbe is the AGENTS.md §4 functional-DI shape. Tests construct one
// with stub Getenv/StatFile; production wires os.Getenv / os.Stat.
type k8sProbe struct {
	Getenv   func(string) string
	StatFile func(string) (os.FileInfo, error)
}

// DefaultK8sProbe returns a probe wired to the live OS reads.
func DefaultK8sProbe() k8sProbe {
	return k8sProbe{Getenv: os.Getenv, StatFile: os.Stat}
}

// Detect runs the AND-signal autodetect per CONTEXT.md D-02:
//   - KUBERNETES_SERVICE_HOST env var present (non-empty after TrimSpace), AND
//   - /var/run/secrets/kubernetes.io/serviceaccount/token exists with size > 0.
//
// Returns one of the six Reason* string constants documenting WHY the result
// was chosen (logged once at startup per D-02b for operator forensics).
//
// Security note (T-05-11): the token's contents are never read — only its
// presence and size are inspected via os.Stat. JWT bytes never enter process
// memory.
func (p k8sProbe) Detect() (enabled bool, reason string) {
	if p.Getenv == nil || p.StatFile == nil {
		// Defensive: a caller forgot to wire either getter. Treat as not-K8s.
		return false, ReasonServiceHostAbsent
	}
	serviceHost := strings.TrimSpace(p.Getenv(k8sServiceHostEnv))
	if serviceHost == "" {
		return false, ReasonServiceHostAbsent
	}
	info, err := p.StatFile(k8sTokenPath)
	switch {
	case err != nil && os.IsNotExist(err):
		return false, ReasonTokenFileAbsent
	case err != nil:
		// Permission denied or other stat error — fail-safe to not-K8s.
		return false, ReasonTokenStatError
	case info == nil:
		// Defensive: StatFile returned (nil, nil) — treat as absent.
		// The os.Stat signature does not guarantee non-nil FileInfo on nil error,
		// and custom StatFile implementations may return this combination.
		return false, ReasonTokenFileAbsent
	case info.Size() == 0:
		// Empty token file — likely a volume-mount race during pod startup.
		return false, ReasonTokenFileEmpty
	default:
		return true, ReasonK8sDetected
	}
}

// ResolveAndLogTenantLabels is the cmd-level convenience wrapper used at
// startup to resolve the tenant-labels-enabled bool AND emit the single
// MET-22 forensic log line in one call. It:
//
//  1. Constructs the production K8s probe (DefaultK8sProbe).
//  2. Resolves the bool via ResolveTenantLabels(explicit, probe).
//  3. Re-derives the two boolean signal flags from the same probe inputs
//     so the log line documents what was actually checked (no duplicate
//     AND-logic).
//  4. Emits exactly one slog INFO record via the supplied logger with the
//     four canonical fields: enabled, reason, service_host_present,
//     token_file_present.
//
// LOG-09 compliance: the caller must inject the *slog.Logger — this helper
// never touches slog.Default() / slog.SetDefault(). cmd/nornicdb/main.go
// passes the same Phase 2 D-08 logger that flows to pkg/server / pkg/bolt.
//
// Returns the resolved bool which the caller writes into
// cfg.Observability.Metrics.TenantLabelsEnabled before any Phase 4 bag
// constructor reads it.
func ResolveAndLogTenantLabels(explicit *bool, logger *slog.Logger) bool {
	probe := DefaultK8sProbe()
	resolved, source := ResolveTenantLabels(explicit, probe)
	serviceHostPresent := strings.TrimSpace(probe.Getenv(k8sServiceHostEnv)) != ""
	tokenInfo, tokenStatErr := probe.StatFile(k8sTokenPath)
	// Mirror the same size check Detect() uses: "present" means exists AND non-empty.
	tokenFilePresent := tokenStatErr == nil && tokenInfo != nil && tokenInfo.Size() > 0
	logger.Info("resolved tenant labels enabled",
		"enabled", resolved,
		"reason", source,
		"service_host_present", serviceHostPresent,
		"token_file_present", tokenFilePresent,
	)
	return resolved
}

// ResolveTenantLabels enforces D-02a precedence:
//
//	explicit YAML (TenantLabelsExplicit *bool, R-02) > K8s autodetect > default false.
//
// When explicit is non-nil the autodetect probe is short-circuited and the
// operator's intent wins (returning ReasonExplicitYAML). When explicit is nil
// the probe's Detect outcome and reason are returned verbatim.
func ResolveTenantLabels(explicit *bool, probe k8sProbe) (resolved bool, source string) {
	if explicit != nil {
		return *explicit, ReasonExplicitYAML
	}
	return probe.Detect()
}
