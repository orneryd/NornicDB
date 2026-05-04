// Package observability — Phase 5 K8s autodetect.
//
// k8sProbe inspects env + filesystem signals (KUBERNETES_SERVICE_HOST and
// /var/run/secrets/kubernetes.io/serviceaccount/token presence) to decide
// whether tenant labels should default ON. Conservative AND-signal logic
// per CONTEXT.md D-02. Pure-ish function: only stdlib os reads.
//
// Phase 5 / Plan 05-03 fills the Detect + ResolveTenantLabels bodies.
// This file is the Wave-0 skeleton.
package observability

import "os"

// Reason* are the closed-set source strings logged at startup (D-02b).
const (
	ReasonExplicitYAML      = "explicit_yaml"
	ReasonK8sDetected       = "k8s_detected"
	ReasonServiceHostAbsent = "not_k8s_service_host_absent"
	ReasonTokenFileAbsent   = "not_k8s_token_file_absent"
	ReasonTokenFileEmpty    = "not_k8s_token_file_empty"
	ReasonTokenStatError    = "not_k8s_token_stat_error"
)

// k8sProbe is the AGENTS.md §4 functional-DI shape. Tests construct one
// with stub Getenv/StatFile; production wires os.Getenv / os.Stat.
type k8sProbe struct {
	Getenv   func(string) string
	StatFile func(string) (os.FileInfo, error)
}

// DefaultK8sProbe returns a probe wired to the live OS reads.
// Wave-0 stub returns the zero-value struct. Plan 05-03 implements.
func DefaultK8sProbe() k8sProbe { return k8sProbe{} }

// Detect runs the AND-signal autodetect. Wave-0 stub. Plan 05-03 implements.
func (p k8sProbe) Detect() (enabled bool, reason string) { return false, "" }

// ResolveTenantLabels enforces precedence: explicit YAML > autodetect > default false.
// Wave-0 stub. Plan 05-03 implements.
func ResolveTenantLabels(explicit *bool, probe k8sProbe) (resolved bool, source string) {
	_ = explicit
	_ = probe
	return false, ""
}
