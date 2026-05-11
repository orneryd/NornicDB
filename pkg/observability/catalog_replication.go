// Package observability — Replication metric bag (Plan 04-06 GREEN per
// MET-14 + GAP-1 + RISK-3 corrected).
//
// Owns ten families per ADR §2.3 + CONTEXT §domain:
//
//	nornicdb_replication_role                          (Gauge int enum)
//	nornicdb_replication_term                          (Gauge int)
//	nornicdb_replication_commit_index                  (Gauge int)
//	nornicdb_replication_apply_index                   (Gauge int)
//	nornicdb_replication_lag_bytes{peer}               (GaugeVec)
//	nornicdb_replication_lag_entries{peer}             (GaugeVec)
//	nornicdb_replication_apply_duration_seconds        (LatencyHistogram)
//	nornicdb_replication_rtt_seconds{peer}             (HistogramVec)
//	nornicdb_replication_leader_changes_total          (Counter)
//	nornicdb_replication_last_contact_seconds{peer}    (GaugeVec — GAP-1)
//
// **RISK-3 FIX (RESEARCH §RISK-3 verified):**
// pkg/replication/config.PeerConfig has fields {ID string; Addr string} —
// NOT `Name`. CONTEXT D-05 wording is obsolete. PeerLabel(p) returns
// `p.ID` if non-empty, falling back to `p.Addr`. NEVER reads from
// `conn.RemoteAddr()` — runtime sockets would explode cardinality.
//
// **D-05a mode-aware ceiling:**
//
//	ha_standby:   8  (single peer + churn slack)
//	raft:         16 (5-node typical quorum + churn slack)
//	multi_region: 64 (3-region × ~5-peer + churn slack)
//
// Stored in modeCeiling and accessible via Mode() / Ceiling() so the
// AssertCardinalityCeiling test parameterizes by mode.
//
// **D-08a tenant-flag accepted but IGNORED at registration:**
// Replication is per-cluster, not per-database. `tenantLabelsEnabled` is
// part of the constructor signature so callers pass cfg uniformly across
// all subsystem bags, but the database label is never added to any
// replication family. Documented inline; verified by TestReplicationMetrics_*.
//
// **D-15a per-event observation (zero polling):**
// pkg/replication.RaftReplicator + HAStandbyReplicator + MultiRegionReplicator
// call metrics.Role.Set(RoleEnum("leader")) at the SAME sites that emit the
// existing role-transition log line. No background goroutine polls state.
// Pre-bound observers cached in struct fields per MET-25.
//
// **D-05b stale-peer GC (lifecycle.Component) lives in
// pkg/replication/peer_metrics_gc.go.** It calls DeleteLabelValues on the
// per-peer GaugeVecs after a configurable staleness threshold. Replicator
// rebinds on reconnect (Pitfall 3 mitigation per RESEARCH §Q7).
//
// **D-02d leaf-package boundary:** pkg/observability never imports
// pkg/replication. The replication code imports pkg/observability and
// satisfies its peer-label inputs with PeerConfig (whose two-field shape
// {ID, Addr} is documented here for the test suite).
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// AllowedReplicationModes is the closed enum of replication modes. Mirrors
// pkg/replication.ReplicationMode constants. `standalone` is permitted at
// the bag level but the standalone replicator never observes (see
// pkg/replication.StandaloneReplicator — zero-overhead noop).
var AllowedReplicationModes = []string{"standalone", "ha_standby", "raft", "multi_region"}

// AllowedReplicationRoles is the closed enum for the role gauge value lookup.
// The role is published as a NUMERIC enum (Gauge.Set), not a label, because
// per-role series would either explode cardinality or produce stale
// "follower=0 / leader=1" series across transitions.
var AllowedReplicationRoles = []string{"follower", "candidate", "leader", "standby"}

// modeCeiling captures the D-05a per-mode peer cardinality ceiling. Stored
// here so AssertCardinalityCeiling tests parameterize by mode.
var modeCeiling = map[string]int{
	"ha_standby":   8,
	"raft":         16,
	"multi_region": 64,
}

// RoleEnum maps a role string to a numeric gauge value. Closed mapping —
// any caller passing an unknown role gets -1 so the gauge value is
// distinguishable from a legitimate role state during debugging.
//
// Mapping per CONTEXT §domain:
//
//	-1 = unknown (defensive — never set in production)
//	 0 = follower
//	 1 = candidate
//	 2 = leader
//	 3 = standby (HA mode)
//
// The numeric values are the WIRE contract for alert rules — flipping them
// is a breaking change requiring an ADR amendment. Operators write rules
// like `nornicdb_replication_role == 2` for "is leader".
func RoleEnum(role string) float64 {
	switch role {
	case "follower":
		return 0
	case "candidate":
		return 1
	case "leader":
		return 2
	case "standby":
		return 3
	default:
		return -1
	}
}

// PeerLabel returns the stable peer label value per RISK-3 (corrected).
// The two-field PeerConfigLike interface is the leaf-package boundary
// indirection (D-02d) — pkg/replication/config.PeerConfig satisfies it via
// its actual {ID, Addr} fields.
//
// Resolution order:
//  1. ID (non-empty) — UUID-stable across restarts; preferred.
//  2. Addr (fallback) — stable across reconnects to the same configured peer.
//  3. "" (empty) — allowed but a sign of misconfiguration; tests flag.
//
// **NEVER** read from runtime sockets (`conn.RemoteAddr()`). That would
// produce a fresh label every reconnect and explode cardinality —
// exactly the RISK-3 failure mode the corrected wiring prevents.
func PeerLabel(p PeerConfigLike) string {
	if id := p.GetID(); id != "" {
		return id
	}
	return p.GetAddr()
}

// PeerConfigLike is the minimal accessor surface this package needs from
// pkg/replication.PeerConfig (D-02d boundary indirection). The replication
// package satisfies it with a tiny adapter over its concrete PeerConfig
// struct — keeps pkg/observability free of pkg/replication dependencies.
//
// We use accessor methods rather than struct fields because Go's structural
// typing matches by method set (struct field equality across packages
// requires identical declarations, which would force a shared type and
// reintroduce the import cycle).
type PeerConfigLike interface {
	GetID() string
	GetAddr() string
}

// ReplicationMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the
// Replication subsystem. One bag per Provider, constructed at cmd/nornicdb
// startup and injected into pkg/replication.Replicator implementations.
//
// **Hot-path discipline (MET-25):** the per-peer GaugeVec families are
// pre-bound at peer-tracker mark time (replicator rebinds on connect; the
// peer_metrics_gc lifecycle.Component drops bindings on staleness — Pitfall
// 3 mitigation per RESEARCH §Q7). The per-cluster scalar gauges (Role,
// Term, CommitIndex, ApplyIndex, LeaderChanges) need no Bind — they're
// already singletons.
//
// **Dual-access (D-02):** the raw *prometheus.GaugeVec / *Counter / Gauge
// is exposed so subsystem tests drive AssertCardinalityCeiling and edge
// cases. ApplyDuration uses the LatencyHistogram wrapper for MET-24
// exemplar centralization (callers Bind() once at apply chokepoint).
type ReplicationMetrics struct {
	// Role is the current role gauge. Published as a numeric enum via
	// Set(RoleEnum(roleString)) at lifecycle transition log sites (D-15a).
	// Per-cluster, no labels — there is exactly one role per process.
	Role prometheus.Gauge

	// Term is the Raft term gauge. Set at term-change log sites (D-15a).
	Term prometheus.Gauge

	// CommitIndex is the Raft last-committed-log-index gauge. Set at
	// commit-advance log sites (D-15a).
	CommitIndex prometheus.Gauge

	// ApplyIndex is the Raft last-applied-log-index gauge. Set at
	// apply-advance log sites (D-15a).
	ApplyIndex prometheus.Gauge

	// LagBytes is the per-peer replication-lag-bytes gauge. Hot-path —
	// observed at every replicate/heartbeat call. peer label is stable
	// per PeerLabel (RISK-3 fix). Mode-aware ceiling stored in modeCeiling.
	LagBytes *prometheus.GaugeVec

	// LagEntries is the per-peer replication-lag-entries gauge. Same
	// observation cadence as LagBytes.
	LagEntries *prometheus.GaugeVec

	// ApplyDuration is the histogram for command-apply latency. No labels —
	// per-cluster distribution. Pre-bound observer cached in
	// pkg/replication.Replicator struct field per MET-25.
	ApplyDuration *LatencyHistogram

	// RTTSeconds is the per-peer round-trip-time histogram. Observed at
	// AppendEntries response chokepoint. Hot-path — pre-bound per peer.
	RTTSeconds *prometheus.HistogramVec

	// LeaderChangesTotal counts every role transition that crosses the
	// leader boundary (non-leader→leader OR leader→non-leader). The
	// keystone alert metric for cluster instability — increments at the
	// SAME sites that emit "became leader" / "stepped down" log lines.
	LeaderChangesTotal prometheus.Counter

	// LastContactSeconds is the GAP-1 gauge — wall-clock seconds since the
	// last successful AppendEntries from a peer (or sent to a peer for
	// leader perspective). Operators alert on
	// `time() - nornicdb_replication_last_contact_seconds{peer="..."} > N`
	// per Phase 9 Helm/Grafana plan. Per-peer; populated at heartbeat
	// observation sites; cleared by peer_metrics_gc.
	LastContactSeconds *prometheus.GaugeVec

	// mode captures the constructed replication mode for ceiling assertions.
	mode string

	// tenantLabelsEnabled is the D-08a accepted-but-ignored flag. Stored
	// for diagnostic introspection only; never affects label arity.
	tenantLabelsEnabled bool
}

// NewReplicationMetrics constructs the replication bag against reg.
//
// Constructor signature per D-05a + D-08a:
//
//	mode (string): one of AllowedReplicationModes; stored for ceiling
//	  assertions. An unrecognized mode is permitted at construction (the
//	  bag still functions) but mode-aware ceiling lookups return 0 — tests
//	  must use a recognized mode.
//
//	tenantLabelsEnabled (bool): accepted for callsite uniformity across
//	  subsystem bags, but IGNORED — replication is per-cluster, not
//	  per-database. No `database` label is ever added to any family.
//	  Documented in CONTEXT D-08a.
//
// Validation chain inherited from pkg/observability typed constructors:
//   - subsystem "replication" must be in allowedSubsystems (metrics.go:59)
//   - histogram name must end in _seconds (registration.go validateNameSuffix)
//   - counter name must end in _total
//   - labels rejected against ForbiddenLabels (path/query/user/user_id/ip/uuid/...)
//
// Pitfall 8 / MustRegister precedent: validation failure panics — programming
// bug surfaces at startup before any traffic.
func NewReplicationMetrics(reg *prometheus.Registry, mode string, tenantLabelsEnabled bool) *ReplicationMetrics {
	bag := &ReplicationMetrics{
		mode:                mode,
		tenantLabelsEnabled: tenantLabelsEnabled,
	}

	// Per-cluster scalar gauges — no labels, no suffix-validation
	// (gauges admit any suffix per metrics.go:213 NewGaugeVec).
	bag.Role = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "replication",
		Name:      "role",
		Help: "Current replication role as numeric enum (-1=unknown, " +
			"0=follower, 1=candidate, 2=leader, 3=standby). Set at the " +
			"same lifecycle log sites that emit 'became leader' etc. " +
			"(D-15a). Per-cluster — no labels.",
	})
	reg.MustRegister(bag.Role)

	bag.Term = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "replication",
		Name:      "term",
		Help: "Current Raft term. Set at term-change log sites (D-15a). " +
			"Per-cluster — no labels.",
	})
	reg.MustRegister(bag.Term)

	bag.CommitIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "replication",
		Name:      "commit_index",
		Help: "Last committed log index. Set at commit-advance log sites " +
			"(D-15a). Per-cluster — no labels.",
	})
	reg.MustRegister(bag.CommitIndex)

	bag.ApplyIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "replication",
		Name:      "apply_index",
		Help: "Last applied log index. Set at apply-advance log sites " +
			"(D-15a). Per-cluster — no labels.",
	})
	reg.MustRegister(bag.ApplyIndex)

	bag.LeaderChangesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nornicdb",
		Subsystem: "replication",
		Name:      "leader_changes_total",
		Help: "Total leader-boundary transitions (non-leader→leader OR " +
			"leader→non-leader). Keystone alert metric for cluster " +
			"instability. Increments at lifecycle log sites (D-15a).",
	})
	reg.MustRegister(bag.LeaderChangesTotal)

	// Per-peer gauge families — peer label is stable per PeerLabel (RISK-3
	// fix). Mode-aware cardinality ceiling enforced by the test suite via
	// AssertCardinalityCeiling(t, name, modeCeiling[mode], drive).
	bag.LagBytes = NewGaugeVec(reg,
		MetricOpts{
			Subsystem: "replication",
			Name:      "lag_bytes",
			Help: "Per-peer replication lag in bytes. peer label sources " +
				"from PeerConfig.ID (RISK-3 fix); fallback to PeerConfig.Addr. " +
				"Mode-aware cardinality ceiling: ha_standby=8, raft=16, " +
				"multi_region=64 (D-05a). Stale peers GC'd by peer_metrics_gc " +
				"lifecycle.Component (D-05b).",
		},
		[]string{"peer"})

	bag.LagEntries = NewGaugeVec(reg,
		MetricOpts{
			Subsystem: "replication",
			Name:      "lag_entries",
			Help: "Per-peer replication lag in log entries. Same peer-label " +
				"discipline as lag_bytes. Mode-aware ceiling per D-05a.",
		},
		[]string{"peer"})

	bag.LastContactSeconds = NewGaugeVec(reg,
		MetricOpts{
			Subsystem: "replication",
			Name:      "last_contact_seconds",
			Help: "GAP-1 keystone — wall-clock seconds since the last " +
				"successful AppendEntries with each peer. Operators alert " +
				"on `time() - nornicdb_replication_last_contact_seconds{...} " +
				"> N` per Phase 9 Helm/Grafana plan. Per-peer; mode-aware " +
				"ceiling per D-05a.",
		},
		[]string{"peer"})

	// ApplyDuration uses the LatencyHistogram wrapper for MET-24 exemplar
	// emission centralization. Hot-path: callers Bind() once at apply
	// chokepoint and cache the BoundLatencyObserver.
	bag.ApplyDuration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "replication",
			Name:      "apply_duration_seconds",
			Help: "Per-command apply latency at the replicator chokepoint. " +
				"Phase-3-locked LatencyBucketsSeconds (≤10s tail). Hot-path: " +
				"single Bound observer cached in pkg/replication.Replicator " +
				"struct field per MET-25. No labels — per-cluster distribution.",
		},
		nil)

	// RTTSeconds is per-peer; uses raw HistogramVec for direct hot-path
	// WithLabelValues binding. RTT exemplars deferred until Phase 8 (TRC-20
	// replication spans pin them).
	bag.RTTSeconds = NewLatencyHistogramVec(reg,
		MetricOpts{
			Subsystem: "replication",
			Name:      "rtt_seconds",
			Help: "Per-peer round-trip time observed at AppendEntries " +
				"response chokepoint. Same peer-label discipline as " +
				"lag_bytes. Mode-aware ceiling per D-05a.",
		},
		[]string{"peer"})

	return bag
}

// Mode returns the replication mode this bag was constructed for.
// Used by mode-aware cardinality ceiling assertions.
func (r *ReplicationMetrics) Mode() string { return r.mode }

// Ceiling returns the mode-aware peer cardinality ceiling per D-05a.
// Returns 0 for unknown modes — the caller should treat that as a test bug.
func (r *ReplicationMetrics) Ceiling() int { return modeCeiling[r.mode] }

// TenantLabelsEnabled reports the D-08a flag value. Diagnostic only —
// no replication family is tenant-labeled.
func (r *ReplicationMetrics) TenantLabelsEnabled() bool { return r.tenantLabelsEnabled }
