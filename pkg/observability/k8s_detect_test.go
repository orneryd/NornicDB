package observability

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeFI is a minimal os.FileInfo stub used by table-driven autodetect tests.
type fakeFI struct{ size int64 }

func (f fakeFI) Name() string       { return "" }
func (f fakeFI) Size() int64        { return f.size }
func (f fakeFI) Mode() os.FileMode  { return 0 }
func (f fakeFI) ModTime() time.Time { return time.Time{} }
func (f fakeFI) IsDir() bool        { return false }
func (f fakeFI) Sys() any           { return nil }

// TestDetectK8s_Signals exercises the AND-signal autodetect matrix per D-02.
// RED in Wave-0 (Detect returns (false, "")); Plan 05-03 turns GREEN.
func TestDetectK8s_Signals(t *testing.T) {
	someErr := errors.New("permission denied")
	cases := []struct {
		name        string
		envValue    string
		statSize    int64
		statErr     error
		wantEnabled bool
		wantReason  string
	}{
		{name: "env_present_token_present", envValue: "10.0.0.1", statSize: 1024, statErr: nil, wantEnabled: true, wantReason: ReasonK8sDetected},
		{name: "env_absent", envValue: "", statSize: 0, statErr: nil, wantEnabled: false, wantReason: ReasonServiceHostAbsent},
		{name: "env_whitespace_only", envValue: "   ", statSize: 0, statErr: nil, wantEnabled: false, wantReason: ReasonServiceHostAbsent},
		{name: "env_present_token_absent", envValue: "10.0.0.1", statSize: 0, statErr: os.ErrNotExist, wantEnabled: false, wantReason: ReasonTokenFileAbsent},
		{name: "env_present_token_empty", envValue: "10.0.0.1", statSize: 0, statErr: nil, wantEnabled: false, wantReason: ReasonTokenFileEmpty},
		{name: "env_present_token_stat_error", envValue: "10.0.0.1", statSize: 0, statErr: someErr, wantEnabled: false, wantReason: ReasonTokenStatError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := k8sProbe{
				Getenv: func(string) string { return tc.envValue },
				StatFile: func(string) (os.FileInfo, error) {
					if tc.statErr != nil {
						return nil, tc.statErr
					}
					return fakeFI{size: tc.statSize}, nil
				},
			}
			gotEnabled, gotReason := probe.Detect()
			assert.Equal(t, tc.wantEnabled, gotEnabled)
			assert.Equal(t, tc.wantReason, gotReason)
		})
	}
}

// TestResolveTenantLabels_Precedence exercises D-02a explicit > autodetect > default.
// RED in Wave-0 (ResolveTenantLabels returns (false, "")); Plan 05-03 GREEN.
func TestResolveTenantLabels_Precedence(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }
	cases := []struct {
		name        string
		explicit    *bool
		probeOut    bool
		probeReason string
		wantBool    bool
		wantSource  string
	}{
		{name: "explicit_true_wins_over_autodetect_false", explicit: boolPtr(true), probeOut: false, probeReason: ReasonServiceHostAbsent, wantBool: true, wantSource: ReasonExplicitYAML},
		{name: "explicit_false_wins_over_autodetect_true", explicit: boolPtr(false), probeOut: true, probeReason: ReasonK8sDetected, wantBool: false, wantSource: ReasonExplicitYAML},
		{name: "autodetect_true_when_no_explicit", explicit: nil, probeOut: true, probeReason: ReasonK8sDetected, wantBool: true, wantSource: ReasonK8sDetected},
		{name: "default_false_when_no_explicit_no_k8s", explicit: nil, probeOut: false, probeReason: ReasonServiceHostAbsent, wantBool: false, wantSource: ReasonServiceHostAbsent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Construct a probe whose Detect returns the desired pair.
			// Easy via stubbed Getenv/StatFile that ResolveTenantLabels
			// calls into. Plan 05-03 may use a thin probeStub seam if
			// cleaner — the contract is the (resolved, source) tuple.
			probe := stubProbe(tc.probeOut, tc.probeReason)
			gotBool, gotSource := ResolveTenantLabels(tc.explicit, probe)
			assert.Equal(t, tc.wantBool, gotBool)
			assert.Equal(t, tc.wantSource, gotSource)
		})
	}
}

// stubProbe builds a k8sProbe whose Detect returns (enabled, reason).
// For enabled=true, both signals are present. For enabled=false, the probe
// is wired to produce the specific failure reason so future test rows that
// exercise non-ReasonServiceHostAbsent disabled paths yield the correct result.
func stubProbe(enabled bool, reason string) k8sProbe {
	if enabled {
		return k8sProbe{
			Getenv:   func(string) string { return "10.0.0.1" },
			StatFile: func(string) (os.FileInfo, error) { return fakeFI{size: 1024}, nil },
		}
	}
	switch reason {
	case ReasonTokenFileAbsent:
		return k8sProbe{
			Getenv:   func(string) string { return "10.0.0.1" },
			StatFile: func(string) (os.FileInfo, error) { return nil, os.ErrNotExist },
		}
	case ReasonTokenFileEmpty:
		return k8sProbe{
			Getenv:   func(string) string { return "10.0.0.1" },
			StatFile: func(string) (os.FileInfo, error) { return fakeFI{size: 0}, nil },
		}
	case ReasonTokenStatError:
		return k8sProbe{
			Getenv:   func(string) string { return "10.0.0.1" },
			StatFile: func(string) (os.FileInfo, error) { return nil, errors.New("permission denied") },
		}
	default: // ReasonServiceHostAbsent or unknown
		return k8sProbe{
			Getenv:   func(string) string { return "" },
			StatFile: func(string) (os.FileInfo, error) { return nil, os.ErrNotExist },
		}
	}
}

// TestK8sProbe_DefaultProbeIsLive verifies DefaultK8sProbe wires real os
// reads. RED in Wave-0 (returns zero-value); Plan 05-03 GREEN.
func TestK8sProbe_DefaultProbeIsLive(t *testing.T) {
	probe := DefaultK8sProbe()
	require.NotNil(t, probe.Getenv, "DefaultK8sProbe must wire os.Getenv")
	require.NotNil(t, probe.StatFile, "DefaultK8sProbe must wire os.Stat")
}
