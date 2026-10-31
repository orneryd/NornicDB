// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Attempt struct {
	Author         string        `json:"author"`
	State          string        `json:"state"`
	Commit         string        `json:"commit"`
	Key            string        `json:"key"`
	Sections       string        `json:"requested_sections"`
	Section        string        `json:"active_section"`
	Phase          string        `json:"phase"`
	Reason         string        `json:"reason"`
	SupervisorPID  int           `json:"supervisor_pid"`
	WorkerPID      int           `json:"worker_pid"`
	WorkDir        string        `json:"work_dir"`
	Started        string        `json:"started_utc"`
	Updated        string        `json:"updated_utc"`
	ToolSHA256     string        `json:"tool_binary_sha256"`
	Events         []StageEvent  `json:"stage_events,omitempty"`
	Metrics        []StageMetric `json:"stage_metrics,omitempty"`
	LimitHit       string        `json:"limit_hit"`
	ObservedRSSKiB int64         `json:"observed_rss_kib,omitempty"`
	RSSLimitKiB    int64         `json:"rss_limit_kib,omitempty"`
}

type rssLimitError struct{ ObservedKiB, LimitKiB int64 }

var failureRecorded bool
var failureOutputRoot = "/work/sig/divergence/results"
var requestSignals <-chan os.Signal

type requestCancelled struct {
	Phase  string
	Signal os.Signal
}

func (e *requestCancelled) Error() string {
	return fmt.Sprintf("request received %s during %s", signalName(e.Signal), e.Phase)
}
func cancelled(phase string) error {
	select {
	case sig := <-requestSignals:
		return &requestCancelled{phase, sig}
	default:
		return nil
	}
}

func preflightFailure(reason error) (string, error) {
	dir := filepath.Join(failureOutputRoot, "preflight", digest(strings.Join(os.Args[1:], "\x00")))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, fmt.Sprintf("%d-%d.json", time.Now().UnixNano(), os.Getpid()))
	a := Attempt{Author: author, State: "failed", Section: "preflight", Phase: "request_validation_or_delivery", Reason: reason.Error(), SupervisorPID: os.Getpid(), Updated: time.Now().UTC().Format(time.RFC3339Nano), LimitHit: "none"}
	var interruption *requestCancelled
	if errors.As(reason, &interruption) {
		a.Section = "not_started"
		a.Phase = interruption.Phase
	}
	return path, atomicJSON(path, a)
}

func (e *rssLimitError) Error() string {
	return fmt.Sprintf("analysis exceeded RSS limit: observed %d KiB, limit %d KiB; no result cached", e.ObservedKiB, e.LimitKiB)
}
func markAttemptFailed(a *Attempt, err error) {
	a.State = "failed"
	a.Reason = attemptReason(err)
	a.LimitHit = "none"
	var limit *rssLimitError
	if errors.As(err, &limit) {
		a.LimitHit = "rss"
		a.ObservedRSSKiB = limit.ObservedKiB
		a.RSSLimitKiB = limit.LimitKiB
	}
}

type StageEvent struct {
	Phase         string `json:"phase"`
	AtNano        int64  `json:"at_unix_nano"`
	RSSKiB        int64  `json:"rss_kib"`
	HeapAlloc     uint64 `json:"heap_alloc_bytes"`
	KernelPeakKiB int64  `json:"cumulative_kernel_peak_rss_kib"`
}
type StageMetric struct {
	Phase                   string  `json:"phase"`
	Seconds                 float64 `json:"seconds"`
	SampledPeakRSSKiB       int64   `json:"sampled_peak_rss_kib"`
	CumulativeKernelPeakKiB int64   `json:"cumulative_kernel_peak_rss_kib"`
}
type memorySample struct{ AtNano, RSSKiB int64 }

func processRSS(pid int) int64 {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			p := strings.Fields(line)
			if len(p) >= 2 {
				n, _ := strconv.ParseInt(p[1], 10, 64)
				return n
			}
		}
	}
	return 0
}
func stageMetrics(events []StageEvent, samples []memorySample, end int64) []StageMetric {
	out := []StageMetric{}
	for i, e := range events {
		if e.Phase == "analysis_complete" {
			continue
		}
		stop := end
		peak := e.RSSKiB
		kernel := e.KernelPeakKiB
		if i+1 < len(events) {
			stop = events[i+1].AtNano
			peak = max(peak, events[i+1].RSSKiB)
			kernel = max(kernel, events[i+1].KernelPeakKiB)
		}
		for _, s := range samples {
			if s.AtNano >= e.AtNano && s.AtNano <= stop {
				peak = max(peak, s.RSSKiB)
			}
		}
		out = append(out, StageMetric{e.Phase, max(0, float64(stop-e.AtNano)/1e9), peak, kernel})
	}
	return out
}

func atomicJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".receipt-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_, err = f.Write(append(data, '\n'))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, path)
}
func readAttempt(path string) (Attempt, error) {
	var a Attempt
	data, err := os.ReadFile(path)
	if err != nil {
		return a, err
	}
	err = json.Unmarshal(data, &a)
	return a, err
}

func setStage(phase string) {
	path := os.Getenv("DIVERGENCE_ATTEMPT")
	if path == "" {
		return
	}
	a, err := readAttempt(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "divergence: cannot update attempt:", err)
		return
	}
	a.Phase = phase
	for _, section := range sectionNames {
		if phase == section {
			a.Section = section
		}
	}
	a.WorkerPID = os.Getpid()
	a.Updated = time.Now().UTC().Format(time.RFC3339Nano)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	a.Events = append(a.Events, StageEvent{phase, time.Now().UnixNano(), processRSS(os.Getpid()), mem.HeapAlloc, usage.Maxrss})
	if err = atomicJSON(path, a); err != nil {
		fmt.Fprintln(os.Stderr, "divergence: cannot update attempt:", err)
	}
}

func attemptReason(err error) string {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return fmt.Sprintf("worker received %s; signal sender unknown (not an automatic RSS-guard decision)", signalName(status.Signal()))
		}
	}
	return err.Error()
}

func signalName(sig os.Signal) string {
	switch sig {
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGINT:
		return "SIGINT"
	}
	return strings.ToUpper(sig.String())
}

func recoverAttempts(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		a, err := readAttempt(path)
		if err != nil || a.State != "running" {
			continue
		}
		if syscall.Kill(a.SupervisorPID, 0) != syscall.ESRCH {
			continue
		}
		// An uncatchable supervisor kill leaves a durable journal. Cancel only its
		// identified orphan worker; inherited slot FD preserves the global limit.
		if a.WorkerPID > 0 {
			data, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", a.WorkerPID))
			if strings.Contains(string(data), a.WorkDir+"/options.json") {
				_ = syscall.Kill(a.WorkerPID, syscall.SIGTERM)
			}
		}
		a.State = "failed"
		a.LimitHit = "unknown"
		a.Reason = "supervisor disappeared before completion; uncatchable kill or container restart, exact cause unavailable"
		a.Updated = time.Now().UTC().Format(time.RFC3339Nano)
		if err = atomicJSON(path, a); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "divergence: recovered failure receipt", path)
	}
	return nil
}
