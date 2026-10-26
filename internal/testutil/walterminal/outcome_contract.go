// Author: Codex,NOR-180 consumer-class regressions,29October2026; the
// driver retry-decision oracle, and the generated composition population with
// per-child WAL and an oracle from native truth, by Claude, 30 October 2026.
package walterminal

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ContractChild is one independent child of a composition: its outcome kind
// and whether its own logical WAL fails at its finish.
type ContractChild struct {
	Kind   string
	Closed bool
}

type ContractProfile struct {
	Name     string
	Children []ContractChild
}

// ContractKinds are the child outcomes a composition is built from: a commit,
// a commit whose post-commit step fails transiently, and two aborts.
var ContractKinds = []string{"commit", "commit_postcommit", "abort_memory", "abort_timeout"}

// ContractProfiles is the complete population of two-child compositions:
// every ordered pair of child kind × that child's own WAL (writable, or failed
// at its finish). It is generated, so no composition depends on a listed case.
var ContractProfiles = func() []ContractProfile {
	var children []ContractChild
	for _, kind := range ContractKinds {
		for _, closed := range []bool{false, true} {
			children = append(children, ContractChild{kind, closed})
		}
	}
	var profiles []ContractProfile
	for _, first := range children {
		for _, second := range children {
			profiles = append(profiles, ContractProfile{Name: first.name() + "+" + second.name(), Children: []ContractChild{first, second}})
		}
	}
	return profiles
}()

func (c ContractChild) name() string {
	return fmt.Sprintf("%s.%s", c.Kind, map[bool]string{false: "wal_ok", true: "wal_failed"}[c.Closed])
}

// committed is native truth: the child's work is stored.
func (c ContractChild) committed() bool { return c.Kind == "commit" || c.Kind == "commit_postcommit" }

// reaches reports whether the child's outcome survives a count-only consumer,
// which discards every child return: only terminal outcomes do (a failed WAL
// record, and a committed child's failure).
func (c ContractChild) reaches() bool { return c.Closed || c.Kind == "commit_postcommit" }

// DriverRetries is the managed-client retry decision for a failure with code,
// taken from the pinned neo4j-go-driver itself (ExecuteWrite retries exactly
// the errors for which Neo4jError.IsRetriable holds).
func DriverRetries(code string) bool { return (&db.Neo4jError{Code: code}).IsRetriable() }

// OutcomeStatus is the overall status a client may receive for an outcome
// whose child statuses are codes: a committed outcome never asks a driver to
// retry, so a retriable child status becomes TransactionCommitFailed.
func OutcomeStatus(code string, committed bool) string {
	if committed && DriverRetries(code) {
		return nornicerrors.TransactionCommitFailed
	}
	return code
}

type OutcomeContract struct {
	Flow      *OutcomeFlow
	Engines   []*Engine
	Primaries []error // per child, aligned with Profile.Children
	Profile   ContractProfile
	Parallel  bool
}

func NewOutcomeContract(t testing.TB, profile ContractProfile, parallel bool) *OutcomeContract {
	t.Helper()
	c := &OutcomeContract{Profile: profile, Parallel: parallel}
	kinds, closed := make([]string, len(profile.Children)), make([]bool, len(profile.Children))
	for i, child := range profile.Children {
		c.Engines = append(c.Engines, New(t))
		kinds[i], closed[i] = child.Kind, child.Closed
	}
	c.Flow = NewMultipleOutcomeFlow(t, c.Engines, MultipleOutcomeOptions{Parallel: parallel, Kinds: kinds, Closed: closed, ObservePrimary: func(primary error) { c.Primaries = append(c.Primaries, primary) }})
	return c
}

// Committed is the composition's native truth, independent of which child
// outcomes reach the client.
func (c *OutcomeContract) Committed() bool {
	for _, child := range c.Profile.Children {
		if child.committed() {
			return true
		}
	}
	return false
}

// WALFailed reports whether any child outcome reaching the client failed its WAL.
func (c *OutcomeContract) WALFailed() bool {
	for _, child := range c.Profile.Children {
		if child.Closed {
			return true
		}
	}
	return false
}

func (c *OutcomeContract) RequireNative(t testing.TB) {
	t.Helper()
	for i, engine := range c.Engines {
		want := int64(0)
		if c.Profile.Children[i].committed() {
			want = 1
		}
		require.Equal(t, want, Count(t, engine), "native child%d", i)
	}
}

func ContractStatus(kind string) (string, string) {
	if kind == "commit" {
		return nornicerrors.TransactionCommitFailed, ""
	}
	if kind == "rollback" {
		return nornicerrors.TransactionRollbackFailed, ""
	}
	if kind == "commit_postcommit" {
		return nornicerrors.Neo4jStatus(storage.ErrStorageRestoring)
	}
	return nornicerrors.Neo4jStatus(Primary(strings.TrimPrefix(kind, "abort_")))
}

func (c *OutcomeContract) RequireIdentity(t testing.TB, err error) {
	t.Helper()
	observed := c.Flow.Observed()
	require.Len(t, observed, len(c.Profile.Children))
	failedWAL, wantFailedWAL := 0, 0
	for i, child := range c.Profile.Children {
		if child.Closed {
			wantFailedWAL++
		}
		if child.reaches() && c.Primaries[i] != nil {
			require.ErrorIs(t, err, c.Primaries[i], "each reaching child's original primary identity")
		}
	}
	for _, child := range observed {
		if errors.Is(child, storage.ErrWALClosed) {
			failedWAL++
		}
	}
	require.Equal(t, wantFailedWAL, failedWAL, "each child's own WAL outcome")
	if c.WALFailed() {
		require.ErrorIs(t, err, storage.ErrWALClosed)
	}
}

func (c *OutcomeContract) RequireView(t testing.TB, code, message string) {
	t.Helper()
	codes := map[string]int{}
	messages := map[string]int{}
	commits, aborts, first := 0, 0, ""
	for _, child := range c.Profile.Children {
		if !child.reaches() {
			continue
		}
		ownCode, ownMessage := ContractStatus(child.Kind)
		codes[ownCode]++
		if ownMessage != "" {
			messages[strings.TrimSpace(ownMessage)]++
		}
		if first == "" {
			first = ownCode
		}
		if child.Closed {
			if child.committed() {
				commits++
			} else {
				aborts++
			}
		}
	}
	committed := c.Committed()
	if code != "" {
		// The client's retry decision, from native truth: stored work is never retried.
		require.False(t, committed && DriverRetries(code), "a driver would retry committed work on %s", code)
		if c.Parallel {
			found := false
			for own := range codes {
				found = found || OutcomeStatus(own, committed) == code
			}
			require.True(t, found, "primary classification belongs to an actual child: %s", code)
		} else {
			require.Equal(t, OutcomeStatus(first, committed), code)
		}
	}
	// A single protocol status cannot represent several distinct child statuses.
	// Every reaching child's classification/message must remain in the rendering.
	for value, count := range codes {
		require.GreaterOrEqual(t, strings.Count(message, value), count, "each child's own classification")
	}
	for value, count := range messages {
		require.GreaterOrEqual(t, strings.Count(message, value), count, "each child's own canonical primary message")
	}
	require.Equal(t, aborts, strings.Count(message, "failed to write WAL tx abort"))
	require.Equal(t, commits, strings.Count(message, "committed, but failed to write WAL tx commit"))
	if aborts+commits != 0 {
		require.Contains(t, message, storage.ErrWALClosed.Error())
	}
}
