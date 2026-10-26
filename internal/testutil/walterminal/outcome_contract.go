// Author: Codex,NOR-180 consumer-class regressions,29October2026; the
// driver retry-decision oracle, memory_commit order and the per-child WAL
// (commit_postcommit) profiles by Claude, 30 October 2026.
package walterminal

import (
	"errors"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

type ContractProfile struct {
	Name  string
	Kinds []string
}

var ContractProfiles = []ContractProfile{
	{"timeout_memory", []string{"abort_timeout", "abort_memory"}},
	{"memory_timeout", []string{"abort_memory", "abort_timeout"}},
	{"commit_memory", []string{"commit", "abort_memory"}},
	{"memory_memory", []string{"abort_memory", "abort_memory"}},
	// The reverse of commit_memory: a retriable aborted child comes first.
	{"memory_commit", []string{"abort_memory", "commit"}},
	// A committed child whose own WAL stays writable (its COMMIT record is
	// written, its post-commit step fails transiently) beside a retriable
	// aborted child whose WAL fails or not, in both orders.
	{"memory_postcommit", []string{"abort_memory", "commit_postcommit"}},
	{"postcommit_memory", []string{"commit_postcommit", "abort_memory"}},
}

// committedKind reports a child whose native work committed.
func committedKind(kind string) bool { return kind == "commit" || kind == "commit_postcommit" }

// reaches reports whether a child's outcome reaches the client. A count-only
// consumer discards its children's returns; only terminal outcomes survive
// (every child's failed WAL record, and a committed child's failure always).
func (c *OutcomeContract) reaches(kind string) bool { return c.Closed || kind == "commit_postcommit" }

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
	Flow             *OutcomeFlow
	Engines          []*Engine
	Primaries        []error
	Profile          ContractProfile
	Closed, Parallel bool
}

func NewOutcomeContract(t testing.TB, profile ContractProfile, closed, parallel bool) *OutcomeContract {
	t.Helper()
	c := &OutcomeContract{Profile: profile, Closed: closed, Parallel: parallel}
	for range profile.Kinds {
		c.Engines = append(c.Engines, New(t))
	}
	c.Flow = NewMultipleOutcomeFlow(t, c.Engines, MultipleOutcomeOptions{Parallel: parallel, HealthyWAL: !closed, Kinds: profile.Kinds, ObservePrimary: func(primary error) { c.Primaries = append(c.Primaries, primary) }})
	return c
}
func (c *OutcomeContract) RequireNative(t testing.TB) {
	t.Helper()
	for i, engine := range c.Engines {
		want := int64(0)
		if committedKind(c.Profile.Kinds[i]) {
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
	require.Len(t, observed, len(c.Profile.Kinds))
	for _, child := range observed {
		if errors.Is(child, storage.ErrStorageRestoring) {
			require.NotErrorIs(t, child, storage.ErrWALClosed, "the committed child's own WAL record was written")
			continue
		}
		if c.Closed {
			require.ErrorIs(t, child, storage.ErrWALClosed)
		}
	}
	for _, kind := range c.Profile.Kinds {
		if kind == "commit_postcommit" {
			require.ErrorIs(t, err, storage.ErrStorageRestoring, "a committed child's discarded failure is retained")
		}
	}
	if !c.Closed {
		return
	}
	require.ErrorIs(t, err, storage.ErrWALClosed)
	for _, primary := range c.Primaries {
		if primary != nil {
			require.ErrorIs(t, err, primary, "each directly captured original primary identity")
		}
	}
}
func (c *OutcomeContract) RequireView(t testing.TB, code, message string) {
	t.Helper()
	codes := map[string]int{}
	messages := map[string]int{}
	commits, aborts, committed, first := 0, 0, false, ""
	for _, kind := range c.Profile.Kinds {
		if !c.reaches(kind) {
			continue
		}
		ownCode, ownMessage := ContractStatus(kind)
		codes[ownCode]++
		if ownMessage != "" {
			messages[strings.TrimSpace(ownMessage)]++
		}
		if first == "" {
			first = ownCode
		}
		committed = committed || committedKind(kind)
		switch kind {
		case "commit":
			commits++
		case "commit_postcommit":
		default:
			aborts++
		}
	}
	if code != "" {
		// The client's retry decision: work that is stored is never retried.
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
	// Every child's classification/message must remain in the shared rendering.
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
