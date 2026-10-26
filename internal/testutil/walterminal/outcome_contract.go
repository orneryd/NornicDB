// Author: Codex,NOR-180 consumer-class regressions,29October2026.
package walterminal

import (
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
		if c.Profile.Kinds[i] == "commit" {
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
	return nornicerrors.Neo4jStatus(Primary(strings.TrimPrefix(kind, "abort_")))
}
func (c *OutcomeContract) RequireIdentity(t testing.TB, err error) {
	t.Helper()
	require.ErrorIs(t, err, storage.ErrWALClosed)
	observed := c.Flow.Observed()
	require.Len(t, observed, len(c.Profile.Kinds))
	for _, child := range observed {
		require.ErrorIs(t, child, storage.ErrWALClosed)
	}
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
	commits, aborts := 0, 0
	for _, kind := range c.Profile.Kinds {
		ownCode, ownMessage := ContractStatus(kind)
		codes[ownCode]++
		if ownMessage != "" {
			messages[strings.TrimSpace(ownMessage)]++
		}
		if kind == "commit" {
			commits++
		} else {
			aborts++
		}
	}
	if code != "" {
		if c.Parallel {
			_, found := codes[code]
			require.True(t, found, "primary classification belongs to an actual child: %s", code)
		} else {
			want, _ := ContractStatus(c.Profile.Kinds[0])
			require.Equal(t, want, code)
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
	require.Contains(t, message, storage.ErrWALClosed.Error())
}
