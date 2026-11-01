package storage

// Author of fixture cleanup changes: L4 implementation lane (Claude and Codex), 1 November 2026.

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"testing"
)

func newRebuildHeadsFixture(t *testing.T, retainedVersions int) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		InMemory:      true,
		EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: retainedVersions}},
	}))
	require.NoError(t, err)
	return engine
}
