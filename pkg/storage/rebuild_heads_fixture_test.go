package storage

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
