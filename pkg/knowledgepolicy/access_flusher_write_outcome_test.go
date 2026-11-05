package knowledgepolicy

import (
	"context"

	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAccessFlusherCallbacksFollowAcceptedWrite(t *testing.T) {
	failure := errors.New("actual storage boundary failure")
	for _, tc := range []struct {
		name                      string
		lookupErr, writeErr       error
		skip                      bool
		wantWrites, wantCallbacks int
		wantErr                   error
	}{
		{name: "healthy", wantWrites: 1, wantCallbacks: 1},
		{name: "actual retirement", lookupErr: ErrEntityMetaNotFound},
		{name: "lookup fault", lookupErr: failure, wantErr: failure},
		{name: "write fault", writeErr: failure, wantErr: failure},
		{name: "atomic final eligibility skip", skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &scriptedAccessMetaStore{writeErr: tc.writeErr, skipWrite: tc.skip}
			acc := NewAccessAccumulator(true, 0)
			f := NewAccessFlusher(acc, store, time.Hour)
			invalidates, rechecks := 0, 0
			f.SetPropertySuppression(nil, &mockEntityMeta{err: tc.lookupErr}, func(string) { invalidates++ })
			f.SetSuppressionRecheck(func(context.Context, string, EntityMeta) { rechecks++ })
			acc.IncrementAccess("test:n")
			f.Flush()
			require.Len(t, store.writes, tc.wantWrites)
			require.Equal(t, tc.wantCallbacks, rechecks)
			require.Zero(t, invalidates, "no property transition was requested")
			for range 2 {
				err := f.Stop()
				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestAccessFlusherEmbeddingInvalidationRequiresAcceptedSuppression(t *testing.T) {
	failure := errors.New("metadata persistence refused")
	for _, tc := range []struct {
		name     string
		writeErr error
		skip     bool
		accepted bool
	}{
		{name: "accepted", accepted: true}, {name: "failed", writeErr: failure}, {name: "deleted at final write", skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := &DecayProfileBundle{Name: "profile", Scope: ScopeNode, Function: DecayFunctionExponential, HalfLifeSeconds: 3600, VisibilityThreshold: 0.1, ScoreFrom: ScoreFromCreated, Enabled: true, DecayEnabled: true}
			binding := &DecayProfileBinding{Name: "binding", ProfileRef: "profile", TargetLabels: []string{"Doc"}, PropertyRules: []DecayProfilePropertyRule{{PropertyPath: "bio", HalfLifeSeconds: 1800}}}
			fixture := newAccessFlusherFixture(t, bundle, binding, nil, &mockEntityMeta{labels: []string{"Doc"}, propKeys: []string{"bio"}, createdAt: time.Now().Add(-720 * time.Hour).UnixNano()})
			store := &scriptedAccessMetaStore{writeErr: tc.writeErr, skipWrite: tc.skip}
			fixture.flusher.store = store
			fixture.install()
			fixture.flushAccesses("entity")
			if tc.accepted {
				require.Equal(t, []string{"testns:entity"}, fixture.invalidated)
				require.NotNil(t, store.writes["testns:entity"])
				_, present := store.writes["testns:entity"].Overflow["_suppress:bio"]
				require.True(t, present)
			} else {
				require.Empty(t, fixture.invalidated)
				require.Empty(t, store.writes)
			}
			err := fixture.flusher.Stop()
			if tc.writeErr != nil {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
