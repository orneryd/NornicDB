package storage

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// PrepareMergeNode is the native MERGE preparation path a statement reaches
// with its own request context. Its first schema read is constraint discovery
// (uniqueMergeKeys -> GetConstraintsForLabels). When another goroutine holds
// the schema write lock, that discovery must observe the request: a canceled
// request leaves with context.Canceled and admits no key, instead of waiting
// for the holder and preparing the MERGE on its behalf.
//
// The admission observer pins the wait to the real schema mutex and to the
// discovery frame, and records the context the discovery passed to admission,
// so this proof covers the actual discovery call rather than an earlier one.
func TestEvidenceMergePreparationDiscoveryCancelsWhileSchemaWriterHeld(t *testing.T) {
	engine := createTestBadgerEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	constraint := Constraint{Name: "tuple", Type: ConstraintNodeKey, Label: "Tuple", Properties: []string{"tenant", "id"}}
	require.NoError(t, schema.AddConstraint(constraint))
	// A filled owned index makes the healthy retry admit the tuple's key, so
	// "no key was admitted" is a real observation on both paths.
	idx, exists := schema.ConstraintPropertyIndex(constraint.Name)
	require.True(t, exists)
	require.NoError(t, schema.BackfillCompositeIndex(t.Context(), idx.Name, nil))
	other := engine.GetSchemaForNamespace("other")
	require.NoError(t, other.AddUniqueConstraint("doc_uid", "Doc", "uid"))

	tx := beginMergeKeyTx(t, engine)
	t.Cleanup(func() { require.NoError(t, tx.Rollback()) })

	properties := map[string]interface{}{"tenant": int64(7), "id": int64(42)}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waited := make(chan context.Context, 1)
	restore := setAdmissionWaitObserver(func(owner any, observed context.Context) {
		if owner != &schema.mu {
			return
		}
		pcs := make([]uintptr, 24)
		frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, ".GetConstraintsForLabels") {
				select {
				case waited <- observed:
				default:
				}
				return
			}
			if !more {
				return
			}
		}
	})
	t.Cleanup(restore)

	schema.mu.Lock()
	var release sync.Once
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		release.Do(schema.mu.Unlock)
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("MERGE preparation did not join after schema release")
		}
	})
	go func() {
		defer close(joined)
		done <- tx.PrepareMergeNode(ctx, []string{"Tuple"}, properties)
	}()

	var admission context.Context
	select {
	case admission = <-waited:
	case <-time.After(5 * time.Second):
		release.Do(schema.mu.Unlock)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("MERGE preparation did not join after releasing the schema")
		}
		t.Fatal("MERGE preparation never reached constraint discovery behind the schema writer")
	}
	require.True(t, admission == ctx, "constraint discovery dropped the statement's request context")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Error("canceled MERGE preparation stayed blocked in constraint discovery behind the schema writer")
		release.Do(schema.mu.Unlock)
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled,
				"the discovery answered a canceled request instead of reporting its cancellation")
		case <-time.After(5 * time.Second):
			t.Fatal("MERGE preparation did not join after schema release")
		}
	}
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("MERGE preparation worker did not join after returning its result")
	}
	require.Empty(t, tx.mergeKeyReleases, "a canceled preparation admitted unique keys")

	release.Do(schema.mu.Unlock)
	// A canceled request must not poison the healthy route or another
	// namespace's discovery.
	require.NoError(t, tx.PrepareMergeNode(t.Context(), []string{"Tuple"}, properties))
	require.Len(t, tx.mergeKeyReleases, 1)
	constraints, err := other.GetConstraintsForLabels(t.Context(), []string{"Doc"})
	require.NoError(t, err)
	require.Len(t, constraints, 1)
	require.Equal(t, "doc_uid", constraints[0].Name)
}
