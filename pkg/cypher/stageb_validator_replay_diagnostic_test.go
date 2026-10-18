package cypher

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This diagnostic chooses phase 128. The phase, latch state and cancellation
// timestamp in the retained 77e7 timeout were not recovered. This does not
// replace that fixture, a cold-parser test, or an actual deadline measurement.
func TestStageBValidatorReplayDiagnostic(t *testing.T) {
	query := "RETURN 1 +" + strings.Repeat(" ", 129) + "*"
	require.Len(t, query, 140)
	require.Equal(t, "015be2ab13faecf076c01ae3e1023a1765c200d51ed28c2319f535fd61f16faa", fmt.Sprintf("%x", sha256.Sum256([]byte(query))))
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "stageb_validator_replay")
	for _, reader := range []struct {
		name string
		stop int
		run  func(*StorageExecutor, string, *pipelineCancellation) error
	}{
		{"nornic", 380, func(e *StorageExecutor, q string, p *pipelineCancellation) error {
			if p == nil {
				return e.validateSyntaxNornic(q)
			}
			return e.validateSyntaxNornic(q)
		}},
		{"antlr", 449, func(e *StorageExecutor, q string, p *pipelineCancellation) error {
			if p == nil {
				return e.validateSyntaxANTLR(q, false)
			}
			return e.validateSyntaxANTLRWithCancellation(q, false, p)
		}},
	} {
		t.Run(reader.name, func(t *testing.T) {
			baselineCalls := 0
			var baselineErr error
			// Exactly two synchronous invocations per reader: stop0 and its one
			// observed stop. There is no all-stop or all-phase sweep here.
			for _, requestedStop := range []int{0, reader.stop} {
				stop := requestedStop
				if stop > 0 && baselineCalls > 0 && stop > baselineCalls {
					// A historical stack's counter is not necessarily reachable in
					// this reader/phase. Exercise its actual final sampled work,
					// explicitly retaining both numbers rather than claiming that
					// the historical stop was reproduced.
					stop = baselineCalls
					t.Logf("historical stop not reached by current baseline: reader=%s requested=%d actualReplayStop=%d", reader.name, requestedStop, stop)
				}
				t.Run(fmt.Sprintf("chosen_phase128_requested%d_stop%d", requestedStop, stop), func(t *testing.T) {
					base, cancel := context.WithCancel(context.Background())
					defer cancel()
					sampled := &queryCacheSampleContext{Context: base, cancel: cancel, stop: stop}
					ctx := withExpressionFailureSlot(sampled)
					probe := &pipelineCancellation{ctx: ctx, remaining: 128}
					ctx = context.WithValue(ctx, pipelineCancellationKey{}, probe)
					preCalls, preCancelled := sampled.receipt()
					t.Logf("BEFORE reader=%s query=%q bytes=%d phase=128 phaseRecovered=false stop=%d baselineCalls=%d calls=%d remaining=%d latch=%v cancelled=%s", reader.name, query, len(query), stop, baselineCalls, preCalls, probe.remaining, probe.err, preCancelled.Format(time.RFC3339Nano))
					err := reader.run(NewStorageExecutor(engine), query, probe)
					calls, cancelled := sampled.receipt()
					elapsedAfterCancel := time.Duration(0)
					if !cancelled.IsZero() {
						elapsedAfterCancel = time.Since(cancelled)
					}
					t.Logf("AFTER reader=%s query=%q phase=128 stop=%d baselineCalls=%d calls=%d remaining=%d latch=%v expressionFailure=%v returned=%v cancelled=%s afterCancel=%s", reader.name, query, stop, baselineCalls, calls, probe.remaining, probe.err, getExpressionFailure(ctx), err, cancelled.Format(time.RFC3339Nano), elapsedAfterCancel)
					if stop == 0 {
						baselineCalls, baselineErr = calls, err
						require.Zero(t, preCalls)
						require.True(t, cancelled.IsZero(), "NONACCEPTANCE baseline cancelled")
						require.NoError(t, probe.err)
						require.NoError(t, getExpressionFailure(ctx))
						return
					}
					if calls < stop || cancelled.IsZero() || probe.err == nil {
						t.Fatalf("NONACCEPTANCE replay stop/latch not reached: reader=%s baseline=%d stop=%d calls=%d latch=%v", reader.name, baselineCalls, stop, calls, probe.err)
					}
					require.ErrorIs(t, probe.err, context.Canceled)
					require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
					require.ErrorIs(t, err, context.Canceled)
					require.Less(t, elapsedAfterCancel, 250*time.Millisecond)
				})
			}
			// Controls follow the diagnostic. Their ordinary parsing is not
			// claimed as cold setup or as warming that establishes acceptance.
			t.Run("nil_probe_error_class_control", func(t *testing.T) {
				want := reader.run(NewStorageExecutor(engine), query, nil)
				require.Error(t, want, "NONACCEPTANCE original invalid-query setup")
				require.Error(t, baselineErr, "NONACCEPTANCE checked baseline accepted invalid query")
				wantCode, _ := nornicerrors.Neo4jStatus(want)
				gotCode, _ := nornicerrors.Neo4jStatus(baselineErr)
				if wantCode != "" {
					require.Equal(t, wantCode, gotCode)
				} else {
					require.Equal(t, fmt.Sprintf("%T", want), fmt.Sprintf("%T", baselineErr))
					require.Equal(t, want.Error(), baselineErr.Error())
				}
			})
			t.Run("healthy_RETURN1_control", func(t *testing.T) {
				require.NoError(t, reader.run(NewStorageExecutor(engine), "RETURN 1 AS value", nil))
				base, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx := withExpressionFailureSlot(base)
				probe := &pipelineCancellation{ctx: ctx, remaining: 128}
				require.NoError(t, reader.run(NewStorageExecutor(engine), "RETURN 1 AS value", probe))
				require.NoError(t, probe.err)
				require.NoError(t, getExpressionFailure(ctx))
			})
		})
	}
}
