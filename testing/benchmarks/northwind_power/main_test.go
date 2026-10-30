package main

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestThroughputOpsPerSecondUsesMeasuredDuration(t *testing.T) {
	got := throughputOpsPerSecond(40, 5239.733)
	if math.Abs(got-7.634) > 0.001 {
		t.Fatalf("end-to-end throughput = %.3f ops/sec, want 7.634", got)
	}

	latenciesMs := []float64{9.6038, 216.3478, 67.3183, 85.4274}
	latencyOnly := throughputOpsPerSecond(40, sumFloat64(latenciesMs)*10)
	if math.Abs(latencyOnly-10.56) > 0.01 {
		t.Fatalf("latency-only throughput = %.3f ops/sec, want about 10.56", latencyOnly)
	}
	if throughputOpsPerSecond(0, 100) != 0 || throughputOpsPerSecond(1, 0) != 0 {
		t.Fatal("non-positive operations or duration must produce zero throughput")
	}
}

func TestGraphWipeBatchSizeIsBounded(t *testing.T) {
	tests := []struct {
		name          string
		seedBatchSize int
		want          int
	}{
		{name: "configured smaller batch", seedBatchSize: 100, want: 100},
		{name: "default for invalid batch", seedBatchSize: 0, want: maxGraphWipeBatchSize},
		{name: "cap larger batch", seedBatchSize: 5000, want: maxGraphWipeBatchSize},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := graphWipeBatchSize(test.seedBatchSize); got != test.want {
				t.Fatalf("graphWipeBatchSize(%d) = %d, want %d", test.seedBatchSize, got, test.want)
			}
		})
	}
}

func TestWipeGraphBatchesRepeatsUntilEmpty(t *testing.T) {
	deletedPerBatch := []int64{250, 250, 12, 0}
	calls := 0
	totalDeleted, err := wipeGraphBatches(context.Background(), 250, func(_ context.Context, limit int) (int64, error) {
		if limit != 250 {
			t.Fatalf("delete batch limit = %d, want 250", limit)
		}
		deleted := deletedPerBatch[calls]
		calls++
		return deleted, nil
	})
	if err != nil {
		t.Fatalf("wipeGraphBatches() error = %v", err)
	}
	if totalDeleted != 512 || calls != len(deletedPerBatch) {
		t.Fatalf("wipe deleted %d nodes in %d batches, want 512 nodes in 4 batches", totalDeleted, calls)
	}
}

func TestWipeGraphBatchesStopsOnFailure(t *testing.T) {
	wantErr := errors.New("transaction failed")
	calls := 0
	totalDeleted, err := wipeGraphBatches(context.Background(), 500, func(_ context.Context, _ int) (int64, error) {
		calls++
		if calls == 1 {
			return 3, nil
		}
		return 0, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("wipeGraphBatches() error = %v, want %v", err, wantErr)
	}
	if totalDeleted != 3 || calls != 2 {
		t.Fatalf("failed wipe returned %d deleted nodes after %d batches, want 3 after 2", totalDeleted, calls)
	}
}
