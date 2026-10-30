package encryption

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubReencryptor struct {
	count atomic.Int64
}

func (s *stubReencryptor) ReencryptWithDataKey(_ context.Context, _ *kms.DataKey) (int, error) {
	s.count.Add(1)
	return int(s.count.Load()), nil
}

func testLocalProvider(t *testing.T) kms.KeyProvider {
	t.Helper()
	p, err := kms.NewLocalProvider(kms.LocalConfig{
		MasterKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { kms.CloseProvider(p) })
	return p
}

func TestNewRotationManager_Defaults(t *testing.T) {
	t.Parallel()
	rm := NewRotationManager(nil, nil, RotationConfig{})
	assert.Equal(t, 90*24*time.Hour, rm.config.Interval)
	assert.Equal(t, 5, rm.config.RetentionCount)
}

func TestNewRotationManager_CustomConfig(t *testing.T) {
	t.Parallel()
	rm := NewRotationManager(nil, nil, RotationConfig{
		Interval:       7 * 24 * time.Hour,
		RetentionCount: 3,
	})
	assert.Equal(t, 7*24*time.Hour, rm.config.Interval)
	assert.Equal(t, 3, rm.config.RetentionCount)
}

func TestRotationManager_StartDisabled(t *testing.T) {
	t.Parallel()
	rm := NewRotationManager(testLocalProvider(t), &stubReencryptor{}, RotationConfig{
		Enabled: false,
	})
	// Should not panic or start goroutine
	rm.Start(context.Background())
	rm.Stop()
}

func TestRotationManager_StartNilProvider(t *testing.T) {
	t.Parallel()
	rm := NewRotationManager(nil, &stubReencryptor{}, RotationConfig{
		Enabled: true,
	})
	rm.Start(context.Background())
	rm.Stop()
}

func TestRotationManager_StartNilReencryptor(t *testing.T) {
	t.Parallel()
	rm := NewRotationManager(testLocalProvider(t), nil, RotationConfig{
		Enabled: true,
	})
	rm.Start(context.Background())
	rm.Stop()
}

func TestRotationManager_StartAndStop(t *testing.T) {
	t.Parallel()
	re := &stubReencryptor{}
	rm := NewRotationManager(testLocalProvider(t), re, RotationConfig{
		Enabled:  true,
		Interval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rm.Start(ctx)

	// Wait for at least one rotation
	time.Sleep(50 * time.Millisecond)
	rm.Stop()

	assert.GreaterOrEqual(t, re.count.Load(), int64(1), "should have performed at least one rotation")
}

func TestRotationManager_StopIdempotent(t *testing.T) {
	t.Parallel()
	rm := NewRotationManager(testLocalProvider(t), &stubReencryptor{}, RotationConfig{
		Enabled:  true,
		Interval: time.Hour,
	})
	rm.Start(context.Background())
	// Multiple stops should not panic
	rm.Stop()
	rm.Stop()
	rm.Stop()
}

func TestRotationManager_StopViaContextCancel(t *testing.T) {
	t.Parallel()
	re := &stubReencryptor{}
	rm := NewRotationManager(testLocalProvider(t), re, RotationConfig{
		Enabled:  true,
		Interval: time.Hour, // won't fire
	})

	ctx, cancel := context.WithCancel(context.Background())
	rm.Start(ctx)
	cancel() // Should cause loop to exit
	time.Sleep(20 * time.Millisecond)
	rm.Stop()
}

func TestPerformRotation(t *testing.T) {
	t.Parallel()
	re := &stubReencryptor{}
	rm := NewRotationManager(testLocalProvider(t), re, RotationConfig{
		Interval: time.Hour,
	})

	n, err := rm.performRotation(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, int64(1), re.count.Load())
}

func TestPerformRotation_ProviderError(t *testing.T) {
	t.Parallel()
	p, err := kms.NewLocalProvider(kms.LocalConfig{
		MasterKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	require.NoError(t, err)
	require.NoError(t, p.Close(context.Background())) // Close so GenerateDataKey fails

	re := &stubReencryptor{}
	rm := NewRotationManager(p, re, RotationConfig{
		Interval: time.Hour,
	})

	_, err = rm.performRotation(context.Background())
	require.Error(t, err)
	assert.Equal(t, int64(0), re.count.Load(), "reencryptor should not be called on provider error")
}
