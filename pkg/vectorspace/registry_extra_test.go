package vectorspace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVectorSpace_SetVectorCount(t *testing.T) {
	vs := &VectorSpace{}

	vs.SetVectorCount(42)
	stats := vs.stats("hash")
	assert.Equal(t, int64(42), stats.VectorCount)

	vs.SetVectorCount(0)
	stats = vs.stats("hash")
	assert.Equal(t, int64(0), stats.VectorCount)
}

func TestVectorSpace_IncrementVectorCount(t *testing.T) {
	vs := &VectorSpace{}

	n := vs.IncrementVectorCount(5)
	assert.Equal(t, int64(5), n)

	n = vs.IncrementVectorCount(3)
	assert.Equal(t, int64(8), n)

	n = vs.IncrementVectorCount(-2)
	assert.Equal(t, int64(6), n)
}

func TestVectorSpace_IncrementVectorCount_Concurrent(t *testing.T) {
	vs := &VectorSpace{}
	const goroutines = 50

	done := make(chan struct{}, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			vs.IncrementVectorCount(1)
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	stats := vs.stats("hash")
	assert.Equal(t, int64(goroutines), stats.VectorCount)
}
