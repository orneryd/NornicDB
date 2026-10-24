package fabric

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionLifetimeOwner(t *testing.T) {
	type key struct{}
	owner, cancelOwner := context.WithCancel(context.Background())
	defer cancelOwner()
	request, cancelRequest := context.WithCancel(context.WithValue(context.Background(), key{}, "identity"))
	tx := NewFabricTransaction("owner")
	lifetime := tx.BindContext(WithTransactionLifetime(request, owner))
	cancelRequest()
	require.NoError(t, lifetime.Err())
	require.Equal(t, "identity", lifetime.Value(key{}))
	require.Same(t, lifetime, tx.BindContext(context.Background()))
	cancelOwner()
	<-lifetime.Done()
	require.ErrorIs(t, lifetime.Err(), context.Canceled)
}

func TestTransactionLifetimeEndsAfterParticipants(t *testing.T) {
	for _, commit := range []bool{false, true} {
		tx := NewFabricTransaction("terminal")
		lifetime := tx.BindContext(context.Background())
		_, err := tx.GetOrOpen("one", false)
		require.NoError(t, err)
		end := func(*SubTransaction) error { require.NoError(t, lifetime.Err()); return nil }
		if commit {
			err = tx.Commit(end, end)
		} else {
			err = tx.Rollback(end)
		}
		require.NoError(t, err)
		require.ErrorIs(t, lifetime.Err(), context.Canceled)
	}
}
