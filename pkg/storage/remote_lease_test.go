package storage

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRemoteLeaseRenewalInterval (NOR-284): a coordinator renews its HTTP
// participant's remote session at a third of the lifetime the server reported
// when the transaction opened, at most every 10 s and at least every 100 ms,
// and every 10 s when the server reported none.
func TestRemoteLeaseRenewalInterval(t *testing.T) {
	now := time.Date(2026, 11, 9, 6, 0, 0, 0, time.UTC)
	at := func(after time.Duration) string { return now.Add(after).Format(http.TimeFormat) }
	require.Equal(t, 10*time.Second, remoteLeaseRenewalInterval(at(30*time.Second), now), "NornicDB's 30 s session")
	require.Equal(t, 10*time.Second, remoteLeaseRenewalInterval(at(60*time.Second), now), "Neo4j's 60 s idle timeout")
	require.Equal(t, 3*time.Second, remoteLeaseRenewalInterval(at(9*time.Second), now))
	require.Equal(t, 100*time.Millisecond, remoteLeaseRenewalInterval(at(0), now))
	require.Equal(t, 10*time.Second, remoteLeaseRenewalInterval("", now))
	require.Equal(t, 10*time.Second, remoteLeaseRenewalInterval("not a date", now))
}
