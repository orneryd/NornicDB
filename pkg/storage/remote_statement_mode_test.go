package storage

import "testing"

func TestRemoteHTTPModeEvidence(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name  string
		mode  string
		stats *remoteQueryStats
		want  int32
	}{
		{"missing", "", nil, remoteModeUnknown},
		{"empty", "", &remoteQueryStats{}, remoteModeUnknown},
		{"zero counters", "", &remoteQueryStats{ContainsUpdates: &no}, remoteModeUnknown},
		{"positive standard counters", "", &remoteQueryStats{ContainsUpdates: &yes}, remoteModeWrite},
		{"system updates", "", &remoteQueryStats{ContainsSystemUpdates: &yes}, remoteModeWrite},
		{"resolved read", "r", &remoteQueryStats{ContainsUpdates: &no}, remoteModeRead},
		{"resolved write without counters", "w", &remoteQueryStats{ContainsUpdates: &no}, remoteModeWrite},
		{"invalid mode", "invalid", &remoteQueryStats{}, remoteModeUnknown},
		{"updates override read", "r", &remoteQueryStats{ContainsUpdates: &yes}, remoteModeWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := remoteHTTPQueryMode(tc.stats, tc.mode); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}
