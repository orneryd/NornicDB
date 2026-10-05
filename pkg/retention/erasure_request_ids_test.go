package retention

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Erasure requests made back to back each keep their own record.
func TestCreateErasureRequestGivesEachRequestItsOwnID(t *testing.T) {
	m := NewManager()
	const subjects = 64
	ids := map[string]string{}
	for i := 0; i < subjects; i++ {
		subject := string(rune('a'+i%26)) + string(rune('A'+i/26))
		req, err := m.CreateErasureRequest(subject, subject+"@example.com")
		require.NoError(t, err)
		require.NotContains(t, ids, req.ID)
		ids[req.ID] = subject
	}
	for id, subject := range ids {
		req, err := m.GetErasureRequest(id)
		require.NoError(t, err)
		require.Equal(t, subject, req.SubjectID)
	}
	require.Len(t, m.ListErasureRequests(), subjects)
}
