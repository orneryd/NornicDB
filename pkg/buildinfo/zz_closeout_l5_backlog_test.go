package buildinfo

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This administrative check uses the frozen tracker record and the linked
// parking inventory. It evaluates the disposition independently of release tests.
func TestCloseoutL5BacklogDisposition(t *testing.T) {
	raw, err := base64.RawURLEncoding.DecodeString(os.Getenv("CLOSEOUT_L5_BACKLOG_EVIDENCE"))
	require.NoError(t, err)
	var evidence struct {
		SequenceID     int      `json:"sequence_id"`
		ItemState      string   `json:"item_state"`
		StateID        string   `json:"state_id"`
		StateGroup     string   `json:"state_group"`
		CompletedAt    *string  `json:"completed_at"`
		ReviewPending  string   `json:"review_pending"`
		DecisionStatus string   `json:"decision_status"`
		IssueNumber    int      `json:"issue_number"`
		Parked         []string `json:"parked"`
	}
	require.NoError(t, json.Unmarshal(raw, &evidence))
	require.Equal(t, 28, evidence.SequenceID)
	require.NotEmpty(t, evidence.ItemState)
	require.Equal(t, evidence.ItemState, evidence.StateID)
	require.Equal(t, "backlog", evidence.StateGroup)
	require.Nil(t, evidence.CompletedAt)
	require.Regexp(t, `(?i)review the parking ledger`, evidence.ReviewPending)
	require.Regexp(t, `(?i)have not been selected`, evidence.DecisionStatus)
	require.Equal(t, 1516, evidence.IssueNumber)
	require.NotEmpty(t, evidence.Parked)
	entry := regexp.MustCompile(`^#[0-9]+\|[A-Za-z0-9/_-]+\|[a-f0-9]{7,40}$`)
	var impersonation, roles bool
	for _, parked := range evidence.Parked {
		require.True(t, entry.MatchString(parked), parked)
		impersonation = impersonation || strings.HasPrefix(parked, "#1352|")
		roles = roles || strings.HasPrefix(parked, "#1393|")
	}
	require.True(t, impersonation)
	require.True(t, roles)
	t.Logf("L5 P-N151-28 partly present: parking inventory #%d has %d retained branches, including impersonation and roles. The frozen Plane record is open in Backlog, with review and release selection pending.", evidence.IssueNumber, len(evidence.Parked))
}
