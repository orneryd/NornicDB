package nornicdb

import (
	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDBCloseR3RetentionOutcome(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	db.retentionManager = retention.NewManager()
	db.config.Retention.PoliciesFile = t.TempDir() // A directory cannot be overwritten as a policy file.
	err = db.Close()
	require.ErrorContains(t, err, "save retention policies")
	require.EqualError(t, db.Close(), err.Error(), "repeated Close must publish the same outcome")
}
