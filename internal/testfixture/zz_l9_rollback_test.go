package testfixture

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

type l9FixtureTransaction struct{ rollbackError error }

func (*l9FixtureTransaction) SetNamespace(string) error { return nil }
func (tx *l9FixtureTransaction) Rollback() error {
	fmt.Printf("L9_ROLLBACK error=%v\n", tx.rollbackError)
	return tx.rollbackError
}

func TestL9FixtureRollbackChild(t *testing.T) {
	mode := os.Getenv("L9_FIXTURE_ROLLBACK")
	if mode == "" {
		t.Skip("run by the subprocess tests")
	}
	tx := &l9FixtureTransaction{}
	if mode != "success" {
		tx.rollbackError = errors.New("L9 sentinel")
	}
	if mode == "checked" {
		t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
		return
	}
	NamespaceTransaction(t, func(context.Context, bool) (*l9FixtureTransaction, error) {
		return tx, nil
	}, true, "tenant")
}

func l9FixtureRollbackChild(t *testing.T, mode string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestL9FixtureRollbackChild$", "-test.v")
	cmd.Env = append(os.Environ(), "L9_FIXTURE_ROLLBACK="+mode)
	output, err := cmd.CombinedOutput()
	t.Logf("%s child exit=%v output=%s", mode, err, output)
	return string(output), err
}

func TestL9FixtureReportsRollbackError(t *testing.T) {
	control, err := l9FixtureRollbackChild(t, "success")
	require.NoError(t, err)
	require.Contains(t, control, "L9_ROLLBACK error=<nil>")
	output, err := l9FixtureRollbackChild(t, "unchecked")
	require.Contains(t, output, "L9_ROLLBACK error=L9 sentinel")
	require.Error(t, err, "the child must fail when fixture rollback returns an error")
}

func TestL9FixtureRollbackOwner(t *testing.T) {
	unchecked, uncheckedErr := l9FixtureRollbackChild(t, "unchecked")
	checked, checkedErr := l9FixtureRollbackChild(t, "checked")
	require.Contains(t, unchecked, "L9_ROLLBACK error=L9 sentinel")
	require.Contains(t, checked, "L9_ROLLBACK error=L9 sentinel")
	require.NoError(t, uncheckedErr, "fixture discards this rollback result")
	require.Error(t, checkedErr, "checking that same call surfaces its error")
}
