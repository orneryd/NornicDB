package storage

import (
	"os"
	"runtime"
	"testing"
)

// skipIfPermissionsBypassed skips a test that expects a file-permission
// denial (chmod 0o000 / 0o400) when running as root, which bypasses file
// permissions, so the denied open succeeds.
func skipIfPermissionsBypassed(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not enforced on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root: file permissions are not enforced")
	}
}
