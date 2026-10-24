package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryMalformedManifestClosesStore(t *testing.T) {
	for _, api := range []string{"result", "logger", "plain"} {
		t.Run(api, func(t *testing.T) {
			requireRecoveryStoresClosed(t)
			dir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "segments"), 0755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "segments", "manifest.json"), []byte("{broken"), 0644))
			var engine *MemoryEngine
			var err error
			require.NotPanics(t, func() {
				switch api {
				case "result":
					engine, _, err = fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(dir, ""))
				case "logger":
					engine, err = fixtureowner.OwnResult[*MemoryEngine](t, ErrStorageClosed)(RecoverFromWALWithLogger(dir, "", nil))
				case "plain":
					engine, err = fixtureowner.OwnResult[*MemoryEngine](t, ErrStorageClosed)(RecoverFromWAL(dir, ""))
				}
			})
			require.ErrorContains(t, err, "manifest")
			require.Nil(t, engine)
		})
	}
}
