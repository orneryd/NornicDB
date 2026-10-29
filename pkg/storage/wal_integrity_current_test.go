package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A valid frame checksum does not make an undecodable WAL entry healthy.
func TestWALIntegrityReportsUndecodableCurrentEntry(t *testing.T) {
	for _, corruptCRC := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt_crc=%t", corruptCRC), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal.log")
			record, _ := buildAtomicRecord([]byte{0xff}, walFormatVersionCurrent)
			if corruptCRC {
				record[9+int(binary.LittleEndian.Uint32(record[5:9]))] ^= 0xff
			}
			require.NoError(t, os.WriteFile(path, record, 0600))
			report, err := CheckWALIntegrity(path)
			require.NoError(t, err)
			require.NotNil(t, report)
			require.False(t, report.Healthy)
			require.Equal(t, 1, report.CorruptedEntries, "count one damaged entry even if both checks fail")
			require.NotEmpty(t, report.Errors)
		})
	}
}
