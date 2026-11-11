package storage

import (
	"bytes"
	"math"
	"strconv"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR-412: a large journal record may span any number of frames, but the
// record format stores its data length in 32 bits. A record over that
// ceiling is refused with an error that names the record limit, not the
// frame size. A WAL that writes standard records refuses it as it always
// did: with the frame-size error, before the ceiling is looked at. A
// read-only anonymous mapping exercises the guard without allocating 4 GiB
// of memory.
func TestWALRecordOverTheFormatCeilingIsRefusedWithItsOwnError(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("requires a 64-bit address space")
	}
	size := uint64(math.MaxUint32) + 1
	data, err := syscall.Mmap(-1, 0, int(size), syscall.PROT_READ, syscall.MAP_PRIVATE|syscall.MAP_ANON|syscall.MAP_NORESERVE)
	require.NoError(t, err)
	defer func() { require.NoError(t, syscall.Munmap(data)) }()
	entry := &WALEntry{Sequence: 1, Operation: OpEntityCommit, Database: "test", Data: data}
	_, err = encodeWALRecord(&bytes.Buffer{}, entry, false)
	require.EqualError(t, err, "wal: entry exceeds maximum WAL frame size of 16777216 bytes", "standard records keep the one-frame refusal")
	_, err = encodeWALRecord(&bytes.Buffer{}, entry, true)
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds the record format's limit of 4294967295 bytes")
	require.NotContains(t, err.Error(), "frame size")
	var rejection *walRecordRejection
	require.ErrorAs(t, err, &rejection, "refused before anything is written")
}
