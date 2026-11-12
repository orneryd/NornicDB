package storage

import (
	"bytes"
	"math"
	"strconv"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR-412: encodeWALRecord with a record of 4 GiB of data (a read-only
// anonymous mapping, so no 4 GiB of memory is allocated). Without spanning it
// returns the frame-size error. With spanning it returns an error that names
// the record format's limit of 4294967295 bytes and not the frame size, as the
// journal's record rejection.
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
	require.EqualError(t, err, "wal: entry exceeds maximum WAL frame size of 16777216 bytes", "without spanning, the frame-size error")
	_, err = encodeWALRecord(&bytes.Buffer{}, entry, true)
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds the record format's limit of 4294967295 bytes")
	require.NotContains(t, err.Error(), "frame size")
	var rejection *walRecordRejection
	require.ErrorAs(t, err, &rejection, "the error is the journal's record rejection")
}
