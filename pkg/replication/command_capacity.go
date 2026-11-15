package replication

import (
	"context"
	"encoding/base64"
	"math"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// commandAdmission belongs to storage owners that impose a physical log limit.
// Custom state machines retain their own admission and publication contracts.
type commandAdmission interface {
	checkCommandCapacity(context.Context, *Command) error
}

func checkCommandCapacity(ctx context.Context, owner Storage, cmd *Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cmd == nil {
		return storage.ErrInvalidData
	}
	if admission, ok := owner.(commandAdmission); ok {
		return admission.checkCommandCapacity(ctx, cmd)
	}
	return nil
}

// checkCommandCapacity measures the same replicationWALRecord the FIFO writer
// appends. Its byte slice is standard JSON base64: measure an empty slot and add
// its exact encoded length, without allocating/copying the command's large body.
func (a *StorageAdapter) checkCommandCapacity(ctx context.Context, cmd *Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.wal == nil {
		return ErrClosed
	}
	// The immutable field-layout bound proves normal commands fit using only
	// lengths. Exact metadata encoding is needed only near the record limit or
	// for a timestamp that the JSON owner may reject.
	_, offset := cmd.Timestamp.Zone()
	if cmd.Timestamp.Year() >= 0 && cmd.Timestamp.Year() <= 9999 && offset > -24*60*60 && offset < 24*60*60 {
		bound := maxCommandMetadataSize + 6*len(cmd.RequestID)
		if cmd.Data != nil {
			bound += base64.StdEncoding.EncodedLen(len(cmd.Data)) + 2 - len("null")
		}
		if a.wal.CheckRecordCapacity(replicationWALOperation, bound, "") == nil {
			return nil
		}
	}
	size, err := (replicationWALRecord{Timestamp: cmd.Timestamp.UnixNano(), Command: cmd}).encodedSize()
	if err != nil {
		return err
	}
	return a.wal.CheckRecordCapacity(replicationWALOperation, size, "")
}

func (record replicationWALRecord) encodedSize() (int, error) {
	withoutData := *record.Command
	withoutData.Data = nil
	body := record
	body.Command = &withoutData
	header, err := jsontext.Marshal(body)
	if err != nil {
		return 0, err
	}
	size := len(header)
	if record.Command.Data != nil {
		// Replace the four bytes of null with a quoted base64 value.
		size += base64.StdEncoding.EncodedLen(len(record.Command.Data)) + 2 - len("null")
	}
	return size, nil
}

var maxCommandMetadataSize = func() int {
	at := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("", 23*60*60+59*60))
	// MinInt64 includes the sign and widest UnixNano representation.
	record := replicationWALRecord{Timestamp: math.MinInt64, Command: &Command{Type: CommandType(math.MaxUint8), Timestamp: at}}
	encoded, err := jsontext.Marshal(record)
	if err != nil {
		panic(err)
	}
	return len(encoded)
}()
