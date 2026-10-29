package replication

import (
	"errors"
	"fmt"
	"io"

	"github.com/orneryd/nornicdb/internal/gobcodec"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// TranscodeV4ReplicationSnapshot converts a caller-owned snapshot written by
// the old replication adapter to the current MessagePack snapshot. The caller
// owns durable publication of the output and must retain the original until
// the converted snapshot has been verified. Current snapshot reads do not
// fall back to the historical gob format.
func TranscodeV4ReplicationSnapshot(source io.Reader, destination io.Writer) error {
	var snapshot storage.ReplicationSnapshot
	if err := gobcodec.Decode(source, &snapshot, gobcodec.RejectTrailing); err != nil {
		if errors.Is(err, gobcodec.ErrTrailing) {
			return fmt.Errorf("trailing historical replication snapshot data: %w", err)
		}
		return fmt.Errorf("decode historical replication snapshot: %w", err)
	}
	data, err := storage.EncodeCommandPayload(snapshot)
	if err != nil {
		return fmt.Errorf("encode replication snapshot: %w", err)
	}
	written, err := destination.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return nil
}
