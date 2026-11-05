package search

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/vmihailenco/msgpack/v5"
)

// encodeRelationshipSnapshot writes one MessagePack map. Each vector is a
// bin value holding little-endian IEEE-754 float32 bits; writing in chunks
// avoids a copy or per-float filesystem write for large vector populations.
func encodeRelationshipSnapshot(w io.Writer, snapshot *relationshipVectorsFile) error {
	enc := msgpack.NewEncoder(w)
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := enc.EncodeString("Format"); err != nil {
		return err
	}
	if err := enc.EncodeInt(int64(snapshot.Format)); err != nil {
		return err
	}
	if err := enc.EncodeString("Entries"); err != nil {
		return err
	}
	if snapshot.Entries == nil {
		return enc.EncodeNil()
	}
	if uint64(len(snapshot.Entries)) > math.MaxUint32 {
		return fmt.Errorf("too many relationship snapshot entries")
	}
	if err := enc.EncodeMapLen(len(snapshot.Entries)); err != nil {
		return err
	}
	var scratch [4096]byte
	for id, entry := range snapshot.Entries {
		if uint64(len(id)) > math.MaxUint32 {
			return fmt.Errorf("relationship snapshot ID is too long")
		}
		if err := enc.EncodeString(id); err != nil {
			return err
		}
		if entry == nil {
			if err := enc.EncodeNil(); err != nil {
				return err
			}
			continue
		}
		if err := enc.EncodeMapLen(3); err != nil {
			return err
		}
		if err := enc.EncodeString("Source"); err != nil {
			return err
		}
		if err := enc.EncodeUint64(entry.Source); err != nil {
			return err
		}
		if err := enc.EncodeString("Vectors"); err != nil {
			return err
		}
		if entry.Vectors == nil {
			if err := enc.EncodeNil(); err != nil {
				return err
			}
		} else {
			if uint64(len(entry.Vectors)) > math.MaxUint32 {
				return fmt.Errorf("too many relationship vectors")
			}
			if err := enc.EncodeArrayLen(len(entry.Vectors)); err != nil {
				return err
			}
			for _, vector := range entry.Vectors {
				if vector == nil {
					if err := enc.EncodeNil(); err != nil {
						return err
					}
					continue
				}
				if uint64(len(vector)) > math.MaxUint32/4 || len(vector) > int(^uint(0)>>1)/4 {
					return fmt.Errorf("relationship vector is too long")
				}
				if err := enc.EncodeBytesLen(4 * len(vector)); err != nil {
					return err
				}
				for offset := 0; offset < len(vector); {
					count := min(len(vector)-offset, len(scratch)/4)
					for j, value := range vector[offset : offset+count] {
						binary.LittleEndian.PutUint32(scratch[4*j:], math.Float32bits(value))
					}
					written, err := w.Write(scratch[:4*count])
					if err != nil {
						return err
					}
					if written != 4*count {
						return io.ErrShortWrite
					}
					offset += count
				}
			}
		}
		if err := enc.EncodeString("Spans"); err != nil {
			return err
		}
		if err := enc.Encode(entry.Spans); err != nil {
			return err
		}
	}
	return nil
}
