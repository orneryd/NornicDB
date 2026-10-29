// Package storage - Serialization helpers for BadgerDB.
//
// All new writes use msgpack with a small header. Legacy gob bodies (no
// header) are still decodable for one purpose only: the offline in-place
// migration tool that rewrites them as msgpack. The encoder NEVER emits
// gob.
package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/internal/gobcodec"
	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	serializationMagic   = "\xffNDB"
	serializationVersion = byte(1)
	serializerIDGob      = byte(1) // legacy: still decoded; never emitted
	serializerIDMsgpack  = byte(2)
)

// init registers types with gob for the legacy decoder. New writes never
// touch gob, but historical bodies in pre-msgpack data directories still
// decode through this path until the migration tool rewrites them.
func init() {
	msgpack.RegisterExtEncoder(40, storedTime{}, encodeStoredTime)
	msgpack.RegisterExtDecoder(40, storedTime{}, decodeStoredTime)
	gob.Register(int(0))
	gob.Register(int32(0))
	gob.Register(int64(0))
	gob.Register(float32(0))
	gob.Register(float64(0))
	gob.Register("")
	gob.Register(true)
	gob.Register(time.Time{})

	gob.Register([]interface{}{})
	gob.Register([]string{})
	gob.Register([]int{})
	gob.Register([]int32{})
	gob.Register([]int64{})
	gob.Register([]float32{})
	gob.Register([]float64{})
	gob.Register([]bool{})

	gob.Register(map[string]interface{}{})
}

type storedTime struct{ value time.Time }

func encodeStoredTime(_ *msgpack.Encoder, value reflect.Value) ([]byte, error) {
	timestamp := value.Interface().(storedTime).value
	zoneID := timestamp.Location().String()
	if !strings.Contains(zoneID, "/") {
		zoneID = ""
	}
	data := make([]byte, 16+len(zoneID))
	binary.BigEndian.PutUint64(data[0:8], uint64(timestamp.Unix()))
	binary.BigEndian.PutUint32(data[8:12], uint32(timestamp.Nanosecond()))
	_, offset := timestamp.Zone()
	binary.BigEndian.PutUint32(data[12:16], uint32(int32(offset)))
	copy(data[16:], zoneID)
	return data, nil
}

func decodeStoredTime(decoder *msgpack.Decoder, value reflect.Value, length int) error {
	if length < 16 {
		return fmt.Errorf("invalid stored time payload length %d", length)
	}
	data := make([]byte, length)
	if err := decoder.ReadFull(data); err != nil {
		return err
	}
	seconds := int64(binary.BigEndian.Uint64(data[0:8]))
	nanos := int64(binary.BigEndian.Uint32(data[8:12]))
	offset := int(int32(binary.BigEndian.Uint32(data[12:16])))
	location := time.FixedZone("", offset)
	if zoneID := string(data[16:]); zoneID != "" {
		loaded, err := time.LoadLocation(zoneID)
		if err != nil {
			return err
		}
		location = loaded
	}
	value.Set(reflect.ValueOf(storedTime{value: time.Unix(seconds, nanos).In(location)}))
	return nil
}

// encodeValue serializes value as msgpack with the standard header. There
// is no longer a runtime serializer choice — msgpack is the only format
// the engine emits.
func encodeValue(value any) ([]byte, error) {
	return encodeValueWithContext(nil, value)
}

func encodeValueWithContext(ctx context.Context, value any) ([]byte, error) {
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	payload, err := encoder.encodeWithContext(ctx, value)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(payload), nil
}

// valueEncoder owns its output until the next encode. Streams retain one;
// callers that need owned bytes clone the result before returning it to the pool.
type valueEncoder struct {
	buffer  bytes.Buffer
	msgpack *msgpack.Encoder
}

var valueEncoders = sync.Pool{New: func() any { return new(valueEncoder) }}

func getValueEncoder() *valueEncoder { return valueEncoders.Get().(*valueEncoder) }

func putValueEncoder(encoder *valueEncoder) {
	// Do not retain unusually large materialized records between requests.
	if encoder.buffer.Cap() > defaultSnapshotRecordSize {
		return
	}
	valueEncoders.Put(encoder)
}

func (e *valueEncoder) encode(value any) ([]byte, error) {
	return e.encodeWithContext(nil, value)
}

func (e *valueEncoder) encodeWithContext(ctx context.Context, value any) ([]byte, error) {
	if err := copyContextError(ctx); err != nil {
		return nil, err
	}
	e.reset()
	if ctx != nil {
		e.msgpack.Reset(util.WriterWithContext(ctx, &e.buffer))
	}
	if err := e.msgpack.Encode(value); err != nil {
		return nil, err
	}
	return e.buffer.Bytes(), nil
}

func (e *valueEncoder) reset() {
	e.buffer.Reset()
	e.buffer.WriteString(serializationMagic)
	e.buffer.WriteByte(serializationVersion)
	e.buffer.WriteByte(serializerIDMsgpack)
	if e.msgpack == nil {
		e.msgpack = msgpack.NewEncoder(&e.buffer)
	} else {
		// Reset options and dictionaries just as msgpack.Marshal does.
		e.msgpack.Reset(&e.buffer)
	}
}

// decodeValue reads a value emitted by encodeValue OR a legacy
// header-less gob body. Legacy gob handling exists solely so the
// in-place migration tool can read old data; the engine's hot path will
// only ever see msgpack bodies in databases written by current code.
func decodeValue(data []byte, value any) error {
	return decodeValueWithContext(nil, data, value)
}
func decodeValueWithContext(ctx context.Context, data []byte, value any) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	headerSerializer, payload, ok, err := splitSerializationHeader(data)
	if err != nil {
		return err
	}
	if ok {
		switch headerSerializer {
		case serializerIDMsgpack:
			return util.DecodeMsgpackBytesWithContext(ctx, payload, value)
		case serializerIDGob:
			return gobcodec.Decode(util.ReaderWithContext(ctx, bytes.NewReader(payload)), value, gobcodec.AllowTrailing)
		default:
			return fmt.Errorf("unsupported storage serializer id: %d", headerSerializer)
		}
	}
	// Legacy fallback: pre-header gob bodies.
	return gobcodec.Decode(util.ReaderWithContext(ctx, bytes.NewReader(data)), value, gobcodec.AllowTrailing)
}

// splitSerializationHeader returns the serializer id from the leading
// header bytes, or ok=false if data has no recognizable header.
func splitSerializationHeader(data []byte) (byte, []byte, bool, error) {
	if len(data) < len(serializationMagic)+2 {
		return 0, nil, false, nil
	}
	if string(data[:len(serializationMagic)]) != serializationMagic {
		return 0, nil, false, nil
	}
	version := data[len(serializationMagic)]
	if version != serializationVersion {
		return 0, nil, false, fmt.Errorf("unsupported serialization version: %d", version)
	}
	return data[len(serializationMagic)+1], data[len(serializationMagic)+2:], true, nil
}

// serializeNode converts a Node to bytes for BadgerDB storage.
func serializeNode(node *Node) ([]byte, error) {
	data, err := encodeValue(node)
	if err != nil {
		return nil, fmt.Errorf("encoding node: %w", err)
	}
	return data, nil
}

// deserializeNode converts stored bytes back to a Node.
func deserializeNode(data []byte) (*Node, error) {
	var node Node
	if err := decodeValue(data, &node); err != nil {
		return nil, fmt.Errorf("decoding node: %w", err)
	}
	return &node, nil
}

// serializeEdge converts an Edge to bytes for BadgerDB storage.
func serializeEdge(edge *Edge) ([]byte, error) {
	data, err := encodeValue(edge)
	if err != nil {
		return nil, fmt.Errorf("encoding edge: %w", err)
	}
	return data, nil
}

// deserializeEdge converts stored bytes back to an Edge.
func deserializeEdge(data []byte) (*Edge, error) {
	var edge Edge
	if err := decodeValue(data, &edge); err != nil {
		return nil, fmt.Errorf("decoding edge: %w", err)
	}
	return &edge, nil
}
