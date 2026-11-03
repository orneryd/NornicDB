package convert

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

// AppendBoltNumber writes the same numeric kinds as the JSON codec. PackStream
// has signed INTEGER and FLOAT, but no unsigned integer. NornicDB's native
// unsigned extension above MaxInt64 therefore travels as an exact decimal
// STRING. It must never wrap to a negative INTEGER or round to FLOAT.
func AppendBoltNumber(dst []byte, value interface{}) ([]byte, bool) {
	number, ok := ReadNumber(value)
	if !ok {
		return dst, false
	}
	if integer, ok := number.Signed(); ok {
		return AppendBoltInteger(dst, integer), true
	}
	if unsigned, ok := number.Unsigned(); ok {
		return AppendBoltUnsigned(dst, unsigned), true
	}
	floating, _ := number.Float()
	return AppendBoltFloat(dst, floating), true
}

// AppendBoltUnsigned owns the unsigned protocol boundary for both typed scalar
// dispatch and reflective numeric values. Signed and FLOAT leaves need no
// unsigned range check or intermediate Number carrier.
func AppendBoltUnsigned(dst []byte, value uint64) []byte {
	if value <= math.MaxInt64 {
		return AppendBoltInteger(dst, int64(value))
	}
	text := strconv.FormatUint(value, 10) // 19 or 20 bytes: STRING_8.
	return append(append(dst, 0xD0, byte(len(text))), text...)
}

func AppendBoltInteger(dst []byte, value int64) []byte {
	switch {
	case value >= -16 && value <= 127:
		return append(dst, byte(value))
	case value >= -128 && value < -16:
		return append(dst, 0xC8, byte(value))
	case value >= -32768 && value <= 32767:
		return binary.BigEndian.AppendUint16(append(dst, 0xC9), uint16(value))
	case value >= -2147483648 && value <= 2147483647:
		return binary.BigEndian.AppendUint32(append(dst, 0xCA), uint32(value))
	default:
		return binary.BigEndian.AppendUint64(append(dst, 0xCB), uint64(value))
	}
}

func AppendBoltFloat(dst []byte, value float64) []byte {
	return binary.BigEndian.AppendUint64(append(dst, 0xC1), math.Float64bits(value))
}

// DecodeBoltNumber recognizes a numeric prefix and reports bytes consumed.
// A recognized but truncated number is an error; other markers belong to the
// enclosing protocol decoder. This shares the numeric boundary with writers.
func DecodeBoltNumber(data []byte) (value interface{}, size int, recognized bool, err error) {
	if len(data) == 0 {
		return nil, 0, false, nil
	}
	marker := data[0]
	if marker <= 0x7F || marker >= 0xF0 {
		return int64(int8(marker)), 1, true, nil
	}
	width := 0
	name := ""
	switch marker {
	case 0xC8:
		width, name = 1, "INT8"
	case 0xC9:
		width, name = 2, "INT16"
	case 0xCA:
		width, name = 4, "INT32"
	case 0xCB:
		width, name = 8, "INT64"
	case 0xC1:
		width, name = 8, "Float64"
	default:
		return nil, 0, false, nil
	}
	if len(data) < width+1 {
		return nil, 0, true, fmt.Errorf("incomplete %s", name)
	}
	switch marker {
	case 0xC8:
		value = int64(int8(data[1]))
	case 0xC9:
		value = int64(int16(binary.BigEndian.Uint16(data[1:])))
	case 0xCA:
		value = int64(int32(binary.BigEndian.Uint32(data[1:])))
	case 0xCB:
		value = int64(binary.BigEndian.Uint64(data[1:]))
	case 0xC1:
		value = math.Float64frombits(binary.BigEndian.Uint64(data[1:]))
	}
	return value, width + 1, true, nil
}
