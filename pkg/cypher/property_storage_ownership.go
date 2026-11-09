package cypher

import (
	"slices"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// storedTemporalTime is the representation returned by the temporal extensions
// 41-44: the instant and offset survive, while a fixed-zone name does not.
func storedTemporalTime(value time.Time) time.Time {
	_, offset := value.Zone()
	return time.Unix(value.Unix(), int64(value.Nanosecond())).In(time.FixedZone("", int(int32(offset))))
}

func (v CypherDate) StoragePropertyValue() (any, bool) {
	return CypherDate{Time: storedTemporalTime(v.Time)}, true
}
func (v CypherLocalTime) StoragePropertyValue() (any, bool) {
	return CypherLocalTime{Time: storedTemporalTime(v.Time)}, true
}
func (v CypherTime) StoragePropertyValue() (any, bool) {
	return CypherTime{Time: storedTemporalTime(v.Time)}, true
}
func (v CypherLocalDateTime) StoragePropertyValue() (any, bool) {
	return CypherLocalDateTime{Time: storedTemporalTime(v.Time)}, true
}
func (v CypherDateTime) StoragePropertyValue() (any, bool) {
	location := v.Time.Location()
	if v.ZoneID == "" {
		_, offset := v.Time.Zone()
		location = time.FixedZone("", int(int32(offset)))
	} else if pinned, ok := loadPinnedTemporalLocation(v.ZoneID); ok {
		location = pinned
	} else {
		return v, false
	}
	return CypherDateTime{Time: time.Unix(v.Time.Unix(), int64(v.Time.Nanosecond())).In(location), ZoneID: v.ZoneID}, true
}
func (v *CypherDuration) StoragePropertyValue() (any, bool) {
	if v == nil {
		return nil, true
	}
	owned := *v
	return &owned, true
}
func (v CypherPoint) StoragePropertyValue() (any, bool) {
	v.SRID = int(uint32(v.SRID))
	_, known := pointCRSBySRID(v.SRID)
	return v, known
}
func (v CypherVector) StoragePropertyValue() (any, bool) {
	owned := CypherVector{Type: v.Type}
	if v.Type.IsFloat() {
		if len(v.Floats) > 0 {
			owned.Floats = slices.Clone(v.Floats)
		}
	} else {
		if len(v.Ints) > 0 {
			owned.Ints = slices.Clone(v.Ints)
		}
	}
	return owned, v.Type <= VectorFloat32
}
func (v CypherUUID) StoragePropertyValue() (any, bool) { return v, true }

func (CypherVector) PropertyValueKind() string { return "vector" }
func (CypherUUID) PropertyValueKind() string   { return "uuid" }

var (
	_ storage.OwnedPropertyValue = CypherDate{}
	_ storage.OwnedPropertyValue = CypherLocalTime{}
	_ storage.OwnedPropertyValue = CypherTime{}
	_ storage.OwnedPropertyValue = CypherLocalDateTime{}
	_ storage.OwnedPropertyValue = CypherDateTime{}
	_ storage.OwnedPropertyValue = (*CypherDuration)(nil)
	_ storage.OwnedPropertyValue = CypherPoint{}
	_ storage.OwnedPropertyValue = CypherVector{}
	_ storage.OwnedPropertyValue = CypherUUID{}
)
