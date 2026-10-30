package date

import (
	"testing"
	"time"
)

func TestParseFormatAndISO(t *testing.T) {
	ts := Parse("2024-01-15", "yyyy-MM-dd")
	if ts == 0 {
		t.Fatalf("expected valid timestamp")
	}
	if got := Format(ts, "yyyy-MM-dd"); got != "2024-01-15" {
		t.Fatalf("format mismatch: %s", got)
	}
	if got := Parse("bad", "yyyy-MM-dd"); got != 0 {
		t.Fatalf("bad parse should be 0")
	}

	iso := ToISO8601(ts)
	if got := FromISO8601(iso); got != ts {
		t.Fatalf("iso roundtrip mismatch: %d vs %d", got, ts)
	}
	if got := FromISO8601("bad"); got != 0 {
		t.Fatalf("bad iso parse should be 0")
	}
}

func TestFieldFieldsAddAndConvert(t *testing.T) {
	ts := Parse("2024-01-15 10:30:45", "yyyy-MM-dd HH:mm:ss")
	if Field(ts, "year") != 2024 || Field(ts, "month") != 1 || Field(ts, "day") != 15 {
		t.Fatalf("field extraction failed")
	}
	if Field(ts, "unknown") != 0 {
		t.Fatalf("unknown field should be 0")
	}
	f := Fields(ts)
	if f["hour"] != 10 || f["minute"] != 30 || f["second"] != 45 {
		t.Fatalf("fields map mismatch: %#v", f)
	}

	next := Add(ts, 1, "days")
	if next <= ts {
		t.Fatalf("add should increase timestamp")
	}

	if got := Convert(3600, "seconds", "hours"); got != 1 {
		t.Fatalf("convert seconds->hours: %d", got)
	}
	if got := Convert(2, "hours", "minutes"); got != 120 {
		t.Fatalf("convert hours->minutes: %d", got)
	}
	if got := ConvertFormat("2024-01-15", "yyyy-MM-dd", "dd/MM/yyyy"); got != "15/01/2024" {
		t.Fatalf("convert format: %s", got)
	}
}

func TestDateUtilityFunctions(t *testing.T) {
	now := CurrentTimestamp()
	if now <= 0 {
		t.Fatalf("current timestamp should be positive")
	}
	if got := SystemTimezone(); got == "" {
		t.Fatalf("timezone should not be empty")
	}

	tm := time.Unix(1705276800, 0)
	if got := ToUnixTime(tm); got != 1705276800 {
		t.Fatalf("to unix time mismatch: %d", got)
	}
	if got := FromUnixTime(1705276800); got.Unix() != 1705276800 {
		t.Fatalf("from unix time mismatch")
	}
	if got := ToYears(31557600); got < 0.99 || got > 1.01 {
		t.Fatalf("to years unexpected: %f", got)
	}

	if got := ParseAsZonedDateTime("2024-01-15T10:30:00Z", "yyyy-MM-ddTHH:mm:ssZ"); got == 0 {
		t.Fatalf("zoned parse should work")
	}
}
