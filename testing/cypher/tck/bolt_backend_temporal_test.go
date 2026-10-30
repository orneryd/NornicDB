package tck

import (
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

func TestBoltTemporalValuesUseCanonicalCypherRendering(t *testing.T) {
	stockholm, err := time.LoadLocation("Europe/Stockholm")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "date", value: dbtype.Date(time.Date(1984, 10, 11, 0, 0, 0, 0, time.UTC)), want: "1984-10-11"},
		{name: "local time omits zero seconds", value: dbtype.LocalTime(time.Date(1970, 1, 1, 12, 31, 0, 0, time.UTC)), want: "12:31"},
		{name: "time retains nanoseconds and offset seconds", value: dbtype.Time(time.Date(1970, 1, 1, 12, 31, 14, 3, time.FixedZone("", 2*3600+5*60+59))), want: "12:31:14.000000003+02:05:59"},
		{name: "local datetime omits zero seconds", value: dbtype.LocalDateTime(time.Date(1984, 10, 11, 12, 31, 0, 0, time.UTC)), want: "1984-10-11T12:31"},
		{name: "named zone is retained", value: time.Date(1984, 7, 20, 12, 31, 14, 0, stockholm), want: "1984-07-20T12:31:14+02:00[Europe/Stockholm]"},
		{name: "duration is normalized", value: dbtype.Duration{Months: 149, Days: 14, Seconds: 16*3600 + 13*60 + 10, Nanos: 1_000_000}, want: "P12Y5M14DT16H13M10.001S"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := convertBoltValue(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("converted value = %q, want %q", got, test.want)
			}
		})
	}
}
