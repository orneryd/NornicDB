package embeddingsource

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMetadataRoundTripOwnsKeys(t *testing.T) {
	s := Source{Text: true, IncludeLabels: true, Include: []string{"text", "é:名", ""}, Exclude: []string{"secret:\nfoo"}, Properties: []string{"id", "image:uri"}}
	record := s.Metadata()
	s.Properties[0] = "changed after computation"
	encoded, err := json.Marshal(map[string]any{MetaKey: record})
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	if err := json.Unmarshal(encoded, &meta); err != nil {
		t.Fatal(err)
	}
	got, present, valid := Read(meta)
	if !present || !valid {
		t.Fatal("valid persisted selector rejected")
	}
	s.Properties[0] = "id"
	if !reflect.DeepEqual(s, got) {
		t.Fatalf("source changed across snapshot/codec: %#v", got)
	}
}

func TestMetadataRejectsInvalidRecord(t *testing.T) {
	for _, record := range []any{"", "2:0:0:0:0:", "1:8:0:0:0:", "1:0:999999999999999999999:", "1:0:1:99:x", "1:0:0:0:0:trailing", []any{"1"}} {
		_, present, valid := Read(map[string]any{MetaKey: record})
		if !present || valid {
			t.Fatalf("accepted malformed selector: %#v", record)
		}
	}
	_, present, valid := Read(nil)
	if present || !valid {
		t.Fatal("untagged legacy record must remain distinguishable")
	}
}
