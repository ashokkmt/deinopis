package model

import (
	"encoding/json"
	"testing"
)

func TestNormalizedEventJSONEnvelope(t *testing.T) {
	data, err := json.Marshal(NormalizedEvent{
		Deinopis: DeinopisEnvelope{EventID: "deinopis-event"},
		OCSF:     map[string]any{"class_name": "Base Event"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected exactly the DEINOPIS and OCSF envelopes, got %s", data)
	}
	if _, ok := got["deinopis"]; !ok {
		t.Fatalf("missing deinopis envelope: %s", data)
	}
	if _, ok := got["ocsf"]; !ok {
		t.Fatalf("missing OCSF envelope: %s", data)
	}
}
