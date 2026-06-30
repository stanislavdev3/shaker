package earthquake

import "testing"

func TestCanonicalPayloadHash(t *testing.T) {
	a := Event{RawPayload: []byte(`{"b":2,"a":{"y":1,"x":null}}`)}
	b := Event{RawPayload: []byte("{\n \"a\":{\"x\":null,\"y\":1}, \"b\":2 }")}
	ha, err := a.CanonicalPayloadHash()
	if err != nil {
		t.Fatal(err)
	}
	hb, err := b.CanonicalPayloadHash()
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatal("equivalent JSON produced different hashes")
	}
}
func TestCoordinateValidation(t *testing.T) {
	e := Event{Provider: "x", ExternalID: "1", Latitude: 91, Longitude: 0, RawPayload: []byte(`{}`)}
	if err := e.Validate(); err == nil {
		t.Fatal("expected invalid coordinates")
	}
}
