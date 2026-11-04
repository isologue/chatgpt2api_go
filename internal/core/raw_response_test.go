package core

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestUpstreamErrorKeepsBoundedRawResponsePrivate(t *testing.T) {
	raw := []byte(`{"error":{"message":"upstream detail"}}`)
	err := upstreamError(502, raw)
	if string(err.rawResponse) != string(raw) || err.rawResponseTruncated {
		t.Fatalf("unexpected captured response: %q truncated=%v", err.rawResponse, err.rawResponseTruncated)
	}
	encoded, marshalErr := json.Marshal(err)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(encoded), "upstream detail") || strings.Contains(string(encoded), "rawResponse") {
		t.Fatalf("internal response leaked into public API error: %s", encoded)
	}
}

func TestRawResponseTruncationAndAttemptPersistence(t *testing.T) {
	raw := []byte(strings.Repeat("x", rawResponseLimit+17))
	captured, truncated := boundedRawResponse(raw)
	if len(captured) != rawResponseLimit || !truncated {
		t.Fatalf("bounded response len=%d truncated=%v", len(captured), truncated)
	}

	attempt := Attempt{Code: "no_image_generated", RawResponse: string(captured), RawResponseTruncated: truncated}
	encoded, err := json.Marshal(attempt)
	if err != nil {
		t.Fatal(err)
	}
	var restored Attempt
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.RawResponse != string(captured) || !restored.RawResponseTruncated {
		t.Fatalf("attempt response did not persist: len=%d truncated=%v", len(restored.RawResponse), restored.RawResponseTruncated)
	}
}

func TestResponseCaptureIsBoundedButConsumesInput(t *testing.T) {
	capture := &responseCapture{}
	input := []byte(strings.Repeat("s", rawResponseLimit+1))
	n, err := capture.Write(input)
	if err != nil || n != len(input) {
		t.Fatalf("capture write returned n=%d err=%v", n, err)
	}
	if capture.data.Len() != rawResponseLimit || !capture.truncated {
		t.Fatalf("capture len=%d truncated=%v", capture.data.Len(), capture.truncated)
	}
}
