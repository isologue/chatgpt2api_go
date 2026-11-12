package core

import (
	"encoding/json"
	"math/rand"
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
	if len(capture.String()) != rawResponseLimit || !capture.truncated {
		t.Fatalf("capture len=%d truncated=%v", len(capture.String()), capture.truncated)
	}
}

func TestResponseCaptureKeepsExactShortStreamAndBoundedLongTail(t *testing.T) {
	random := rand.New(rand.NewSource(17))
	for _, size := range []int{0, 1, rawResponseLimit/2 - 1, rawResponseLimit/2 + 1, rawResponseLimit - 1, rawResponseLimit, rawResponseLimit + 1, 3 * rawResponseLimit, 1000000} {
		raw := make([]byte, size)
		random.Read(raw)
		for _, chunk := range []int{1, 71, 8192, rawResponseLimit, 2 * rawResponseLimit} {
			capture := &responseCapture{}
			for offset := 0; offset < len(raw); offset += chunk {
				part := raw[offset:min(offset+chunk, len(raw))]
				if n, err := capture.Write(part); n != len(part) || err != nil {
					t.Fatalf("short write: %d %v", n, err)
				}
			}
			got := capture.String()
			if len(got) > rawResponseLimit || capture.total != int64(size) {
				t.Fatalf("unbounded capture %d/%d", len(got), size)
			}
			if size <= rawResponseLimit {
				if got != string(raw) || capture.truncated {
					t.Fatalf("changed untruncated raw size=%d chunk=%d", size, chunk)
				}
			} else {
				tailLen := rawResponseLimit/2 - len(streamExcerptMarker)
				if !strings.HasPrefix(got, string(raw[:rawResponseLimit/2])) || !strings.HasSuffix(got, string(raw[len(raw)-tailLen:])) || !strings.Contains(got, streamExcerptMarker) || !capture.truncated {
					t.Fatalf("lost head/tail size=%d chunk=%d", size, chunk)
				}
			}
		}
	}
}
