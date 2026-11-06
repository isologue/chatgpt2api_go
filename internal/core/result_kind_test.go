package core

import (
	"encoding/json"
	"testing"
)

func TestRequestLogKindIndependentOfModelAndAsync(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, tt := range []struct {
			name, id, kind, model, want string
			n                           int
		}{
			{"explicit alias", "custom", "image", "gpt-image-2.6", "image", 1},
			{"arbitrary alias", "custom", "image", "my-picture-alias", "image", 1},
			{"text sharing image name", "chat_x", "text", "gpt-image-2", "text", 0},
			{"file sharing image name", "filetask_x", "file", "gpt-image-2.6", "file", 0},
			{"legacy task", "task_old", "", "renamed-or-removed-alias", "image", 0},
			{"legacy text", "chat_old", "", "gpt-image-2", "text", 0},
			{"legacy file", "filetask_old", "", "gpt-image-2", "file", 0},
			{"store direct call", "custom", "", "my-picture-alias", "image", 1},
			{"legacy model", "custom", "", "gpt-image-2", "image", 0},
			{"unknown", "custom", "", "auto", "", 0},
		} {
			t.Run(tt.name+map[bool]string{false: "/sync", true: "/async"}[async], func(t *testing.T) {
				req := Request{requestKind: tt.kind, Model: tt.model, N: tt.n, Async: async}
				values, err := requestLogValues(requestLog{id: tt.id, req: req})
				if err != nil {
					t.Fatal(err)
				}
				v := values.([]any)
				if len(v) != 18 || v[17] != tt.want || v[8] != async {
					t.Fatal(v)
				}
			})
		}
	}
	var r Request
	if err := json.Unmarshal([]byte(`{"request_kind":"image","requestKind":"image","kind":"image"}`), &r); err != nil {
		t.Fatal(err)
	}
	if r.requestKind != "" {
		t.Fatal("caller injected internal classification")
	}
	r.requestKind = "image"
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err = json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["request_kind"]; ok {
		t.Fatal("internal kind changed public request JSON")
	}
}
