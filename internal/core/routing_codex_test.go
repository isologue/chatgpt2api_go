package core

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAccountSelectorFiltersRouteAndGroup(t *testing.T) {
	pool := NewPool([]Account{
		{ID: "web-a", Email: "web@example.com", Route: "web", GroupID: "g1", Enabled: true},
		{ID: "codex-a", Email: "codex@example.com", Route: "codex", GroupID: "g1", Enabled: true},
		{ID: "codex-b", Email: "codex2@example.com", Route: "codex", GroupID: "g2", Enabled: true},
	}, 1)
	got, ok := pool.AcquireWithSelector(map[string]bool{}, AccountSelector{Routes: []string{"codex"}, GroupID: "g1"})
	if !ok || got.ID != "codex-a" {
		t.Fatalf("selected wrong account: %#v, ok=%v", got, ok)
	}
	pool.Release(got.ID, nil)
	if _, ok := pool.AcquireWithSelector(map[string]bool{}, AccountSelector{Routes: []string{"web"}, GroupID: "g2"}); ok {
		t.Fatal("selected account outside route/group selector")
	}
	legacy := NewPool([]Account{{ID: "legacy", Email: "legacy@example.com", Enabled: true}}, 1)
	if _, ok := legacy.AcquireWithSelector(map[string]bool{}, AccountSelector{Routes: []string{"web"}}); !ok {
		t.Fatal("legacy account without route should remain selectable as web")
	}
}

func TestCodexModelConfigDefaults(t *testing.T) {
	model, imageModel := codexModelConfig(Config{})
	if model != "gpt-6-luna" || imageModel != "gpt-image-2.6" {
		t.Fatalf("unexpected defaults: %q %q", model, imageModel)
	}
	model, imageModel = codexModelConfig(Config{CodexModel: "custom", CodexImageModel: "custom-image"})
	if model != "custom" || imageModel != "custom-image" {
		t.Fatalf("custom models were not preserved: %q %q", model, imageModel)
	}
}

func TestImportCodexAccountIDAndRoute(t *testing.T) {
	raw := []byte(`{"type":"codex","name":"codex@example.com","credentials":{"access_token":"codex-at","refresh_token":"codex-rt","auth_config":{"account_id":"acct-codex"}}}`)
	accounts, err := ParseAccounts(raw)
	if err != nil || len(accounts) != 1 {
		t.Fatalf("parse codex account: %#v %v", accounts, err)
	}
	a := accounts[0]
	if a.Route != "codex" {
		t.Fatalf("expected codex route, got %q", a.Route)
	}
	if got := codexAccountID(a); got != "acct-codex" {
		t.Fatalf("expected account id, got %q; extra=%#v", got, a.Extra)
	}
	if len(a.Extra["source_record"]) == 0 {
		t.Fatal("codex source record was not preserved")
	}
}

func TestStoredRequestKeepsAccountRouting(t *testing.T) {
	original := Request{Prompt: "test", Model: "gpt-image-2.6", N: 1, apiRoutes: []string{"codex"}, apiGroupID: "group-codex"}
	raw, err := marshalStoredRequest(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored Request
	if err = unmarshalStoredRequest(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if len(restored.apiRoutes) != 1 || restored.apiRoutes[0] != "codex" || restored.apiGroupID != "group-codex" {
		t.Fatalf("stored routing lost: routes=%v group=%q raw=%s", restored.apiRoutes, restored.apiGroupID, raw)
	}
	if _, err = json.Marshal(restored); err != nil {
		t.Fatal(err)
	}
}

func TestReadCodexSSEImageAndError(t *testing.T) {
	body := "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"image_generation_call\",\"result\":\"aGVsbG8=\"}}\n\n"
	image, raw, err := readCodexSSE(strings.NewReader(body))
	if err != nil || string(image.Data) != "hello" || image.MIME != "image/png" || len(raw) == 0 {
		t.Fatalf("unexpected image SSE result: image=%#v raw=%q err=%v", image, raw, err)
	}
	_, _, err = readCodexSSE(strings.NewReader("data: {\"error\":{\"message\":\"quota exceeded\"}}\n"))
	if err == nil || !strings.Contains(err.Error(), "quota exceeded") {
		t.Fatalf("expected upstream SSE error, got %v", err)
	}
}
