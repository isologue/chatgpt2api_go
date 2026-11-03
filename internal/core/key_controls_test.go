package core

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestKeyConcurrencyCRUDAndRotation(t *testing.T) {
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), newMemoryStore(), nil)
	defer e.Close()
	h := NewHandler(e)
	w := requestJSON(t, h, "POST", "/admin/api-keys", `{"name":"client","key":"fixture-client-key-12345","enabled":true,"concurrency":2}`, c.AdminKey)
	var response struct {
		Item managedAPIKey `json:"item"`
	}
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Item.Concurrency != 2 {
		t.Fatal("concurrency create", w.Code, w.Body.String())
	}
	k := response.Item
	release, err := e.acquireKey(Request{apiKeyID: k.ID, keyConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	w = requestJSON(t, h, "PUT", "/admin/api-keys/"+k.ID, fmt.Sprintf(`{"name":"rotated","key":"fixture-client-key-rotated-12345","enabled":true,"revision":%d}`, k.Revision), c.AdminKey)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Item.Concurrency != 2 || response.Item.ID != k.ID || e.keyUsage(k.ID) != 1 {
		t.Fatal("rotation lost slots", w.Code, w.Body.String())
	}
	k = response.Item
	w = requestJSON(t, h, "PUT", "/admin/api-keys/"+k.ID, fmt.Sprintf(`{"name":"rotated","enabled":true,"concurrency":-1,"revision":%d}`, k.Revision), c.AdminKey)
	if w.Code != 400 {
		t.Fatal("negative limit accepted", w.Code)
	}
	w = requestJSON(t, h, "PUT", "/admin/api-keys/"+k.ID, fmt.Sprintf(`{"name":"rotated","enabled":true,"concurrency":1,"revision":%d}`, k.Revision), c.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, _, err = e.Submit(context.Background(), Request{N: 1, apiKeyID: k.ID, keyConcurrency: 1}); err == nil || AsError(err).Code != "api_key_concurrency_full" {
		t.Fatal("rotated lower limit not enforced", err)
	}
}
