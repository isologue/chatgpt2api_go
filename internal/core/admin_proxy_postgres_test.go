package core

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresProxySettingsAccessRevisionAndPersistence(t *testing.T) {
	db := featureDB(t)
	previous, err := db.LoadSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		raw, _ := json.Marshal(previous.Values)
		if _, err := db.db.Exec(context.Background(), "UPDATE core_settings SET revision=$1,values=$2 WHERE id=1", previous.Revision, raw); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.db.Exec(context.Background(), "UPDATE core_settings SET revision=0,values='{}'::jsonb WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), db, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) { return nil, nil }))
	defer e.Close()
	h := NewHandler(e)
	for _, key := range []string{"wrong", c.APIKey} {
		w := requestJSON(t, h, "GET", "/admin/proxy", "", key)
		if w.Code == 200 {
			t.Fatal("proxy settings exposed to non-admin")
		}
	}
	w := requestJSON(t, h, "GET", "/admin/proxy", "", c.AdminKey)
	var initial struct {
		Revision int64
		Config   proxyConfig
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &initial) != nil {
		t.Fatal(w.Code, w.Body.String())
	}

	for _, invalid := range []string{`{"revision":0}`, `{"revision":0,"config":null}`, `{"revision":0,"config":{"mode":"proxy","static_url":"http://old:1234"}}`} {
		w = requestJSON(t, h, "PUT", "/admin/proxy", invalid, c.AdminKey)
		if w.Code != 400 {
			t.Fatal("missing/legacy configuration accepted", w.Code, w.Body.String())
		}
	}
	cfg := proxyConfig{Mode: "proxy", Proxies: []string{"http://test-user:secret-test@127.0.0.1:12345", "socks5://127.0.0.1:1080"}}.normalized("")
	body, _ := json.Marshal(map[string]any{"revision": initial.Revision, "config": cfg})
	w = requestJSON(t, h, "PUT", "/admin/proxy", string(body), c.AdminKey)
	if w.Code != 200 || !reflect.DeepEqual(e.CurrentConfig().Proxy, cfg) {
		t.Fatal(w.Code, w.Body.String())
	}
	saved, err := db.LoadSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := applySettingValues(c, saved.Values)
	if err != nil || !reflect.DeepEqual(restored.Proxy, cfg) {
		t.Fatal("not persisted", restored.Proxy, err)
	}
	// 只有专用代理入口回显凭据，普通设置不能导出或覆盖它。
	w = requestJSON(t, h, "GET", "/admin/config", "", c.AdminKey)
	if w.Code != 200 || strings.Contains(w.Body.String(), "secret-test") {
		t.Fatal("secret leaked in normal settings", w.Code)
	}
	w = requestJSON(t, h, "PUT", "/admin/proxy", string(body), c.AdminKey)
	if w.Code != 409 {
		t.Fatal("stale revision accepted", w.Code, w.Body.String())
	}
	generic, _ := json.Marshal(map[string]any{"revision": saved.Revision, "values": map[string]any{"CORE_PROXY": cfg}})
	w = requestJSON(t, h, "PUT", "/admin/config", string(generic), c.AdminKey)
	if w.Code != 400 {
		t.Fatal("generic proxy update accepted", w.Code, w.Body.String())
	}
	cfg.Proxies[0] = "http://invalid-no-port"
	body, _ = json.Marshal(map[string]any{"revision": saved.Revision, "config": cfg})
	w = requestJSON(t, h, "PUT", "/admin/proxy", string(body), c.AdminKey)
	if w.Code != 400 || !reflect.DeepEqual(e.CurrentConfig().Proxy, restored.Proxy) {
		t.Fatal("invalid config mutated state", w.Code, w.Body.String())
	}
}
