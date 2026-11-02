package core

import (
	"context"
	httpx "github.com/bogdanfinn/fhttp"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// 同源内容接口需要会话，重定向后的 CDN 请求不得携带账号令牌或 Cookie。
func TestEstuaryDownloadSessionIsolation(t *testing.T) {
	png := tinyPNG()
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("account credentials leaked to CDN")
			w.WriteHeader(403)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Write(png)
	}))
	defer cdn.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("account-session")
		if r.URL.Path != "/backend-api/estuary/content" || r.URL.RawQuery != "id=file&sig=test" || r.Header.Get("Authorization") != "Bearer token-0" || err != nil || cookie.Value != "session" {
			t.Error("same-origin content request lost session or signed query")
			w.WriteHeader(403)
			return
		}
		http.Redirect(w, r, cdn.URL+"/image.png", http.StatusFound)
	}))
	defer origin.Close()
	c := testConfig(t)
	c.UpstreamURL = origin.URL
	accounts := testAccounts(1)
	n, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	session := n.session(accounts[0])
	u, _ := url.Parse(origin.URL)
	session.jar.SetCookies(u, []*httpx.Cookie{{Name: "account-session", Value: "session"}})
	image, err := n.download(context.Background(), origin.URL+"/backend-api/estuary/content?id=file&sig=test", map[string]float64{}, session)
	if err != nil || image.Bytes != int64(len(png)) {
		t.Fatalf("artifact=%v error=%v", image, err)
	}
}
